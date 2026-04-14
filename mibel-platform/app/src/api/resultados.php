<?php
/**
 * MIBEL Platform - Resultados API
 *
 * Endpoints for reading clearing results from ClickHouse.
 * All job_id parameters are pre-validated by the router (UUID regex).
 */

declare(strict_types=1);

// ============================================================================
// GET /api/resultados/{job_id}/stats
// Summary statistics for a job
// ============================================================================

function stats(string $jobId): void
{
    $db = Database::getInstance();

    $info = $db->query("
        SELECT
            count()                  AS n_periodos,
            sum(n_bids_substituidos) AS total_bids_sub,
            avg(preco_clearing_orig) AS preco_orig_medio,
            avg(preco_clearing_sub)  AS preco_sub_medio,
            avg(delta_preco)         AS delta_medio,
            min(delta_preco)         AS delta_min,
            max(delta_preco)         AS delta_max,
            toString(min(data_date)) AS data_inicio,
            toString(max(data_date)) AS data_fim
        FROM mibel.clearing_substituicao
        WHERE job_id = '{$jobId}'
    ");

    $jobs = new Jobs();
    $job  = $jobs->get($jobId);

    json_response([
        'job'   => $job,
        'stats' => $info[0] ?? [],
    ]);
}

// ============================================================================
// GET /api/resultados/{job_id}/serie?pais=
// Time-series data for the line chart
// ============================================================================

function serie(string $jobId): void
{
    $db   = Database::getInstance();
    $pais = sanitizePais(get_param('pais', ''));

    $where = "job_id = '{$jobId}'";
    if ($pais !== '') {
        $where .= " AND pais = '{$pais}'";
    }

    $rows = $db->query("
        SELECT
            toString(data_date)      AS data,
            hora_num,
            pais,
            avg(preco_clearing_orig) AS preco_orig,
            avg(preco_clearing_sub)  AS preco_sub,
            avg(delta_preco)         AS delta
        FROM mibel.clearing_substituicao
        WHERE {$where}
        GROUP BY data_date, hora_num, pais
        ORDER BY data_date, hora_num, pais
    ");

    json_response([
        'labels'     => array_map(fn($r) => "{$r['data']} H{$r['hora_num']}", $rows),
        'preco_orig' => array_column($rows, 'preco_orig'),
        'preco_sub'  => array_column($rows, 'preco_sub'),
        'delta'      => array_column($rows, 'delta'),
        'hora_num'   => array_column($rows, 'hora_num'),
    ]);
}

// ============================================================================
// GET /api/resultados/{job_id}/tabela?limit=50&offset=0&pais=
// Paginated data table
// ============================================================================

function tabela(string $jobId): void
{
    $db     = Database::getInstance();
    $pais   = sanitizePais(get_param('pais', ''));
    $limit  = max(1, min(500, (int)get_param('limit', 50)));
    $offset = max(0, (int)get_param('offset', 0));

    $where = "job_id = '{$jobId}'";
    if ($pais !== '') {
        $where .= " AND pais = '{$pais}'";
    }

    $totalRows = $db->query("SELECT count() AS n FROM mibel.clearing_substituicao WHERE {$where}");

    $rows = $db->query("
        SELECT
            toString(data_date)  AS data,
            hora_raw,
            hora_num,
            pais,
            preco_clearing_orig,
            preco_clearing_sub,
            delta_preco,
            volume_clearing_orig,
            volume_clearing_sub,
            n_bids_substituidos
        FROM mibel.clearing_substituicao
        WHERE {$where}
        ORDER BY data_date, hora_num, pais
        LIMIT {$limit} OFFSET {$offset}
    ");

    json_response([
        'total' => (int)($totalRows[0]['n'] ?? 0),
        'rows'  => $rows,
    ]);
}

// ============================================================================
// GET /api/resultados/{job_id}/logs
// Last 200 worker log lines from ClickHouse
// ============================================================================

function logs(string $jobId): void
{
    $db = Database::getInstance();

    $rows = $db->query("
        SELECT nivel, mensagem, toString(ts) AS ts
        FROM mibel.worker_logs
        WHERE job_id = '{$jobId}'
        ORDER BY ts ASC
        LIMIT 200
    ");

    json_response($rows);
}

// ============================================================================
// GET /api/resultados/{job_id}/exportar?formato=csv|json
// File download — bypasses json_response
// ============================================================================

function exportar(string $jobId): void
{
    $db  = Database::getInstance();
    $fmt = get_param('formato', 'csv');

    if (!in_array($fmt, ['csv', 'json'], true)) {
        $fmt = 'csv';
    }

    $rows = $db->query("
        SELECT
            toString(data_date)  AS data,
            hora_raw,
            hora_num,
            pais,
            preco_clearing_orig,
            preco_clearing_sub,
            delta_preco,
            volume_clearing_orig,
            volume_clearing_sub,
            n_bids_substituidos
        FROM mibel.clearing_substituicao
        WHERE job_id = '{$jobId}'
        ORDER BY data_date, hora_num, pais
    ");

    if ($fmt === 'json') {
        header('Content-Type: application/json; charset=utf-8');
        header("Content-Disposition: attachment; filename=\"resultado_{$jobId}.json\"");
        echo json_encode($rows, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    } else {
        header('Content-Type: text/csv; charset=UTF-8');
        header("Content-Disposition: attachment; filename=\"resultado_{$jobId}.csv\"");
        echo "\xEF\xBB\xBF"; // UTF-8 BOM for Excel
        if (!empty($rows)) {
            echo implode(';', array_keys($rows[0])) . "\n";
            foreach ($rows as $row) {
                $cells = array_map(
                    fn($v) => str_replace('.', ',', (string)($v ?? '')),
                    $row
                );
                echo implode(';', $cells) . "\n";
            }
        }
    }

    exit;
}

// ============================================================================
// Helper
// ============================================================================

/**
 * Whitelist pais values to prevent SQL injection
 */
function sanitizePais(string $pais): string
{
    return in_array($pais, ['MI', 'ES', 'PT'], true) ? $pais : '';
}
