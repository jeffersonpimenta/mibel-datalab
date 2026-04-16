<?php
/**
 * MIBEL Platform - Resultados API
 *
 * Suporta dois tipos de estudo:
 *   substituicao → tabela mibel.clearing_substituicao
 *   otimizacao   → tabela mibel.clearing_otimizacao
 *
 * Todos os endpoints detectam o tipo do job e adaptam a query.
 * A resposta normaliza os campos para que o frontend use sempre
 * as mesmas chaves (preco_sim, delta_valor, etc.).
 */

declare(strict_types=1);

// ============================================================================
// Helpers
// ============================================================================

function getJobInfo(string $jobId): array
{
    $jobs = new Jobs();
    return $jobs->get($jobId) ?? ['tipo' => 'substituicao'];
}

function isOtimizacao(array $job): bool
{
    return ($job['tipo'] ?? '') === 'otimizacao';
}

function sanitizePais(string $pais): string
{
    return in_array($pais, ['MI', 'ES', 'PT'], true) ? $pais : '';
}

// ============================================================================
// GET /api/resultados/{job_id}/stats
// ============================================================================

function stats(string $jobId): void
{
    $db  = Database::getInstance();
    $job = getJobInfo($jobId);

    if (isOtimizacao($job)) {
        $info = $db->query("
            SELECT
                count()                      AS n_periodos,
                avg(preco_clearing_orig)     AS preco_orig_medio,
                avg(preco_clearing_opt)      AS preco_sim_medio,
                avg(delta_preco)             AS delta_medio,
                min(delta_preco)             AS delta_min,
                max(delta_preco)             AS delta_max,
                toString(min(data_date))     AS data_inicio,
                toString(max(data_date))     AS data_fim,
                sum(lucro_pre_base)          AS lucro_base_total,
                sum(lucro_pre_opt)           AS lucro_opt_total,
                sum(delta_lucro_pre)         AS delta_lucro_total,
                sum(n_bids_pre_removidos)    AS total_bids_rem
            FROM mibel.clearing_otimizacao
            WHERE job_id = '{$jobId}'
        ");
        $stats = $info[0] ?? [];
        $stats['tipo']          = 'otimizacao';
        $stats['total_bids_sub'] = null;
    } else {
        $info = $db->query("
            SELECT
                count()                  AS n_periodos,
                avg(preco_clearing_orig) AS preco_orig_medio,
                avg(preco_clearing_sub)  AS preco_sim_medio,
                avg(delta_preco)         AS delta_medio,
                min(delta_preco)         AS delta_min,
                max(delta_preco)         AS delta_max,
                toString(min(data_date)) AS data_inicio,
                toString(max(data_date)) AS data_fim,
                sum(n_bids_substituidos) AS total_bids_sub
            FROM mibel.clearing_substituicao
            WHERE job_id = '{$jobId}'
        ");
        $stats = $info[0] ?? [];
        $stats['tipo']            = 'substituicao';
        $stats['lucro_base_total'] = null;
        $stats['lucro_opt_total']  = null;
        $stats['delta_lucro_total'] = null;
        $stats['total_bids_rem']  = null;
    }

    json_response([
        'job'   => $job,
        'stats' => $stats,
    ]);
}

// ============================================================================
// GET /api/resultados/{job_id}/serie?pais=
// ============================================================================

function serie(string $jobId): void
{
    $db   = Database::getInstance();
    $job  = getJobInfo($jobId);
    $pais = sanitizePais(get_param('pais', ''));

    $where = "job_id = '{$jobId}'";
    if ($pais !== '') {
        $where .= " AND pais = '{$pais}'";
    }

    if (isOtimizacao($job)) {
        $rows = $db->query("
            SELECT
                toString(data_date)          AS data,
                hora_num,
                pais,
                avg(preco_clearing_orig)     AS preco_orig,
                avg(preco_clearing_opt)      AS preco_sim,
                avg(delta_preco)             AS delta,
                avg(delta_lucro_pre)         AS delta_lucro
            FROM mibel.clearing_otimizacao
            WHERE {$where}
            GROUP BY data_date, hora_num, pais
            ORDER BY data_date, hora_num, pais
        ");
    } else {
        $rows = $db->query("
            SELECT
                toString(data_date)      AS data,
                hora_num,
                pais,
                avg(preco_clearing_orig) AS preco_orig,
                avg(preco_clearing_sub)  AS preco_sim,
                avg(delta_preco)         AS delta,
                NULL                     AS delta_lucro
            FROM mibel.clearing_substituicao
            WHERE {$where}
            GROUP BY data_date, hora_num, pais
            ORDER BY data_date, hora_num, pais
        ");
    }

    json_response([
        'tipo'        => isOtimizacao($job) ? 'otimizacao' : 'substituicao',
        'rows'        => $rows,
        'pais'        => array_column($rows, 'pais'),
        'labels'      => array_map(fn($r) => "{$r['data']} H{$r['hora_num']}", $rows),
        'preco_orig'  => array_column($rows, 'preco_orig'),
        'preco_sim'   => array_column($rows, 'preco_sim'),
        'delta'       => array_column($rows, 'delta'),
        'delta_lucro' => array_column($rows, 'delta_lucro'),
        'hora_num'    => array_column($rows, 'hora_num'),
    ]);
}

// ============================================================================
// GET /api/resultados/{job_id}/tabela?limit=50&offset=0&pais=
// ============================================================================

function tabela(string $jobId): void
{
    $db     = Database::getInstance();
    $job    = getJobInfo($jobId);
    $pais   = sanitizePais(get_param('pais', ''));
    $limit  = max(1, min(500, (int)get_param('limit', 50)));
    $offset = max(0, (int)get_param('offset', 0));

    $where = "job_id = '{$jobId}'";
    if ($pais !== '') {
        $where .= " AND pais = '{$pais}'";
    }

    if (isOtimizacao($job)) {
        $table = 'mibel.clearing_otimizacao';
        $totalRows = $db->query("SELECT count() AS n FROM {$table} WHERE {$where}");

        $rows = $db->query("
            SELECT
                toString(data_date)          AS data,
                hora_raw,
                hora_num,
                pais,
                preco_clearing_orig          AS preco_orig,
                preco_clearing_base          AS preco_base,
                preco_clearing_opt           AS preco_sim,
                delta_preco                  AS delta_preco,
                lucro_pre_base,
                lucro_pre_opt,
                delta_lucro_pre              AS delta_lucro,
                n_bids_pre_removidos         AS n_bids_sub,
                volume_clearing_orig,
                volume_clearing_opt          AS volume_sim
            FROM {$table}
            WHERE {$where}
            ORDER BY data_date, hora_num, pais
            LIMIT {$limit} OFFSET {$offset}
        ");
    } else {
        $table = 'mibel.clearing_substituicao';
        $totalRows = $db->query("SELECT count() AS n FROM {$table} WHERE {$where}");

        $rows = $db->query("
            SELECT
                toString(data_date)      AS data,
                hora_raw,
                hora_num,
                pais,
                preco_clearing_orig      AS preco_orig,
                preco_clearing_orig      AS preco_base,
                preco_clearing_sub       AS preco_sim,
                delta_preco,
                NULL                     AS lucro_pre_base,
                NULL                     AS lucro_pre_opt,
                NULL                     AS delta_lucro,
                n_bids_substituidos      AS n_bids_sub,
                volume_clearing_orig,
                volume_clearing_sub      AS volume_sim
            FROM {$table}
            WHERE {$where}
            ORDER BY data_date, hora_num, pais
            LIMIT {$limit} OFFSET {$offset}
        ");
    }

    json_response([
        'tipo'  => isOtimizacao($job) ? 'otimizacao' : 'substituicao',
        'total' => (int)($totalRows[0]['n'] ?? 0),
        'rows'  => $rows,
    ]);
}

// ============================================================================
// GET /api/resultados/{job_id}/logs
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
// ============================================================================

function exportar(string $jobId): void
{
    $db  = Database::getInstance();
    $job = getJobInfo($jobId);
    $fmt = in_array(get_param('formato', 'csv'), ['csv', 'json'], true)
        ? get_param('formato', 'csv')
        : 'csv';

    if (isOtimizacao($job)) {
        $rows = $db->query("
            SELECT
                toString(data_date)          AS data,
                hora_raw,
                hora_num,
                pais,
                preco_clearing_orig,
                preco_clearing_base,
                preco_clearing_opt,
                delta_preco,
                lucro_pre_base,
                lucro_pre_opt,
                delta_lucro_pre,
                vol_pre_removido_opt,
                n_bids_pre_removidos,
                volume_clearing_orig,
                volume_clearing_opt
            FROM mibel.clearing_otimizacao
            WHERE job_id = '{$jobId}'
            ORDER BY data_date, hora_num, pais
        ");
    } else {
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
    }

    $tipo = isOtimizacao($job) ? 'otimizacao' : 'substituicao';

    if ($fmt === 'json') {
        header('Content-Type: application/json; charset=utf-8');
        header("Content-Disposition: attachment; filename=\"resultado_{$tipo}_{$jobId}.json\"");
        echo json_encode($rows, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    } else {
        header('Content-Type: text/csv; charset=UTF-8');
        header("Content-Disposition: attachment; filename=\"resultado_{$tipo}_{$jobId}.csv\"");
        echo "\xEF\xBB\xBF";
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
