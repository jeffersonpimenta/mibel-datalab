<?php
/**
 * MIBEL Platform - Ingestão de Dados
 *
 * Gere o upload de ZIPs mensais de bids OMIE e a sua ingestão em mibel.bids_raw.
 *
 * Endpoints:
 *   GET  /api/ingestao           — resumo dos meses já ingeridos no ClickHouse
 *   POST /api/ingestao           — upload de ZIP + lança ingestao_worker.py
 *   DELETE /api/ingestao/mes/{yyyymm} — elimina todos os bids de um mês do ClickHouse
 */

declare(strict_types=1);

define('BIDS_DIR',          '/data/bids');
define('MAX_UPLOAD_BYTES',  512 * 1024 * 1024); // 512 MB
define('FILENAME_PATTERN',  '/^curva_pbc_uof_\d{6}\.zip$/');

// ============================================================================
// GET /api/ingestao — resumo de dados no ClickHouse + jobs activos
// ============================================================================

function index(): void
{
    $db = Database::getInstance();

    // Resumo por mês (mibel.bids_raw)
    $meses = [];
    try {
        $rows = $db->query("
            SELECT
                toYYYYMM(data_ficheiro)      AS mes,
                count()                      AS n_rows,
                countDistinct(data_ficheiro) AS n_dias,
                toString(min(data_ficheiro)) AS data_min,
                toString(max(data_ficheiro)) AS data_max,
                toString(max(ingestao_ts))   AS ultima_ingestao
            FROM mibel.bids_raw
            GROUP BY mes
            ORDER BY mes
        ");

        $nomes_meses = ['Jan','Fev','Mar','Abr','Mai','Jun',
                        'Jul','Ago','Set','Out','Nov','Dez'];

        foreach ($rows as $r) {
            $mes_str = (string)$r['mes'];
            $ano     = substr($mes_str, 0, 4);
            $mes_n   = (int)substr($mes_str, 4, 2);
            $meses[] = [
                'mes'             => $mes_str,
                'mes_label'       => ($nomes_meses[$mes_n - 1] ?? '?') . ' ' . $ano,
                'n_rows'          => (int)$r['n_rows'],
                'n_dias'          => (int)$r['n_dias'],
                'data_min'        => $r['data_min'],
                'data_max'        => $r['data_max'],
                'ultima_ingestao' => $r['ultima_ingestao'],
            ];
        }
    } catch (\Throwable $e) {
        // ClickHouse pode estar em arranque — devolver lista vazia com aviso
        json_response([
            'meses'       => [],
            'total_rows'  => 0,
            'ch_error'    => $e->getMessage(),
        ]);
    }

    $total_rows = array_sum(array_column($meses, 'n_rows'));

    // Jobs de ingestão activos (RUNNING) ou recentes (DONE/FAILED nas últimas 24h)
    $jobs  = new Jobs();
    $todos = $jobs->list(20);
    $jobs_ingestao = array_values(array_filter($todos, function ($j) {
        return $j['tipo'] === 'ingestao';
    }));

    // Actualizar status dos RUNNING
    foreach ($jobs_ingestao as &$job) {
        if ($job['status'] === 'RUNNING') {
            $job = checkIngestaoJobStatus($jobs, $job);
        }
    }
    unset($job);

    json_response([
        'meses'          => $meses,
        'total_rows'     => $total_rows,
        'jobs_ingestao'  => $jobs_ingestao,
    ]);
}

// ============================================================================
// POST /api/ingestao — upload de ZIP e lançamento do worker
// ============================================================================

function store(): void
{
    if (empty($_FILES['file'])) {
        error_response('Nenhum ficheiro recebido. Use multipart/form-data com campo "file".', 400);
    }

    $upload = $_FILES['file'];

    if ($upload['error'] !== UPLOAD_ERR_OK) {
        $msg = match ($upload['error']) {
            UPLOAD_ERR_INI_SIZE, UPLOAD_ERR_FORM_SIZE => 'Ficheiro demasiado grande.',
            UPLOAD_ERR_PARTIAL   => 'Upload incompleto.',
            UPLOAD_ERR_NO_FILE   => 'Nenhum ficheiro enviado.',
            default              => 'Erro de upload (código ' . $upload['error'] . ').',
        };
        error_response($msg, 400);
    }

    $name = basename($upload['name']);

    if (!preg_match(FILENAME_PATTERN, $name)) {
        error_response(
            "Nome de ficheiro inválido: \"{$name}\". " .
            "Formato esperado: curva_pbc_uof_YYYYMM.zip",
            422
        );
    }

    if ($upload['size'] > MAX_UPLOAD_BYTES) {
        error_response('Ficheiro demasiado grande (máximo 512 MB).', 413);
    }

    // Validar que é mesmo um ZIP
    $finfo = new finfo(FILEINFO_MIME_TYPE);
    $mime  = $finfo->file($upload['tmp_name']);
    if (!in_array($mime, ['application/zip', 'application/x-zip-compressed', 'application/octet-stream'], true)) {
        if (strtolower(pathinfo($name, PATHINFO_EXTENSION)) !== 'zip') {
            error_response("O ficheiro não parece ser um ZIP válido (MIME: {$mime}).", 422);
        }
    }

    // Garantir directório
    $dir = BIDS_DIR;
    if (!is_dir($dir)) {
        if (!mkdir($dir, 0755, true)) {
            error_response("Não foi possível criar o directório de destino: {$dir}", 500);
        }
    }

    $dest      = $dir . '/' . $name;
    $overwrite = file_exists($dest);

    if (!move_uploaded_file($upload['tmp_name'], $dest)) {
        error_response('Falha ao mover o ficheiro para o destino.', 500);
    }

    // Criar job de ingestão
    $jobs  = new Jobs();
    $jobId = $jobs->create('ingestao', '', '', $name, 4);

    // Garantir directório de logs
    $outputDir = '/data/outputs';
    if (!is_dir($outputDir)) {
        mkdir($outputDir, 0777, true);
    }

    $logPath = "/data/outputs/{$jobId}.log";

    // Lançar ingestao_worker.py no container Python
    $cmd = sprintf(
        'docker exec mibel-datalab-python-worker-1 python /app/ingestao_worker.py --job_id %s --zip_path %s --workers 4 > %s 2>&1 &',
        escapeshellarg($jobId),
        escapeshellarg($dest),
        $logPath
    );
    exec($cmd);

    $jobs->markRunning($jobId);

    json_response([
        'success'   => true,
        'job_id'    => $jobId,
        'name'      => $name,
        'overwrite' => $overwrite,
    ], 201);
}

// ============================================================================
// DELETE /api/ingestao/mes/{yyyymm} — elimina todos os bids de um mês
// ============================================================================

function destroyMes(string $yyyymm): void
{
    if (!preg_match('/^\d{6}$/', $yyyymm)) {
        error_response("Formato de mês inválido: \"{$yyyymm}\". Esperado: YYYYMM", 422);
    }

    $ano = (int)substr($yyyymm, 0, 4);
    $mes = (int)substr($yyyymm, 4, 2);

    if ($mes < 1 || $mes > 12) {
        error_response("Mês inválido: {$mes}", 422);
    }

    $db = Database::getInstance();

    // Contar registos antes de apagar
    $count_rows = $db->query("
        SELECT count() AS n
        FROM mibel.bids_raw
        WHERE toYYYYMM(data_ficheiro) = {$yyyymm}
    ");
    $n_antes = (int)($count_rows[0]['n'] ?? 0);

    if ($n_antes === 0) {
        error_response("Não existem dados para o mês {$yyyymm} no ClickHouse.", 404);
    }

    // Eliminar por partição (operação eficiente no ClickHouse)
    $db->execute("ALTER TABLE mibel.bids_raw DROP PARTITION {$yyyymm}");

    json_response([
        'success'  => true,
        'mes'      => $yyyymm,
        'deleted'  => $n_antes,
    ]);
}

// ============================================================================
// Verificação de status de um job de ingestão (via log file)
// ============================================================================

function checkIngestaoJobStatus(Jobs $jobs, array $job): array
{
    $logPath = "/data/outputs/{$job['id']}.log";

    if (!file_exists($logPath)) {
        return $job;
    }

    $lines = file($logPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    if (empty($lines)) {
        return $job;
    }

    $lastLines = array_slice($lines, -5);
    foreach (array_reverse($lastLines) as $line) {
        if (strpos($line, '[STATUS] DONE') !== false) {
            $jobs->updateStatus($job['id'], 'DONE');
            $job['status']      = 'DONE';
            $job['finished_at'] = date('Y-m-d H:i:s');
            break;
        } elseif (strpos($line, '[STATUS] FAILED') !== false) {
            $jobs->updateStatus($job['id'], 'FAILED', '', $line);
            $job['status']      = 'FAILED';
            $job['finished_at'] = date('Y-m-d H:i:s');
            $job['erro']        = $line;
            break;
        }
    }

    return $job;
}
