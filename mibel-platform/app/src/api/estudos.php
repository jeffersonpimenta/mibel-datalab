<?php
/**
 * MIBEL Platform - Estudos (Jobs) API
 *
 * Endpoints for managing analysis jobs (substitution and optimization studies).
 */

declare(strict_types=1);

/**
 * POST /api/estudos
 * Create and launch a new study
 * Body: {tipo, data_inicio, data_fim, observacoes, workers_n}
 */
function store(): void
{
    $body = request_body();

    // Validate required fields
    if (empty($body['tipo'])) {
        error_response('Campo "tipo" é obrigatório (substituicao ou otimizacao)', 400);
    }
    if (!in_array($body['tipo'], ['substituicao', 'otimizacao'])) {
        error_response('Tipo deve ser "substituicao" ou "otimizacao"', 400);
    }
    if (empty($body['data_inicio'])) {
        error_response('Campo "data_inicio" é obrigatório', 400);
    }
    if (empty($body['data_fim'])) {
        error_response('Campo "data_fim" é obrigatório', 400);
    }

    // Validate date format
    $dataInicio = $body['data_inicio'];
    $dataFim = $body['data_fim'];

    if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $dataInicio)) {
        error_response('data_inicio deve estar no formato YYYY-MM-DD', 400);
    }
    if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $dataFim)) {
        error_response('data_fim deve estar no formato YYYY-MM-DD', 400);
    }

    // Validate date range
    if ($dataFim < $dataInicio) {
        error_response('data_fim deve ser igual ou posterior a data_inicio', 400);
    }

    $observacoes = trim($body['observacoes'] ?? '');
    $workersN = max(1, min(16, (int)($body['workers_n'] ?? 4)));

    // Create job record
    $jobs = new Jobs();
    $jobId = $jobs->create(
        $body['tipo'],
        $dataInicio,
        $dataFim,
        $observacoes,
        $workersN
    );

    // Ensure output directory exists
    $outputDir = '/data/outputs';
    if (!is_dir($outputDir)) {
        mkdir($outputDir, 0755, true);
    }

    // Determine worker script
    $script = $body['tipo'] === 'otimizacao'
        ? '/app/otimizacao_worker.py'
        : '/app/substituicao_worker.py';

    // Launch worker in background via docker exec
    // The python-worker container runs with tail -f /dev/null, so we exec into it
    $logPath = "/data/outputs/{$jobId}.log";

    $cmd = sprintf(
        'docker exec mibel-platform-python-worker-1 python %s --job_id %s --data_inicio %s --data_fim %s --workers %d > %s 2>&1 &',
        $script,
        escapeshellarg($jobId),
        escapeshellarg($dataInicio),
        escapeshellarg($dataFim),
        $workersN,
        $logPath
    );

    // Execute command
    exec($cmd, $output, $returnCode);

    // Mark as running
    $jobs->markRunning($jobId);

    json_response([
        'job_id' => $jobId,
        'status' => 'RUNNING',
        'message' => 'Estudo lançado com sucesso',
    ]);
}

/**
 * GET /api/estudos
 * List all jobs with status updates
 */
function index(): void
{
    $limit = (int)(get_param('limit', 50));
    $jobs = new Jobs();
    $lista = $jobs->list($limit);

    // Check and update status for RUNNING jobs
    foreach ($lista as &$job) {
        if ($job['status'] === 'RUNNING') {
            $job = checkAndUpdateJobStatus($jobs, $job);
        }
    }

    json_response($lista);
}

/**
 * GET /api/estudos/{id}
 * Get job details and log lines
 */
function show(string $id): void
{
    $jobs = new Jobs();
    $job = $jobs->get($id);

    if (!$job) {
        error_response('Estudo não encontrado', 404);
    }

    // Update status if running
    if ($job['status'] === 'RUNNING') {
        $job = checkAndUpdateJobStatus($jobs, $job);
    }

    // Read last 100 log lines
    $logPath = "/data/outputs/{$id}.log";
    $logLines = [];

    if (file_exists($logPath)) {
        $allLines = file($logPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
        if ($allLines !== false) {
            $logLines = array_slice($allLines, -100);
        }
    }

    json_response([
        'job' => $job,
        'log' => $logLines,
        'log_count' => count($logLines),
    ]);
}

/**
 * POST /api/estudos/{id}/cancelar
 * Cancel a running job
 */
function cancelar(string $id): void
{
    $jobs = new Jobs();
    $job = $jobs->get($id);

    if (!$job) {
        error_response('Estudo não encontrado', 404);
    }

    if (!in_array($job['status'], ['PENDING', 'RUNNING'])) {
        error_response('Apenas estudos PENDING ou RUNNING podem ser cancelados', 400);
    }

    // For v1: just mark as FAILED without killing the process
    // In production, we would need to track the PID and kill it
    $jobs->updateStatus($id, 'FAILED', '', 'Cancelado pelo utilizador');

    // Append cancellation to log
    $logPath = "/data/outputs/{$id}.log";
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents(
        $logPath,
        "\n[{$timestamp}] [STATUS] FAILED - Cancelado pelo utilizador\n",
        FILE_APPEND
    );

    json_response([
        'success' => true,
        'message' => 'Estudo cancelado',
    ]);
}

/**
 * DELETE /api/estudos/{id}
 * Delete a job (only PENDING or FAILED)
 */
function destroy(string $id): void
{
    $jobs = new Jobs();
    $job = $jobs->get($id);

    if (!$job) {
        error_response('Estudo não encontrado', 404);
    }

    if (!in_array($job['status'], ['PENDING', 'FAILED'])) {
        error_response('Apenas estudos PENDING ou FAILED podem ser removidos', 400);
    }

    $deleted = $jobs->delete($id);

    if ($deleted) {
        // Also remove log file if exists
        $logPath = "/data/outputs/{$id}.log";
        if (file_exists($logPath)) {
            @unlink($logPath);
        }

        json_response([
            'success' => true,
            'message' => 'Estudo removido',
        ]);
    } else {
        error_response('Não foi possível remover o estudo', 500);
    }
}

/**
 * Check job log file and update status if finished
 */
function checkAndUpdateJobStatus(Jobs $jobs, array $job): array
{
    $logPath = "/data/outputs/{$job['id']}.log";

    if (!file_exists($logPath)) {
        return $job;
    }

    // Read last few lines to check for status
    $lines = file($logPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    if (empty($lines)) {
        return $job;
    }

    // Check last 5 lines for status marker
    $lastLines = array_slice($lines, -5);
    foreach (array_reverse($lastLines) as $line) {
        if (strpos($line, '[STATUS] DONE') !== false) {
            $jobs->updateStatus($job['id'], 'DONE');
            $job['status'] = 'DONE';
            $job['finished_at'] = date('Y-m-d H:i:s');
            break;
        } elseif (strpos($line, '[STATUS] FAILED') !== false) {
            $jobs->updateStatus($job['id'], 'FAILED', '', $line);
            $job['status'] = 'FAILED';
            $job['finished_at'] = date('Y-m-d H:i:s');
            $job['erro'] = $line;
            break;
        }
    }

    return $job;
}
