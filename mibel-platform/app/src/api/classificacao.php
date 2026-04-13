<?php
/**
 * MIBEL Platform - Classificacao API
 *
 * Endpoints for managing technology classification and unit exceptions.
 */

declare(strict_types=1);

// ============================================================================
// Classificacao (Technology Classification)
// ============================================================================

/**
 * GET /api/classificacao
 * Returns all classification entries with positional index
 */
function index(): void
{
    $data = Config::getClassificacao();
    json_response($data);
}

/**
 * POST /api/classificacao
 * Add new classification entry
 * Body: {tecnologia, regime, categoria}
 */
function store(): void
{
    $body = request_body();

    // Validate required fields
    if (empty($body['tecnologia'])) {
        error_response('Campo "tecnologia" é obrigatório', 400);
    }
    if (empty($body['regime'])) {
        error_response('Campo "regime" é obrigatório', 400);
    }
    if (empty($body['categoria'])) {
        error_response('Campo "categoria" é obrigatório', 400);
    }

    // Validate regime
    $validRegimes = ['PRE', 'PRO', 'CONSUMO', 'COMERCIALIZADOR', 'GENERICA', 'PORFOLIO'];
    if (!in_array($body['regime'], $validRegimes)) {
        error_response('Regime inválido: ' . $body['regime'], 400);
    }

    $dados = Config::getClassificacao();

    // Check for duplicate tecnologia
    foreach ($dados as $entry) {
        if (strtolower($entry['tecnologia']) === strtolower($body['tecnologia'])) {
            error_response('Tecnologia já existe: ' . $body['tecnologia'], 400);
        }
    }

    // Add new entry
    $dados[] = [
        'tecnologia' => trim($body['tecnologia']),
        'regime' => $body['regime'],
        'categoria' => $body['categoria'],
    ];

    Config::saveClassificacao($dados);

    json_response([
        'success' => true,
        'total' => count($dados),
        'message' => 'Classificação adicionada com sucesso',
    ]);
}

/**
 * PUT /api/classificacao/{idx}
 * Update classification entry by positional index
 * Body: {tecnologia, regime, categoria}
 */
function update(int $idx): void
{
    $body = request_body();
    $dados = Config::getClassificacao();

    // Validate index
    if ($idx < 0 || $idx >= count($dados)) {
        error_response('Índice inválido: ' . $idx, 404);
    }

    // Validate required fields
    if (empty($body['tecnologia'])) {
        error_response('Campo "tecnologia" é obrigatório', 400);
    }
    if (empty($body['regime'])) {
        error_response('Campo "regime" é obrigatório', 400);
    }
    if (empty($body['categoria'])) {
        error_response('Campo "categoria" é obrigatório', 400);
    }

    // Validate regime
    $validRegimes = ['PRE', 'PRO', 'CONSUMO', 'COMERCIALIZADOR', 'GENERICA', 'PORFOLIO'];
    if (!in_array($body['regime'], $validRegimes)) {
        error_response('Regime inválido: ' . $body['regime'], 400);
    }

    // Check for duplicate tecnologia (excluding current entry)
    foreach ($dados as $i => $entry) {
        if ($i !== $idx && strtolower($entry['tecnologia']) === strtolower($body['tecnologia'])) {
            error_response('Tecnologia já existe: ' . $body['tecnologia'], 400);
        }
    }

    // Update entry
    $dados[$idx] = [
        'tecnologia' => trim($body['tecnologia']),
        'regime' => $body['regime'],
        'categoria' => $body['categoria'],
    ];

    Config::saveClassificacao($dados);

    json_response([
        'success' => true,
        'message' => 'Classificação actualizada com sucesso',
    ]);
}

/**
 * DELETE /api/classificacao/{idx}
 * Remove classification entry by positional index
 */
function destroy(int $idx): void
{
    $dados = Config::getClassificacao();

    // Validate index
    if ($idx < 0 || $idx >= count($dados)) {
        error_response('Índice inválido: ' . $idx, 404);
    }

    $removed = $dados[$idx];

    // Remove entry and re-index
    array_splice($dados, $idx, 1);

    Config::saveClassificacao($dados);

    json_response([
        'success' => true,
        'removed' => $removed,
        'total' => count($dados),
        'message' => 'Classificação removida com sucesso',
    ]);
}

// ============================================================================
// Excecoes (Unit Exceptions)
// ============================================================================

/**
 * GET /api/excecoes
 * Returns all exception entries
 */
function indexExcecoes(): void
{
    $data = Config::getExcecoes();
    json_response($data);
}

/**
 * POST /api/excecoes
 * Add new exception entry
 * Body: {codigo, categoria_zona, motivo}
 */
function storeExcecao(): void
{
    $body = request_body();

    // Validate required fields
    if (empty($body['codigo'])) {
        error_response('Campo "codigo" é obrigatório', 400);
    }
    if (empty($body['categoria_zona'])) {
        error_response('Campo "categoria_zona" é obrigatório', 400);
    }

    $dados = Config::getExcecoes();

    // Check for duplicate codigo
    foreach ($dados as $entry) {
        if (strtoupper($entry['codigo']) === strtoupper($body['codigo'])) {
            error_response('Código já existe: ' . $body['codigo'], 400);
        }
    }

    // Add new entry
    $dados[] = [
        'codigo' => strtoupper(trim($body['codigo'])),
        'categoria_zona' => trim($body['categoria_zona']),
        'motivo' => trim($body['motivo'] ?? ''),
    ];

    Config::saveExcecoes($dados);

    json_response([
        'success' => true,
        'total' => count($dados),
        'message' => 'Excepção adicionada com sucesso',
    ]);
}

/**
 * DELETE /api/excecoes/{codigo}
 * Remove exception entry by codigo
 */
function destroyExcecao(string $codigo): void
{
    $dados = Config::getExcecoes();
    $codigo = strtoupper($codigo);

    // Find and remove entry
    $found = false;
    $removed = null;

    foreach ($dados as $i => $entry) {
        if (strtoupper($entry['codigo']) === $codigo) {
            $removed = $entry;
            array_splice($dados, $i, 1);
            $found = true;
            break;
        }
    }

    if (!$found) {
        error_response('Código não encontrado: ' . $codigo, 404);
    }

    Config::saveExcecoes($dados);

    json_response([
        'success' => true,
        'removed' => $removed,
        'total' => count($dados),
        'message' => 'Excepção removida com sucesso',
    ]);
}
