<?php
/**
 * MIBEL Platform - Parametros API
 *
 * Endpoints for managing substitution parameters (ESCALOES).
 */

declare(strict_types=1);

/**
 * GET /api/parametros
 * Returns the complete parameters dictionary
 */
function index(): void
{
    json_response(Config::getParametros());
}

/**
 * PUT /api/parametros
 * Update all parameters
 * Body: complete parameters dictionary
 */
function update(): void
{
    $body = request_body();

    if (empty($body)) {
        error_response('Body não pode estar vazio', 400);
    }

    // Validate structure
    $errors = validateParametros($body);
    if (!empty($errors)) {
        error_response('Erros de validação: ' . implode('; ', $errors), 400);
    }

    Config::saveParametros($body);

    json_response([
        'success' => true,
        'message' => 'Parâmetros actualizados com sucesso',
    ]);
}

/**
 * GET /api/parametros/categorias
 * List all categoria_zona keys grouped by regime
 */
function categorias(): void
{
    $params = Config::getParametros();
    $result = [];

    foreach ($params as $regime => $cats) {
        if (is_array($cats)) {
            $result[$regime] = array_keys($cats);
        }
    }

    json_response($result);
}

/**
 * Validate parameters structure and values
 */
function validateParametros(array $params): array
{
    $errors = [];
    $validRegimes = ['PRE', 'PRO', 'CONSUMO', 'COMERCIALIZADOR', 'GENERICA', 'PORFOLIO'];

    foreach ($params as $regime => $categorias) {
        // Check if regime is valid
        if (!in_array($regime, $validRegimes)) {
            $errors[] = "Regime inválido: {$regime}";
            continue;
        }

        if (!is_array($categorias)) {
            $errors[] = "Regime {$regime} deve conter um objecto de categorias";
            continue;
        }

        foreach ($categorias as $categoriaZona => $config) {
            if (!is_array($config)) {
                $errors[] = "{$regime}.{$categoriaZona}: configuração inválida";
                continue;
            }

            // Validate escala (required for all)
            if (!isset($config['escala'])) {
                $errors[] = "{$regime}.{$categoriaZona}: 'escala' é obrigatório";
            } elseif (!is_numeric($config['escala']) || $config['escala'] <= 0) {
                $errors[] = "{$regime}.{$categoriaZona}: 'escala' deve ser > 0";
            }

            // Validate escaloes (only for PRE)
            if ($regime === 'PRE' && isset($config['escaloes'])) {
                if (!is_array($config['escaloes'])) {
                    $errors[] = "{$regime}.{$categoriaZona}: 'escaloes' deve ser um array";
                    continue;
                }

                $totalPct = 0;
                foreach ($config['escaloes'] as $i => $escalao) {
                    if (!isset($escalao['preco']) || !is_numeric($escalao['preco'])) {
                        $errors[] = "{$regime}.{$categoriaZona}.escaloes[{$i}]: 'preco' inválido";
                    }
                    if (!isset($escalao['pct_bids']) || !is_numeric($escalao['pct_bids'])) {
                        $errors[] = "{$regime}.{$categoriaZona}.escaloes[{$i}]: 'pct_bids' inválido";
                    } else {
                        $totalPct += (float)$escalao['pct_bids'];
                    }
                }

                // Check sum of pct_bids (allow small floating point errors)
                if (count($config['escaloes']) > 0 && ($totalPct < 0.99 || $totalPct > 1.01)) {
                    $errors[] = "{$regime}.{$categoriaZona}: soma de pct_bids deve ser 1.0 (actual: " . round($totalPct, 4) . ")";
                }
            }

            // Validate delta_preco if present
            if (isset($config['delta_preco']) && !is_numeric($config['delta_preco'])) {
                $errors[] = "{$regime}.{$categoriaZona}: 'delta_preco' deve ser numérico";
            }
        }
    }

    return $errors;
}
