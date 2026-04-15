<?php
/**
 * MIBEL Platform - REST API Router
 *
 * Routes all /api/* requests to appropriate handlers.
 */

declare(strict_types=1);

// Error handling
set_error_handler(function ($severity, $message, $file, $line) {
    throw new ErrorException($message, 0, $severity, $file, $line);
});

// Load dependencies
require_once __DIR__ . '/../Database.php';
require_once __DIR__ . '/../Jobs.php';
require_once __DIR__ . '/../Config.php';

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Send JSON response with status code
 */
function json_response(array $data, int $status = 200): void
{
    http_response_code($status);
    header('Content-Type: application/json; charset=utf-8');
    header('Access-Control-Allow-Origin: *');
    header('Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type');
    echo json_encode($data, JSON_UNESCAPED_UNICODE);
    exit;
}

/**
 * Send error response
 */
function error_response(string $message, int $status = 400): void
{
    json_response(['error' => $message], $status);
}

/**
 * Parse JSON request body
 */
function request_body(): array
{
    $input = file_get_contents('php://input');
    if (empty($input)) {
        return [];
    }

    $data = json_decode($input, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        error_response('Invalid JSON body: ' . json_last_error_msg(), 400);
    }

    return $data ?? [];
}

/**
 * Get query parameter with optional default
 */
function get_param(string $name, $default = null)
{
    return $_GET[$name] ?? $default;
}

/**
 * Get route parameter from matches array
 */
function route_param(array $matches, int $index): ?string
{
    return $matches[$index] ?? null;
}

// ============================================================================
// Route Matching
// ============================================================================

$method = $_SERVER['REQUEST_METHOD'];
$uri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);

// Remove /api prefix for matching
$path = preg_replace('#^/api#', '', $uri);

// Handle CORS preflight
if ($method === 'OPTIONS') {
    header('Access-Control-Allow-Origin: *');
    header('Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type');
    http_response_code(204);
    exit;
}

// ============================================================================
// Routes
// ============================================================================

try {
    // Health check
    if ($path === '/health' && $method === 'GET') {
        $db = Database::getInstance();
        json_response([
            'status' => 'ok',
            'clickhouse' => $db->ping(),
            'timestamp' => date('c'),
        ]);
    }

    // -------------------------------------------------------------------------
    // Classificacao Routes
    // -------------------------------------------------------------------------

    if ($path === '/classificacao' && $method === 'GET') {
        require_once __DIR__ . '/classificacao.php';
        index();
    }

    if ($path === '/classificacao' && $method === 'POST') {
        require_once __DIR__ . '/classificacao.php';
        store();
    }

    if (preg_match('#^/classificacao/(\d+)$#', $path, $matches) && $method === 'PUT') {
        require_once __DIR__ . '/classificacao.php';
        update((int)$matches[1]);
    }

    if (preg_match('#^/classificacao/(\d+)$#', $path, $matches) && $method === 'DELETE') {
        require_once __DIR__ . '/classificacao.php';
        destroy((int)$matches[1]);
    }

    // -------------------------------------------------------------------------
    // Excecoes Routes
    // -------------------------------------------------------------------------

    if ($path === '/excecoes' && $method === 'GET') {
        require_once __DIR__ . '/classificacao.php';
        indexExcecoes();
    }

    if ($path === '/excecoes' && $method === 'POST') {
        require_once __DIR__ . '/classificacao.php';
        storeExcecao();
    }

    if (preg_match('#^/excecoes/([A-Za-z0-9_]+)$#', $path, $matches) && $method === 'DELETE') {
        require_once __DIR__ . '/classificacao.php';
        destroyExcecao($matches[1]);
    }

    // -------------------------------------------------------------------------
    // Parametros Routes
    // -------------------------------------------------------------------------

    if ($path === '/parametros' && $method === 'GET') {
        require_once __DIR__ . '/parametros.php';
        index();
    }

    if ($path === '/parametros' && $method === 'PUT') {
        require_once __DIR__ . '/parametros.php';
        update();
    }

    if ($path === '/parametros/categorias' && $method === 'GET') {
        require_once __DIR__ . '/parametros.php';
        categorias();
    }

    // -------------------------------------------------------------------------
    // Estudos (Jobs) Routes
    // -------------------------------------------------------------------------

    if ($path === '/estudos' && $method === 'GET') {
        require_once __DIR__ . '/estudos.php';
        index();
    }

    if ($path === '/estudos' && $method === 'POST') {
        require_once __DIR__ . '/estudos.php';
        store();
    }

    if (preg_match('#^/estudos/([a-f0-9-]{36})$#', $path, $matches) && $method === 'GET') {
        require_once __DIR__ . '/estudos.php';
        show($matches[1]);
    }

    if (preg_match('#^/estudos/([a-f0-9-]{36})/cancelar$#', $path, $matches) && $method === 'POST') {
        require_once __DIR__ . '/estudos.php';
        cancelar($matches[1]);
    }

    if (preg_match('#^/estudos/([a-f0-9-]{36})$#', $path, $matches) && $method === 'DELETE') {
        require_once __DIR__ . '/estudos.php';
        destroy($matches[1]);
    }

    // -------------------------------------------------------------------------
    // Resultados Routes
    // -------------------------------------------------------------------------

    if (preg_match('#^/resultados/([a-f0-9-]{36})/serie$#', $path, $matches) && $method === 'GET') {
        require_once __DIR__ . '/resultados.php';
        serie($matches[1]);
    }

    if (preg_match('#^/resultados/([a-f0-9-]{36})/tabela$#', $path, $matches) && $method === 'GET') {
        require_once __DIR__ . '/resultados.php';
        tabela($matches[1]);
    }

    if (preg_match('#^/resultados/([a-f0-9-]{36})/stats$#', $path, $matches) && $method === 'GET') {
        require_once __DIR__ . '/resultados.php';
        stats($matches[1]);
    }

    if (preg_match('#^/resultados/([a-f0-9-]{36})/logs$#', $path, $matches) && $method === 'GET') {
        require_once __DIR__ . '/resultados.php';
        logs($matches[1]);
    }

    if (preg_match('#^/resultados/([a-f0-9-]{36})/exportar$#', $path, $matches) && $method === 'GET') {
        require_once __DIR__ . '/resultados.php';
        exportar($matches[1]);
    }

    // -------------------------------------------------------------------------
    // Ingestão Routes
    // -------------------------------------------------------------------------

    if ($path === '/ingestao' && $method === 'GET') {
        require_once __DIR__ . '/ingestao.php';
        index();
    }

    if ($path === '/ingestao' && $method === 'POST') {
        require_once __DIR__ . '/ingestao.php';
        store();
    }

    if (preg_match('#^/ingestao/mes/(\d{6})$#', $path, $matches) && $method === 'DELETE') {
        require_once __DIR__ . '/ingestao.php';
        destroyMes($matches[1]);
    }

    // -------------------------------------------------------------------------
    // Explorador Routes
    // -------------------------------------------------------------------------

    if ($path === '/explorador/overview' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        overview();
    }

    if ($path === '/explorador/distribuicao' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        distribuicao();
    }

    if ($path === '/explorador/histograma' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        histograma();
    }

    if ($path === '/explorador/perfil-horario' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        perfil_horario();
    }

    if ($path === '/explorador/top-unidades' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        top_unidades();
    }

    if ($path === '/explorador/categorias' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        categorias();
    }

    if ($path === '/explorador/tendencia-mensal' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        tendencia_mensal();
    }

    if ($path === '/explorador/dispersao' && $method === 'GET') {
        require_once __DIR__ . '/explorador.php';
        dispersao();
    }

    if ($path === '/explorador/query' && $method === 'POST') {
        require_once __DIR__ . '/explorador.php';
        query_custom();
    }

    // -------------------------------------------------------------------------
    // 404 - Route Not Found
    // -------------------------------------------------------------------------

    error_response("Route not found: {$method} {$path}", 404);

} catch (\RuntimeException $e) {
    error_response($e->getMessage(), 500);
} catch (\Exception $e) {
    error_response('Internal server error: ' . $e->getMessage(), 500);
} catch (\Throwable $e) {
    error_response('Fatal error: ' . $e->getMessage(), 500);
}
