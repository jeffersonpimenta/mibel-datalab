<?php
/**
 * MIBEL Platform - Explorador de Dados API
 *
 * Todos os endpoints aceitam filtros opcionais via query string:
 *   de   (YYYY-MM-DD) — data início
 *   ate  (YYYY-MM-DD) — data fim
 *   pais (MI|ES|PT)   — filtrar por país
 *   tipo (C|V)        — tipo de oferta
 */

declare(strict_types=1);

// ============================================================================
// Helpers de filtro
// ============================================================================

function exp_filters(): array
{
    $conditions = [];

    $de   = preg_replace('/[^0-9\-]/', '', get_param('de', ''));
    $ate  = preg_replace('/[^0-9\-]/', '', get_param('ate', ''));
    $pais = get_param('pais', '');
    $tipo = get_param('tipo', '');

    if ($de !== '') {
        $conditions[] = "data_ficheiro >= '{$de}'";
    }
    if ($ate !== '') {
        $conditions[] = "data_ficheiro <= '{$ate}'";
    }
    if (in_array($pais, ['MI', 'ES', 'PT'], true)) {
        $conditions[] = "pais = '{$pais}'";
    }
    if (in_array($tipo, ['C', 'V'], true)) {
        $conditions[] = "tipo_oferta = '{$tipo}'";
    }

    return $conditions;
}

function exp_where(array $conditions): string
{
    return empty($conditions) ? '1=1' : implode(' AND ', $conditions);
}

// ============================================================================
// GET /api/explorador/overview
// ============================================================================

function overview(): void
{
    $db    = Database::getInstance();
    $where = exp_where(exp_filters());

    $stats = $db->query("
        SELECT
            count()                                        AS total_bids,
            countDistinct(unidade)                         AS n_unidades,
            sum(energia)                                   AS total_energia,
            avg(precio)                                    AS preco_medio,
            min(precio)                                    AS preco_min,
            max(precio)                                    AS preco_max,
            toString(min(data_ficheiro))                   AS data_inicio,
            toString(max(data_ficheiro))                   AS data_fim,
            countDistinct(toYYYYMM(data_ficheiro))         AS n_meses,
            countIf(tipo_oferta = 'V')                     AS n_bids_venda,
            countIf(tipo_oferta = 'C')                     AS n_bids_compra
        FROM mibel.bids_raw
        WHERE {$where}
    ");

    json_response($stats[0] ?? []);
}

// ============================================================================
// GET /api/explorador/distribuicao
// ============================================================================

function distribuicao(): void
{
    $db    = Database::getInstance();
    $where = exp_where(exp_filters());

    $rows = $db->query("
        SELECT
            pais,
            tipo_oferta,
            count()          AS n_bids,
            sum(energia)     AS total_energia,
            avg(precio)      AS preco_medio,
            min(precio)      AS preco_min,
            max(precio)      AS preco_max
        FROM mibel.bids_raw
        WHERE {$where}
        GROUP BY pais, tipo_oferta
        ORDER BY pais, tipo_oferta
    ");

    json_response(['por_pais' => $rows]);
}

// ============================================================================
// GET /api/explorador/histograma
// ============================================================================

function histograma(): void
{
    $db    = Database::getInstance();
    $where = exp_where(exp_filters());

    $rows = $db->query("
        SELECT
            multiIf(
                precio < 0,   'Negativo',
                precio < 20,  '0–20',
                precio < 40,  '20–40',
                precio < 60,  '40–60',
                precio < 80,  '60–80',
                precio < 100, '80–100',
                precio < 120, '100–120',
                precio < 140, '120–140',
                precio < 160, '140–160',
                precio < 180, '160–180',
                precio < 200, '180–200',
                '>200'
            ) AS faixa,
            multiIf(
                precio < 0,   -1,
                precio < 20,   0,
                precio < 40,   1,
                precio < 60,   2,
                precio < 80,   3,
                precio < 100,  4,
                precio < 120,  5,
                precio < 140,  6,
                precio < 160,  7,
                precio < 180,  8,
                precio < 200,  9,
                10
            ) AS ordem,
            count()          AS n_bids,
            sum(energia)     AS total_energia
        FROM mibel.bids_raw
        WHERE {$where}
        GROUP BY faixa, ordem
        ORDER BY ordem
    ");

    json_response($rows);
}

// ============================================================================
// GET /api/explorador/perfil-horario
// ============================================================================

function perfil_horario(): void
{
    $db    = Database::getInstance();
    $where = exp_where(exp_filters());

    $rows = $db->query("
        SELECT
            hora_num,
            pais,
            avg(precio)      AS preco_medio,
            avg(energia)     AS energia_media,
            sum(energia)     AS total_energia,
            count()          AS n_bids
        FROM mibel.bids_raw
        WHERE {$where}
        GROUP BY hora_num, pais
        ORDER BY hora_num, pais
    ");

    json_response($rows);
}

// ============================================================================
// GET /api/explorador/top-unidades
// ============================================================================

function top_unidades(): void
{
    $db      = Database::getInstance();
    $where   = exp_where(exp_filters());
    $limit   = max(5, min(50, (int)get_param('limit', 25)));
    $sort    = get_param('sort', 'energia');
    $orderBy = $sort === 'bids' ? 'n_bids' : 'total_energia';

    // Try with JOIN to mibel.unidades; fall back to bids_raw-only if it fails
    try {
        $rows = $db->query("
            SELECT
                b.unidade,
                any(u.descricao)     AS descricao,
                any(u.regime)        AS regime,
                any(u.categoria)     AS categoria,
                any(u.zona_frontera) AS zona_frontera,
                count()              AS n_bids,
                sum(b.energia)       AS total_energia,
                avg(b.precio)        AS preco_medio,
                min(b.precio)        AS preco_min,
                max(b.precio)        AS preco_max
            FROM mibel.bids_raw b
            LEFT JOIN mibel.unidades u ON b.unidade = u.codigo
            WHERE {$where}
            GROUP BY b.unidade
            ORDER BY {$orderBy} DESC
            LIMIT {$limit}
        ");
    } catch (\Exception $e) {
        // Fallback sem JOIN
        $rows = $db->query("
            SELECT
                unidade,
                ''        AS descricao,
                ''        AS regime,
                ''        AS categoria,
                ''        AS zona_frontera,
                count()   AS n_bids,
                sum(energia)  AS total_energia,
                avg(precio)   AS preco_medio,
                min(precio)   AS preco_min,
                max(precio)   AS preco_max
            FROM mibel.bids_raw
            WHERE {$where}
            GROUP BY unidade
            ORDER BY {$orderBy} DESC
            LIMIT {$limit}
        ");
    }

    json_response($rows);
}

// ============================================================================
// GET /api/explorador/categorias
// ============================================================================

function categorias(): void
{
    $db    = Database::getInstance();
    $where = exp_where(exp_filters());

    try {
        $rows = $db->query("
            SELECT
                coalesce(nullIf(u.categoria, ''), 'SEM_CATEGORIA')  AS categoria,
                coalesce(nullIf(u.regime, ''), 'DESCONHECIDO')      AS regime,
                count()                    AS n_bids,
                countDistinct(b.unidade)   AS n_unidades,
                sum(b.energia)             AS total_energia,
                avg(b.precio)              AS preco_medio
            FROM mibel.bids_raw b
            LEFT JOIN mibel.unidades u ON b.unidade = u.codigo
            WHERE {$where}
            GROUP BY categoria, regime
            ORDER BY total_energia DESC
            LIMIT 30
        ");
    } catch (\Exception $e) {
        // Fallback sem JOIN
        $rows = $db->query("
            SELECT
                'SEM_CATEGORIA'          AS categoria,
                'DESCONHECIDO'           AS regime,
                count()                  AS n_bids,
                countDistinct(unidade)   AS n_unidades,
                sum(energia)             AS total_energia,
                avg(precio)              AS preco_medio
            FROM mibel.bids_raw
            WHERE {$where}
            LIMIT 1
        ");
    }

    json_response($rows);
}

// ============================================================================
// GET /api/explorador/tendencia-mensal
// ============================================================================

function tendencia_mensal(): void
{
    $db    = Database::getInstance();
    $where = exp_where(exp_filters());

    $rows = $db->query("
        SELECT
            toString(toStartOfMonth(data_ficheiro))  AS mes,
            toYYYYMM(data_ficheiro)                  AS mes_num,
            count()                                   AS n_bids,
            sum(energia)                              AS total_energia,
            avg(precio)                               AS preco_medio,
            countDistinct(unidade)                    AS n_unidades
        FROM mibel.bids_raw
        WHERE {$where}
        GROUP BY mes, mes_num
        ORDER BY mes_num
    ");

    json_response($rows);
}

// ============================================================================
// GET /api/explorador/dispersao-preco-energia
// ============================================================================

function dispersao(): void
{
    $db    = Database::getInstance();
    $conds = exp_filters();
    $pais  = get_param('pais', 'MI');
    if (!in_array($pais, ['MI', 'ES', 'PT'], true)) {
        $pais = 'MI';
    }

    // Aggregate by (hora_num, categoria): media de precio vs soma de energia
    // Return 500 points max — aggregate by category+hour
    $conds[] = "pais = '{$pais}'";
    $where    = exp_where($conds);

    $rows = $db->query("
        SELECT
            b.hora_num,
            coalesce(nullIf(u.categoria, ''), 'SEM_CATEGORIA') AS categoria,
            avg(b.precio)    AS preco_medio,
            sum(b.energia)   AS total_energia,
            count()          AS n_bids
        FROM mibel.bids_raw b
        LEFT JOIN mibel.unidades u ON b.unidade = u.codigo
        WHERE {$where}
        GROUP BY b.hora_num, categoria
        ORDER BY total_energia DESC
        LIMIT 200
    ");

    json_response($rows);
}

// ============================================================================
// POST /api/explorador/query
// ============================================================================

function query_custom(): void
{
    $db   = Database::getInstance();
    $body = request_body();
    $sql  = trim($body['sql'] ?? '');

    if (empty($sql)) {
        error_response('SQL vazio.', 400);
    }

    // Only allow SELECT / WITH statements
    if (!preg_match('/^\s*(SELECT|WITH)\s/i', $sql)) {
        error_response('Apenas queries SELECT são permitidas.', 403);
    }

    // Block dangerous DDL / DML keywords (word-boundary match)
    $blocked = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE',
                'TRUNCATE', 'RENAME', 'ATTACH', 'DETACH', 'OPTIMIZE',
                'KILL', 'SYSTEM', 'GRANT', 'REVOKE'];
    foreach ($blocked as $kw) {
        if (preg_match('/\b' . $kw . '\b/i', $sql)) {
            error_response("Keyword '{$kw}' não é permitida.", 403);
        }
    }

    // Auto-add LIMIT if missing
    if (!preg_match('/\bLIMIT\s+\d+/i', $sql)) {
        $sql .= "\nLIMIT 200";
    }

    try {
        $rows = $db->query($sql);
        json_response(['rows' => $rows, 'count' => count($rows)]);
    } catch (\Exception $e) {
        error_response('Erro ClickHouse: ' . $e->getMessage(), 400);
    }
}
