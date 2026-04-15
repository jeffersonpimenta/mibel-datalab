<?php
/**
 * MIBEL Platform - Database Migration Script
 *
 * Creates ClickHouse tables, SQLite database, and default config files.
 * Safe to re-run (idempotent).
 *
 * Usage: php /var/www/html/src/migrate.php
 */

declare(strict_types=1);

// Configuration
$clickhouseHost = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$clickhousePort = 8123;
$clickhouseDb = 'mibel';

$dataDir = '/data';
$configDir = $dataDir . '/config';
$sqliteDbPath = $dataDir . '/jobs.db';

// ============================================================================
// Helper Functions
// ============================================================================

function clickhouseQuery(string $host, int $port, string $query, string $database = ''): array
{
    $url = "http://{$host}:{$port}/";
    if ($database) {
        $url .= "?database=" . urlencode($database);
    }

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $query,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => ['Content-Type: text/plain'],
        CURLOPT_TIMEOUT => 30,
    ]);

    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    return [
        'success' => $httpCode === 200,
        'response' => $response,
        'error' => $error ?: ($httpCode !== 200 ? $response : ''),
        'http_code' => $httpCode,
    ];
}

function printStatus(bool $success, string $message): void
{
    $prefix = $success ? '[OK]' : '[ERRO]';
    echo "{$prefix} {$message}\n";
}

// ============================================================================
// Default Configuration Data
// ============================================================================

$classificacaoDefault = [
    ["tecnologia" => "RE Mercado Solar Fotovoltáica",  "regime" => "PRE", "categoria" => "SOLAR_FOT"],
    ["tecnologia" => "RE Mercado Solar Térmica",        "regime" => "PRE", "categoria" => "SOLAR_TER"],
    ["tecnologia" => "RE Mercado Eólica",               "regime" => "PRE", "categoria" => "EOLICA"],
    ["tecnologia" => "RE Mercado Eólica Marina",        "regime" => "PRE", "categoria" => "EOLICA_MARINA"],
    ["tecnologia" => "RE Mercado Hidráulica",           "regime" => "PRE", "categoria" => "HIDRICA"],
    ["tecnologia" => "RE Mercado Térmica Renovable",    "regime" => "PRE", "categoria" => "TERMICA_RENOV"],
    ["tecnologia" => "RE Mercado Térmica no Renovab.",  "regime" => "PRE", "categoria" => "TERMICA_NREN"],
    ["tecnologia" => "RE Mercado Geotérmica",           "regime" => "PRE", "categoria" => "GEOTERMICA"],
    ["tecnologia" => "Híbrida Renovable",               "regime" => "PRE", "categoria" => "HIBRIDA_RENOV"],
    ["tecnologia" => "Híbrida Renov.-Almacenamiento",   "regime" => "PRE", "categoria" => "HIBRIDA_RENOV"],
    ["tecnologia" => "Híbrida Renov.-Térmica",          "regime" => "PRE", "categoria" => "HIBRIDA_RENOV"],
    ["tecnologia" => "RE Tarifa CUR (uof)",             "regime" => "PRE", "categoria" => "RE_TARIFA_CUR"],
    ["tecnologia" => "RE Tar. CUR Eólica",              "regime" => "PRE", "categoria" => "RE_TARIFA_CUR"],
    ["tecnologia" => "RE Tar. CUR Solar Fotovoltáica",  "regime" => "PRE", "categoria" => "RE_TARIFA_CUR"],
    ["tecnologia" => "RE Tar. CUR Solar Térmica",       "regime" => "PRE", "categoria" => "RE_TARIFA_CUR"],
    ["tecnologia" => "RE Tar. CUR Hidráulica",          "regime" => "PRE", "categoria" => "RE_TARIFA_CUR"],
    ["tecnologia" => "RE Tar. CUR Térmica Renovable",   "regime" => "PRE", "categoria" => "RE_TARIFA_CUR"],
    ["tecnologia" => "RE Tar. CUR Térmica no Renov.",   "regime" => "PRE", "categoria" => "RE_TARIFA_CUR"],
    ["tecnologia" => "Agente vendedor Reg. Especial",   "regime" => "PRE", "categoria" => "RE_OUTRO"],
    ["tecnologia" => "Almacenamiento Venta",            "regime" => "PRE", "categoria" => "ARMAZENAMENTO_VENDA"],
    ["tecnologia" => "Ciclo Combinado",                 "regime" => "PRO", "categoria" => "CICLO_COMBINADO"],
    ["tecnologia" => "Nuclear",                         "regime" => "PRO", "categoria" => "NUCLEAR"],
    ["tecnologia" => "Hidráulica Generación",           "regime" => "PRO", "categoria" => "HIDRICA_PRO"],
    ["tecnologia" => "Hidráulica de Bombeo Puro",       "regime" => "PRO", "categoria" => "BOMBEO_PURO_PRO"],
    ["tecnologia" => "Hulla Antracita",                 "regime" => "PRO", "categoria" => "CARVAO"],
    ["tecnologia" => "Carbón de Importación",           "regime" => "PRO", "categoria" => "CARVAO"],
    ["tecnologia" => "Gas",                             "regime" => "PRO", "categoria" => "GAS"],
    ["tecnologia" => "Consumo Bombeo Mixto",            "regime" => "CONSUMO", "categoria" => "BOMBEO_CONSUMO"],
    ["tecnologia" => "Consumo Bombeo Puro",             "regime" => "CONSUMO", "categoria" => "BOMBEO_CONSUMO"],
    ["tecnologia" => "Consumo de bombeo",               "regime" => "CONSUMO", "categoria" => "BOMBEO_CONSUMO"],
    ["tecnologia" => "Almacenamiento Compra",           "regime" => "CONSUMO", "categoria" => "ARMAZENAMENTO_COMPRA"],
    ["tecnologia" => "Compras Consumo Directo",         "regime" => "CONSUMO", "categoria" => "CONS_DIRECTO"],
    ["tecnologia" => "Consumidor directo",              "regime" => "CONSUMO", "categoria" => "CONS_DIRECTO"],
    ["tecnologia" => "Compras Cons. Directo Balance",   "regime" => "CONSUMO", "categoria" => "CONS_DIRECTO"],
    ["tecnologia" => "Rep. de consumidores directos",   "regime" => "CONSUMO", "categoria" => "CONS_DIRECTO"],
    ["tecnologia" => "Compras Consumos Auxiliares",     "regime" => "CONSUMO", "categoria" => "CONS_AUXILIARES"],
    ["tecnologia" => "Rep. Consumos Auxiliares",        "regime" => "CONSUMO", "categoria" => "CONS_AUXILIARES"],
    ["tecnologia" => "Consumo de productores",          "regime" => "CONSUMO", "categoria" => "CONS_PRODUTOR"],
    ["tecnologia" => "Comercializador",                 "regime" => "COMERCIALIZADOR", "categoria" => "COMERC"],
    ["tecnologia" => "Comercializador no residente",    "regime" => "COMERCIALIZADOR", "categoria" => "COMERC_NR"],
    ["tecnologia" => "Compras Comercialización",        "regime" => "COMERCIALIZADOR", "categoria" => "COMERC"],
    ["tecnologia" => "Compra Comercializador Balance",  "regime" => "COMERCIALIZADOR", "categoria" => "COMERC"],
    ["tecnologia" => "Import. de agentes externos",     "regime" => "COMERCIALIZADOR", "categoria" => "COMERC_EXT"],
    ["tecnologia" => "Import. de comercializadoras",    "regime" => "COMERCIALIZADOR", "categoria" => "COMERC_EXT"],
    ["tecnologia" => "Rep. de comercializadores",       "regime" => "COMERCIALIZADOR", "categoria" => "COMERC"],
    ["tecnologia" => "Comercializador ultimo recurso",  "regime" => "COMERCIALIZADOR", "categoria" => "COMERC_ULT_REC"],
    ["tecnologia" => "Unidad Generica",                 "regime" => "GENERICA", "categoria" => "GENERICA"],
    ["tecnologia" => "VENTA GENERICA",                  "regime" => "GENERICA", "categoria" => "GENERICA_VENDA"],
    ["tecnologia" => "Porfolio Produccion Compra",      "regime" => "PORFOLIO", "categoria" => "PORTF_PROD"],
    ["tecnologia" => "Porfolio Produccion Venta",       "regime" => "PORFOLIO", "categoria" => "PORTF_PROD"],
    ["tecnologia" => "Porfolio Comerc. Compra",         "regime" => "PORFOLIO", "categoria" => "PORTF_COMERC"],
    ["tecnologia" => "Porfolio Comerc. Venta",          "regime" => "PORFOLIO", "categoria" => "PORTF_COMERC"],
];

$excecoesDefault = [
    ["codigo" => "ACCGV02", "categoria_zona" => "SOLAR_FOT_PT", "motivo" => "Unidade PT classificada como ES pelo OMEL"],
    ["codigo" => "AGPRCTI", "categoria_zona" => "SOLAR_FOT_PT", "motivo" => ""],
    ["codigo" => "AGPRSUL", "categoria_zona" => "SOLAR_FOT_PT", "motivo" => ""],
    ["codigo" => "EGLEV2",  "categoria_zona" => "SOLAR_FOT_PT", "motivo" => ""],
    ["codigo" => "IGNIV02", "categoria_zona" => "SOLAR_FOT_PT", "motivo" => ""],
    ["codigo" => "JAFPV02", "categoria_zona" => "SOLAR_FOT_PT", "motivo" => ""],
    ["codigo" => "AAXRSUL", "categoria_zona" => "EOLICA_PT",    "motivo" => ""],
    ["codigo" => "EDPGPV2", "categoria_zona" => "EOLICA_PT",    "motivo" => ""],
    ["codigo" => "GRENV02", "categoria_zona" => "EOLICA_PT",    "motivo" => ""],
    ["codigo" => "INATV02", "categoria_zona" => "EOLICA_PT",    "motivo" => ""],
    ["codigo" => "LEZIV02", "categoria_zona" => "EOLICA_PT",    "motivo" => ""],
    ["codigo" => "MUONV02", "categoria_zona" => "EOLICA_PT",    "motivo" => ""],
    ["codigo" => "TSRDV02", "categoria_zona" => "EOLICA_PT",    "motivo" => ""],
];

$parametrosDefault = [
    "PRE" => [
        "SOLAR_FOT_ES" => ["escala" => 2.2297, "escaloes" => [["preco" => 0.0, "pct_bids" => 0.30], ["preco" => 20.0, "pct_bids" => 0.30], ["preco" => 35.0, "pct_bids" => 0.40]]],
        "SOLAR_FOT_PT" => ["escala" => 3.6879, "escaloes" => [["preco" => 0.0, "pct_bids" => 0.30], ["preco" => 20.0, "pct_bids" => 0.30], ["preco" => 35.0, "pct_bids" => 0.40]]],
        "SOLAR_TER_ES" => ["escala" => 2.0869, "escaloes" => [["preco" => 40.0, "pct_bids" => 1.00]]],
        "EOLICA_ES" => ["escala" => 1.8857, "escaloes" => [["preco" => 50.0, "pct_bids" => 0.50], ["preco" => 70.0, "pct_bids" => 0.50]]],
        "EOLICA_PT" => ["escala" => 2.1379, "escaloes" => [["preco" => 50.0, "pct_bids" => 0.50], ["preco" => 70.0, "pct_bids" => 0.50]]],
        "EOLICA_MARINA_ES" => ["escala" => 1.0, "escaloes" => [["preco" => 0.0, "pct_bids" => 1.00]]],
        "HIDRICA_ES" => ["escala" => 1.0, "escaloes" => [["preco" => 15.0, "pct_bids" => 1.00]]],
        "TERMICA_RENOV_ES" => ["escala" => 1.4, "escaloes" => [["preco" => 10.0, "pct_bids" => 1.00]]],
        "TERMICA_RENOV_PT" => ["escala" => 1.5348, "escaloes" => [["preco" => 10.0, "pct_bids" => 1.00]]],
        "TERMICA_NREN_ES" => ["escala" => 1.0, "escaloes" => [["preco" => 60.0, "pct_bids" => 1.00]]],
        "GEOTERMICA_ES" => ["escala" => 1.0, "escaloes" => [["preco" => 30.0, "pct_bids" => 1.00]]],
        "HIBRIDA_RENOV_ES" => ["escala" => 1.0, "escaloes" => [["preco" => 45.0, "pct_bids" => 1.00]]],
        "RE_TARIFA_CUR_ES" => ["escala" => 1.7, "escaloes" => [["preco" => 0.0, "pct_bids" => 1.00]]],
        "RE_TARIFA_CUR_PT" => ["escala" => 1.7, "escaloes" => [["preco" => 0.0, "pct_bids" => 1.00]]],
        "RE_OUTRO_ES" => ["escala" => 1.0, "escaloes" => [["preco" => 0.0, "pct_bids" => 1.00]]],
        "ARMAZENAMENTO_VENDA_ES" => ["escala" => 1.0, "escaloes" => [["preco" => 0.0, "pct_bids" => 1.00]]],
        "ARMAZENAMENTO_VENDA_PT" => ["escala" => 1.0, "escaloes" => [["preco" => 0.0, "pct_bids" => 1.00]]],
    ],
    "PRO" => [
        "BOMBEO_PURO_PRO_ES" => ["escala" => 1.0],
        "CARVAO_ES" => ["escala" => 1.0],
        "CICLO_COMBINADO_ES" => ["escala" => 1.0],
        "CICLO_COMBINADO_PT" => ["escala" => 0.7143],
        "GAS_ES" => ["escala" => 1.0],
        "HIDRICA_PRO_ES" => ["escala" => 1.0],
        "HIDRICA_PRO_PT" => ["escala" => 1.0],
        "NUCLEAR_ES" => ["escala" => 0.447],
    ],
    "CONSUMO" => [
        "ARMAZENAMENTO_COMPRA_ES" => ["escala" => 1.0],
        "ARMAZENAMENTO_COMPRA_PT" => ["escala" => 1.0],
        "BOMBEO_CONSUMO_ES" => ["escala" => 1.6926],
        "BOMBEO_CONSUMO_PT" => ["escala" => 1.3871],
        "CONS_AUXILIARES_ES" => ["escala" => 1.0],
        "CONS_DIRECTO_ES" => ["escala" => 1.6926],
        "CONS_DIRECTO_EXT" => ["escala" => 1.6926],
        "CONS_PRODUTOR_ES" => ["escala" => 1.0],
    ],
    "COMERCIALIZADOR" => [
        "COMERC_ES" => ["escala" => 1.3871],
        "COMERC_EXT" => ["escala" => 1.0],
        "COMERC_NR_EXT" => ["escala" => 1.0],
        "COMERC_PT" => ["escala" => 1.6926],
        "COMERC_ULT_REC_ES" => ["escala" => 1.0],
        "COMERC_ULT_REC_PT" => ["escala" => 1.0],
    ],
    "GENERICA" => [
        "GENERICA_ES" => ["escala" => 1.3871],
        "GENERICA_PT" => ["escala" => 1.6926],
        "GENERICA_VENDA_ES" => ["escala" => 1.3871],
        "GENERICA_VENDA_PT" => ["escala" => 1.6926],
    ],
    "PORFOLIO" => [
        "PORTF_COMERC_ES" => ["escala" => 1.0],
        "PORTF_PROD_ES" => ["escala" => 1.0],
        "PORTF_PROD_PT" => ["escala" => 1.0],
    ],
];

// ============================================================================
// Main Migration Script
// ============================================================================

echo "=== MIBEL Platform Migration ===\n\n";

// Step 1: Create ClickHouse database
echo "1. ClickHouse Database\n";
$result = clickhouseQuery($clickhouseHost, $clickhousePort, 'CREATE DATABASE IF NOT EXISTS mibel');
printStatus($result['success'], "Create database 'mibel'" . ($result['success'] ? '' : " - {$result['error']}"));

$clickhouseOk = $result['success'];
if (!$clickhouseOk) {
    echo "\n[AVISO] ClickHouse indisponível — as tabelas não serão criadas agora.\n";
    echo "        Execute novamente após o ClickHouse estar pronto.\n";
}

// Step 2: Create ClickHouse tables
echo "\n2. ClickHouse Tables\n";

if (!$clickhouseOk) {
    echo "[IGNORADO] ClickHouse indisponível.\n";
}

$clickhouseTables = [
    'bids_raw' => "
        CREATE TABLE IF NOT EXISTS mibel.bids_raw (
            data_ficheiro   Date,
            ficheiro_nome   String,
            zip_nome        String,
            hora_raw        String,
            hora_num        UInt8,
            periodo_formato String,
            pais            String,
            tipo_oferta     FixedString(1),
            unidade         String,
            energia         Float64,
            precio          Float64,
            ingestao_ts     DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(data_ficheiro)
        ORDER BY (data_ficheiro, hora_num, pais, tipo_oferta, unidade)
    ",
    'clearing_substituicao' => "
        CREATE TABLE IF NOT EXISTS mibel.clearing_substituicao (
            job_id                  String,
            data_ficheiro           String,
            data_date               Date,
            hora_raw                String,
            hora_num                UInt8,
            pais                    String,
            preco_clearing_orig     Nullable(Float64),
            volume_clearing_orig    Nullable(Float64),
            preco_clearing_sub      Nullable(Float64),
            volume_clearing_sub     Nullable(Float64),
            delta_preco             Nullable(Float64),
            n_bids_substituidos     UInt32,
            created_at              DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(data_date)
        ORDER BY (job_id, data_date, hora_num, pais)
    ",
    'clearing_substituicao_logs' => "
        CREATE TABLE IF NOT EXISTS mibel.clearing_substituicao_logs (
            job_id          String,
            data_ficheiro   String,
            data_date       Date,
            hora_raw        String,
            hora_num        UInt8,
            pais            String,
            unidade         String,
            categoria       String,
            escalao_preco   Float64,
            preco_original  Float64,
            energia_mw      Float64,
            created_at      DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(data_date)
        ORDER BY (job_id, data_date, hora_num, pais, unidade)
    ",
    'clearing_otimizacao' => "
        CREATE TABLE IF NOT EXISTS mibel.clearing_otimizacao (
            job_id                      String,
            data_ficheiro               String,
            data_date                   Date,
            hora_raw                    String,
            hora_num                    UInt8,
            pais                        String,
            preco_clearing_orig         Nullable(Float64),
            volume_clearing_orig        Nullable(Float64),
            preco_clearing_base         Nullable(Float64),
            volume_clearing_base        Nullable(Float64),
            preco_clearing_opt          Nullable(Float64),
            volume_clearing_opt         Nullable(Float64),
            vol_pre_despachado_base     Float64,
            lucro_pre_base              Float64,
            vol_pre_despachado_opt      Float64,
            lucro_pre_opt               Float64,
            delta_preco                 Nullable(Float64),
            delta_vol_pre_despachado    Float64,
            delta_lucro_pre             Float64,
            delta_lucro_pre_pct         Nullable(Float64),
            vol_pre_removido_opt        Float64,
            n_bids_pre_removidos        UInt32,
            unidades_pre_despachadas    String,
            n_cenarios_testados         UInt32,
            created_at                  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(data_date)
        ORDER BY (job_id, data_date, hora_num, pais)
    ",
    'clearing_otimizacao_logs' => "
        CREATE TABLE IF NOT EXISTS mibel.clearing_otimizacao_logs (
            job_id           String,
            data_ficheiro    String,
            data_date        Date,
            hora_raw         String,
            hora_num         UInt8,
            pais             String,
            cenario          String,
            preco_clearing   Nullable(Float64),
            volume_clearing  Nullable(Float64),
            lucro_pre        Float64,
            n_bids_removidos UInt32,
            vol_removido     Float64,
            created_at       DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(data_date)
        ORDER BY (job_id, data_date, hora_num, pais, cenario)
    ",
    'worker_logs' => "
        CREATE TABLE IF NOT EXISTS mibel.worker_logs (
            job_id        String,
            nivel         String,
            mensagem      String,
            ts            DateTime64(3) DEFAULT now64()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(toDate(ts))
        ORDER BY (job_id, ts)
    ",
    'unidades' => "
        CREATE TABLE IF NOT EXISTS mibel.unidades (
            codigo          String,
            descricao       String,
            agente          String,
            tipo_unidad     String,
            zona_frontera   String,
            tecnologia      String,
            regime          String,
            categoria       String,
            atualizado_em   DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(atualizado_em)
        ORDER BY (codigo)
    ",
];

if ($clickhouseOk) {
    foreach ($clickhouseTables as $tableName => $createSql) {
        $result = clickhouseQuery($clickhouseHost, $clickhousePort, $createSql, 'mibel');
        printStatus($result['success'], "Create table '{$tableName}'" . ($result['success'] ? '' : " - {$result['error']}"));
    }
}

// Step 3: Create data directories
echo "\n3. Data Directories\n";

$directories = [$dataDir, $configDir, "$dataDir/bids", "$dataDir/outputs"];
foreach ($directories as $dir) {
    if (!is_dir($dir)) {
        $created = @mkdir($dir, 0755, true);
        printStatus($created, "Create directory '{$dir}'");
    } else {
        printStatus(true, "Directory '{$dir}' exists");
    }
}

// Step 4: Create SQLite database
echo "\n4. SQLite Database\n";

try {
    $pdo = new PDO("sqlite:{$sqliteDbPath}");
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Create jobs table
    $pdo->exec("
        CREATE TABLE IF NOT EXISTS jobs (
            id          TEXT PRIMARY KEY,
            tipo        TEXT NOT NULL,
            data_inicio TEXT NOT NULL,
            data_fim    TEXT NOT NULL,
            observacoes TEXT DEFAULT '',
            workers_n   INTEGER DEFAULT 4,
            status      TEXT DEFAULT 'PENDING',
            resultado   TEXT DEFAULT '',
            erro        TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now')),
            started_at  TEXT,
            finished_at TEXT
        )
    ");
    printStatus(true, "Create table 'jobs'");

    // Create bids_ingeridos table
    $pdo->exec("
        CREATE TABLE IF NOT EXISTS bids_ingeridos (
            data_ficheiro TEXT PRIMARY KEY,
            n_bids        INTEGER,
            ingerido_em   TEXT DEFAULT (datetime('now'))
        )
    ");
    printStatus(true, "Create table 'bids_ingeridos'");

} catch (PDOException $e) {
    printStatus(false, "SQLite error: " . $e->getMessage());
    exit(1);
}

// Step 5: Create / seed config files
echo "\n5. Config Files\n";

// parametros.json — create only if missing (user may customise scales/escalões)
$parametrosPath = "{$configDir}/parametros.json";
if (!file_exists($parametrosPath)) {
    $written = file_put_contents(
        $parametrosPath,
        json_encode($parametrosDefault, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE)
    );
    printStatus($written !== false, "Create 'parametros.json'");
} else {
    printStatus(true, "'parametros.json' already exists (skipped)");
}

// classificacao.json — merge: add any default entries not yet present (by tecnologia)
$classPath = "{$configDir}/classificacao.json";
$existingClass = file_exists($classPath)
    ? (json_decode(file_get_contents($classPath), true) ?: [])
    : [];

$existingTecnologias = array_map(
    fn($e) => strtolower($e['tecnologia']),
    $existingClass
);

$toAdd = [];
foreach ($classificacaoDefault as $entry) {
    if (!in_array(strtolower($entry['tecnologia']), $existingTecnologias, true)) {
        $toAdd[] = $entry;
    }
}

if (!empty($toAdd) || !file_exists($classPath)) {
    $merged = array_merge($existingClass, $toAdd);
    $written = file_put_contents(
        $classPath,
        json_encode($merged, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE)
    );
    $label = file_exists($classPath) && !empty($existingClass)
        ? "Merged 'classificacao.json' (+" . count($toAdd) . " entries)"
        : "Create 'classificacao.json'";
    printStatus($written !== false, $label);
} else {
    printStatus(true, "'classificacao.json' already up-to-date (" . count($existingClass) . " entries)");
}

// excecoes.json — merge: add any default exceptions not yet present (by codigo)
$excPath = "{$configDir}/excecoes.json";
$existingExc = file_exists($excPath)
    ? (json_decode(file_get_contents($excPath), true) ?: [])
    : [];

$existingCodigos = array_map(
    fn($e) => strtoupper($e['codigo']),
    $existingExc
);

$toAddExc = [];
foreach ($excecoesDefault as $entry) {
    if (!in_array(strtoupper($entry['codigo']), $existingCodigos, true)) {
        $toAddExc[] = $entry;
    }
}

if (!empty($toAddExc) || !file_exists($excPath)) {
    $mergedExc = array_merge($existingExc, $toAddExc);
    $written = file_put_contents(
        $excPath,
        json_encode($mergedExc, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE)
    );
    $label = file_exists($excPath) && !empty($existingExc)
        ? "Merged 'excecoes.json' (+" . count($toAddExc) . " entries)"
        : "Create 'excecoes.json'";
    printStatus($written !== false, $label);
} else {
    printStatus(true, "'excecoes.json' already up-to-date (" . count($existingExc) . " entries)");
}

// Step 6: Verify installation
echo "\n6. Verification\n";

// Verify ClickHouse tables
if ($clickhouseOk) {
    $result = clickhouseQuery($clickhouseHost, $clickhousePort, "SELECT name FROM system.tables WHERE database='mibel' FORMAT JSONCompact");
    if ($result['success']) {
        $data = json_decode($result['response'], true);
        $tableCount = count($data['data'] ?? []);
        printStatus($tableCount >= 5, "ClickHouse tables: {$tableCount} found");
    } else {
        printStatus(false, "Could not verify ClickHouse tables");
    }
} else {
    printStatus(false, "ClickHouse tables: ignorado (serviço indisponível)");
}

// Verify SQLite
$tableCount = $pdo->query("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")->fetchColumn();
printStatus($tableCount >= 2, "SQLite tables: {$tableCount} found");

// Verify config files
$configCount = 0;
foreach (['classificacao.json', 'excecoes.json', 'parametros.json'] as $filename) {
    if (file_exists("{$configDir}/{$filename}")) {
        $configCount++;
    }
}
printStatus($configCount === 3, "Config files: {$configCount}/3 found");

echo "\n=== Migration Complete ===\n";
