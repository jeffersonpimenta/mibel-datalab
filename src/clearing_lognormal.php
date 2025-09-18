<?php
// Copied from src/clearing_fixed.php and modified to handle log-normal distribution for zero-price offers.
// Allows user-provided parameters mu (mean of underlying normal) and sigma (standard deviation).

$host      = 'clickhouse';
$port      = 8123;
$user      = 'default';
$password  = '';

$dia       = isset($_GET['dia']) && preg_match('/^\\d{4}-\\d{2}-\\d{2}$/', $_GET['dia'])
            ? $_GET['dia'] : '2025-09-01';
$periodo   = isset($_GET['periodo']) && ctype_digit($_GET['periodo']) &&
             (int)$_GET['periodo'] >= 1 && (int)$_GET['periodo'] <= 24
            ? (int)$_GET['periodo'] : 12;
$lognormal_mu   = isset($_GET['mu']) && is_numeric($_GET['mu'])
                ? floatval($_GET['mu']) : null;
$lognormal_sigma= isset($_GET['sigma']) && is_numeric($_GET['sigma'])
                ? floatval($_GET['sigma']) : null;

// Query to fetch country, offer type, volume and price
$query = "SELECT pais, tipo_oferta, volume, preco\n          FROM ofertas\n          WHERE data = '$dia' AND periodo = $periodo AND status IN ('C', 'O')";
$url = "http://$host:$port/?user=$user&password=$password&default_format=JSON&query=" . urlencode($query);
$response = @file_get_contents($url);

if ($response === false) {
    $error = error_get_last();
    http_response_code(500);
    echo '<h1>Erro ao conectar no ClickHouse</h1>' .
         (isset($error['message']) ? '<pre>' . htmlspecialchars($error['message']) . '</pre>' : '');
    exit;
}

$result = json_decode($response, true);
if (!isset($result['data'])) {
    http_response_code(500);
    echo '<h1>Formato inesperado da resposta do ClickHouse</h1>';
    exit;
}

$rowsOriginal = $result['data'];
// For modified data we will clone and adjust prices.
$rowsModified = [];
if ($lognormal_mu !== null && $lognormal_sigma !== null) {
    // Helper to sample from normal distribution using Box-Muller transform
    function rand_normal(float $mean, float $stddev): float {
        static $use_last = false; static $y1 = 0.0;
        if ($use_last) { $use_last = false; return $y1 * $stddev + $mean; }
        $x1 = mt_rand() / mt_getrandmax();
        $x2 = mt_rand() / mt_getrandmax();
        $w  = -2.0 * log($x1);
        $y1 = sqrt($w) * cos(2*M_PI*$x2);
        $y2 = sqrt($w) * sin(2*M_PI*$x2);
        $use_last = true;
        return ($y1 * $stddev + $mean); // we only need one sample
    }
    foreach ($rowsOriginal as $row) {
        if (isset($row['preco']) && ((float)$row['preco'] === 0.0)) {
            $normalSample = rand_normal($lognormal_mu, $lognormal_sigma);
            // lognormal value
            $row['preco'] = exp($normalSample);
        }
        $rowsModified[] = $row;
    }
}

// Helper functions -----------------------------------------------------
function hexToRgba(string $hex, float $alpha): string {
    $hex = ltrim($hex, '#');
    if (strlen($hex) === 3) {
        $r = hexdec(str_repeat(substr($hex,0,1),2));
        $g = hexdec(str_repeat(substr($hex,1,1),2));
        $b = hexdec(str_repeat(substr($hex,2,1),2));
    } else {
        $r = hexdec(substr($hex,0,2));
        $g = hexdec(substr($hex,2,2));
        $b = hexdec(substr($hex,4,2));
    }
    return "rgba({$r},{$g},{$b},{$alpha})";
}

// Process clearing logic -------------------------------------------------
function processClearing(array $rows): array {
    // Group offers by country and detect MI case.
    $hasMI = false;
    $comprasAll = [];
    $vendasAll   = [];
    $offersByPais = [];

    foreach ($rows as $row) {
        $pais = $row['pais'];
        if ($pais === 'MI') { $hasMI = true; }
        $volume = isset($row['volume']) ? (float)$row['volume'] : 0.0;
        $preco  = isset($row['preco'])  ? (float)$row['preco']  : 0.0;

        if ($row['tipo_oferta'] === 'C') {
            $comprasAll[] = ['volume'=>$volume, 'preco'=>$preco];
            if (!isset($offersByPais[$pais])) { $offersByPais[$pais] = ['compras'=>[], 'vendas'=>[]]; }
            $offersByPais[$pais]['compras'][] = ['volume'=>$volume, 'preco'=>$preco];
        } elseif ($row['tipo_oferta'] === 'V') {
            $vendasAll[]   = ['volume'=>$volume, 'preco'=>$preco];
            if (!isset($offersByPais[$pais])) { $offersByPais[$pais] = ['compras'=>[], 'vendas'=>[]]; }
            $offersByPais[$pais]['vendas'][]  = ['volume'=>$volume, 'preco'=>$preco];
        }
    }

    // Sorting and cumulative volume function.
    $processOffers = function (&$offers, bool $ascending) use (&$processOffers) {
        usort($offers, function ($a, $b) use ($ascending) {
            if ($a['preco'] == $b['preco']) return 0;
            return ($ascending ? ($a['preco'] < $b['preco'] ? -1 : 1)
                               : ($a['preco'] > $b['preco'] ? -1 : 1));
        });
        $cum = 0.0;
        foreach ($offers as &$o) { $cum += $o['volume']; $o['vol_acum'] = $cum; }
    };

    $clearingResults = [];
    $chartDatasets   = [];
    if ($hasMI) {
        $compras = $comprasAll;
        $vendas  = $vendasAll;
        $processOffers($compras, false);
        $processOffers($vendas, true);

        $clearingPrice  = null; $clearingVolume = null;
        foreach ($vendas as $sell) {
            $demandaMax = array_sum(
                array_column(
                    array_filter($compras, function ($c) use ($sell) { return $c['preco'] >= $sell['preco']; }),
                    'volume'
                )
            );
            if ($sell['vol_acum'] >= $demandaMax && $demandaMax > 0) {
                $clearingPrice  = round($sell['preco'], 2);
                $clearingVolume = $demandaMax;
                break;
            }
        }
        $clearingResults['MI'] = ['price'=>$clearingPrice, 'volume'=>$clearingVolume];

        $comprasColor = '#1f77b4'; $vendasColor  = '#ff7f0e';
        $bgComprasColor = hexToRgba($comprasColor, 0.1);
        $bgVendasColor  = hexToRgba($vendasColor,  0.1);

        $chartDatasets[] = ['label'=>'Compras MI','data'=>array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $compras),
                            'borderColor'=>$comprasColor,'backgroundColor'=>$bgComprasColor,'pointRadius'=>0];
        $chartDatasets[] = ['label'=>'Vendas MI','data'=>array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $vendas),
                            'borderColor'=>$vendasColor,'backgroundColor'=>$bgVendasColor,'pointRadius'=>0];
        if ($clearingPrice !== null) {
            $chartDatasets[] = ['label'=>'Clearing MI','data'=>[['x'=>$clearingVolume, 'y'=>$clearingPrice]],
                                'borderColor'=>'green','backgroundColor'=>hexToRgba('#00FF00',0.1),
                                'showLine'=>false,'pointRadius'=>10];
        }
    } else {
        foreach ($offersByPais as $pais => $group) {
            $compras = $group['compras'];
            $vendas  = $group['vendas'];
            $processOffers($compras, false);
            $processOffers($vendas, true);

            $clearingPrice  = null; $clearingVolume = null;
            foreach ($vendas as $sell) {
                $demandaMax = array_sum(
                    array_column(
                        array_filter($compras, function ($c) use ($sell) { return $c['preco'] >= $sell['preco']; }),
                        'volume'
                    )
                );
                if ($sell['vol_acum'] >= $demandaMax && $demandaMax > 0) {
                    $clearingPrice  = round($sell['preco'], 2);
                    $clearingVolume = $demandaMax;
                    break;
                }
            }
            $clearingResults[$pais] = ['price'=>$clearingPrice, 'volume'=>$clearingVolume];

            $colorMap = [
                'MI'=>['compras'=>'#1f77b4','vendas'=>'#ff7f0e'],
                'PT'=>['compras'=> '#2ca02c', 'vendas'=> '#d62728'],
                'ES'=>['compras'=> '#9467bd', 'vendas'=> '#8c564b']
            ];
            $comprasColor = $colorMap[$pais]['compras'] ?? '#1f77b4';
            $vendasColor  = $colorMap[$pais]['vendas'] ?? '#ff7f0e';
            $bgComprasColor = hexToRgba($comprasColor, 0.1);
            $bgVendasColor  = hexToRgba($vendasColor,  0.1);

            $chartDatasets[] = ['label'=>'Compras ('.$pais.')','data'=>array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $compras),
                                'borderColor'=>$comprasColor,'backgroundColor'=>$bgComprasColor,'pointRadius'=>0];
            $chartDatasets[] = ['label'=>'Vendas ('.$pais.')','data'=>array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $vendas),
                                'borderColor'=>$vendasColor,'backgroundColor'=>$bgVendasColor,'pointRadius'=>0];
            if ($clearingPrice !== null) {
                $chartDatasets[] = ['label'=>'Clearing ('.$pais.')','data'=>[['x'=>$clearingVolume, 'y'=>$clearingPrice]],
                                    'borderColor'=>'green','backgroundColor'=>hexToRgba('#00FF00',0.1),
                                    'showLine'=>false,'pointRadius'=>10];
            }
        }
    }

    return ['results'=>$clearingResults, 'datasets'=>$chartDatasets, 'offers_by_pais'=>$offersByPais];
}

// Run for original data
$original = processClearing($rowsOriginal);
$originalResults = $original['results'];
$originalChart   = $original['datasets'];

// If lognormal parameters supplied, run modified calculation
if ($lognormal_mu !== null && $lognormal_sigma !== null) {
    $modified = processClearing($rowsModified ?? []);
    $modifiedResults = $modified['results'];
    $modifiedChart   = $modified['datasets'];
    $modifiedOffersByPais = $modified['offers_by_pais'] ?? [];
}
?>
<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <title>Clearing Price – Dia <?= htmlspecialchars($dia) ?>, Período <?= htmlspecialchars((string)$periodo) ?></title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
<div class="container">
<h1>Resultado do Clearing (Original)</h1>
<nav>
	<a href="index.php">Consulta Geral</a> |
	<a href="clearing.php">Curva de Clearing</a> |
	<a href="clearing_fixed.php">Curva de Clearing com PRE fixado</a> |
	<a href="clearing_uniform.php">Curva de Clearing com Distribuição Uniforme</a> |
	<a href="clearing_lognormal.php">Curva de Clearing com Distribuição LogNormal</a> |
	<a href="frequency_distribution.php">Distribuição Bid</a> |
	<a href="daily_clearing.php">Clearing price diário</a> |
</nav>
<div class="selection-row"><form method="get" style="margin-bottom:1rem;">
    <label for="dia">Data:</label>
    <input type="date" id="dia" name="dia" value="<?= htmlspecialchars($dia) ?>" required>
    <label for="periodo">Período (1-24):</label>
    <input type="number" id="periodo" name="periodo" min="1" max="24" value="<?= $periodo ?>" required>
    <label for="mu">Mu:</label>
    <input type="number" step="0.01" id="mu" name="mu" value="<?= htmlspecialchars($lognormal_mu ?? '') ?>">
    <label for="sigma">Sigma:</label>
    <input type="number" step="0.01" id="sigma" name="sigma" value="<?= htmlspecialchars($lognormal_sigma ?? '') ?>">
    <button type="submit">Consultar</button>
</form></div>
<div class="clearing-info">
<?php foreach ($originalResults as $pais => $res): ?>
    <p><strong>Clearing Price (<?= htmlspecialchars($pais) ?>):</strong> <?= $res['price'] !== null ? htmlspecialchars(number_format($res['price'], 2)) : 'Não encontrado' ?> €</p>
    <p><strong>Volume Comercializado (<?= htmlspecialchars($pais) ?>):</strong> <?= $res['volume'] !== null ? htmlspecialchars(number_format($res['volume'], 2)) : '0' ?> MWh</p>
<?php endforeach; ?>
</div>
<canvas id="clearingChart" width="800" height="400"></canvas>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const chartDatasets = <?= json_encode($originalChart) ?>;
<?php if ($lognormal_mu !== null && $lognormal_sigma !== null && !empty($modifiedChart)): ?>
    // Add modified series with dashed lines
    <?php foreach ($modifiedChart as &$ds): $ds['borderDash']=[5,5]; $ds['label'].=' (Modificado)'; endforeach; ?>
    <?= 'const modifiedDatasets = '.json_encode($modifiedChart).';' ?>
    chartDatasets.push(...modifiedDatasets);
<?php endif; ?>
const ctx = document.getElementById('clearingChart').getContext('2d');
new Chart(ctx, {
    type: 'line',
    data: { datasets: chartDatasets },
    options: {
        elements: { line: { tension: 0.4 } },
        scales: {
            x: { type: 'linear', title: { display: true, text: 'Volume Acumulado (MWh)' } },
            y: { type: 'linear', title: { display: true, text: 'Preço (€)' } }
        }
    }
});
</script>
<?php if ($lognormal_mu !== null && $lognormal_sigma !== null): ?>
<h2>Comparação de Clearing (Distribuição LogNormal = [Mu=<?= htmlspecialchars(number_format($lognormal_mu, 2)) ?>; Sigma=<?= htmlspecialchars(number_format($lognormal_sigma, 2)) ?>])</h2>
<table border="1" cellpadding="5">
<tr><th>País</th><th>Clearing Original (€)</th><th>Clearing Modificado (€)</th><th>Volume Original (MWh)</th><th>Volume Modificado (MWh)</th></tr>
<?php
$allCountries = array_unique(array_merge(array_keys($originalResults), array_keys($modifiedResults)));
foreach ($allCountries as $pais) {
    $orig = $originalResults[$pais] ?? ['price'=>null,'volume'=>0];
    $mod  = $modifiedResults[$pais] ?? ['price'=>null,'volume'=>0];
    echo '<tr>';
    echo '<td>'.htmlspecialchars($pais).'</td>';
    echo '<td>'.($orig['price'] !== null ? number_format($orig['price'],2) : 'Não').'</td>';
    echo '<td>'.($mod['price'] !== null ? number_format($mod['price'],2) : 'Não').'</td>';
    echo '<td>'.number_format($orig['volume'],2).'</td>';
    echo '<td>'.number_format($mod['volume'],2).'</td>';
    echo '</tr>';
}
?>
</table>
<?php endif; ?>
<h2>Detalhes das Ofertas (Original)</h2>
<button id="toggleBtn" class="btn-toggle">Mostrar Tabela</button>
<table id="offersTable" border="1" cellpadding="5">
    <thead>
        <tr><th>País</th><th colspan="3">Compras (Demanda)</th><th colspan="3">Vendas (Oferta)</th></tr>
        <tr><th></th><th>Preço (€)</th><th>Volume</th><th>Cum. Volume</th><th>Preço (€)</th><th>Volume</th><th>Cum. Volume</th></tr>
    </thead>
    <tbody>
<?php
foreach ($original['offers_by_pais'] as $pais => $group) {
    $compras = $group['compras'] ?? [];
    $vendas  = $group['vendas'] ?? [];
    $maxRows = max(count($compras), count($vendas));
    for ($i=0;$i<$maxRows;$i++) {
        $c = $compras[$i] ?? null;
        $v = $vendas[$i] ?? null;
        echo '<tr>';
        echo '<td>'.htmlspecialchars($pais).'</td>';
        if ($c) {
            echo '<td>'.number_format($c['preco'],2).'</td><td>'.number_format($c['volume'],2).'</td><td>'.(isset($c['vol_acum'])?number_format($c['vol_acum'],2):number_format($c['volume'],2)).'</td>';
        } else {
            echo '<td colspan=\"3\">&nbsp;</td>';
        }
        if ($v) {
            echo '<td>'.number_format($v['preco'],2).'</td><td>'.number_format($v['volume'],2).'</td><td>'.(isset($v['vol_acum'])?number_format($v['vol_acum'],2):number_format($v['volume'],2)).'</td>';
        } else {
            echo '<td colspan=\"3\">&nbsp;</td>';
        }
        echo '</tr>';
    }
}?>
    </tbody>
</table>
<script>
document.getElementById('toggleBtn').addEventListener('click', function(){
    var tbl = document.getElementById('offersTable');
    var computedDisplay = window.getComputedStyle(tbl).display;
    if (computedDisplay === 'none' || tbl.style.display === 'none') {
        tbl.style.display = '';
        this.textContent = 'Esconder Tabela';
    } else {
        tbl.style.display = 'none';
        this.textContent = 'Mostrar Tabela';
    }
});
document.getElementById('offersTable').style.display = 'none';
</script>
<!-- Tabela das ofertas modificadas -->
<?php if ($lognormal_mu !== null && $lognormal_sigma !== null): ?>
<h2>Detalhes das Ofertas (Modificado)</h2>
<button id="toggleBtnMod" class="btn-toggle">Mostrar Tabela</button>
<table id="offersTableMod" border="1" cellpadding="5">
    <thead>
        <tr><th>País</th><th colspan="3">Compras (Demanda)</th><th colspan="3">Vendas (Oferta)</th></tr>
        <tr><th></th><th>Preço (€)</th><th>Volume</th><th>Cum. Volume</th><th>Preço (€)</th><th>Volume</th><th>Cum. Volume</th></tr>
    </thead>
    <tbody>
<?php
foreach ($modifiedOffersByPais as $pais => $group) {
    $compras = $group['compras'] ?? [];
    $vendas  = $group['vendas'] ?? [];
    $maxRows = max(count($compras), count($vendas));
    for ($i=0;$i<$maxRows;$i++) {
        $c = $compras[$i] ?? null;
        $v = $vendas[$i] ?? null;
        echo '<tr>';
        echo '<td>'.htmlspecialchars($pais).'</td>';
        if ($c) {
            echo '<td>'.number_format($c['preco'],2).'</td><td>'.number_format($c['volume'],2).'</td><td>'.(isset($c['vol_acum'])?number_format($c['vol_acum'],2):number_format($c['volume'],2)).'</td>';
        } else {
            echo '<td colspan=\"3\">&nbsp;</td>';
        }
        if ($v) {
            echo '<td>'.number_format($v['preco'],2).'</td><td>'.number_format($v['volume'],2).'</td><td>'.(isset($v['vol_acum'])?number_format($v['vol_acum'],2):number_format($v['volume'],2)).'</td>';
        } else {
            echo '<td colspan=\"3\">&nbsp;</td>';
        }
        echo '</tr>';
    }
}?>
    </tbody>
</table>
<script>
document.getElementById('toggleBtnMod').addEventListener('click', function(){
    var tbl = document.getElementById('offersTableMod');
    var computedDisplay = window.getComputedStyle(tbl).display;
    if (computedDisplay === 'none' || tbl.style.display === 'none') {
        tbl.style.display = '';
        this.textContent = 'Esconder Tabela';
    } else {
        tbl.style.display = 'none';
        this.textContent = 'Mostrar Tabela';
    }
});
document.getElementById('offersTableMod').style.display = 'none';
</script>
<?php endif; ?>
</div>
</body>
</html>