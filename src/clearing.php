<?php
// Clearing price calculation based on offers data from ClickHouse
$host      = 'clickhouse'; // ClickHouse running locally on port 8123
$port      = 8123;
$user      = 'default';
$password  = '';

$dia       = isset($_GET['dia']) && preg_match('/^\d{4}-\d{2}-\d{2}$/', $_GET['dia'])
            ? $_GET['dia'] : '2025-09-01';
$periodo   = isset($_GET['periodo']) && ctype_digit($_GET['periodo']) &&
             (int)$_GET['periodo'] >= 1 && (int)$_GET['periodo'] <= 24
            ? (int)$_GET['periodo'] : 12;

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

$rows = $result['data'];

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


// Group offers by country ----------------------------------------------
$hasMI = false;
$comprasAll = [];
$vendasAll   = [];
$offersByPais = [];
foreach ($rows as $row) {
    $pais = $row['pais'];
    if ($pais === 'MI') {
        $hasMI = true;
    }
    $volume = isset($row['volume']) ? (float)$row['volume'] : 0.0;
    $preco  = isset($row['preco'])  ? (float)$row['preco']  : 0.0;

    if ($row['tipo_oferta'] === 'C') {
        // Aggregate for MI case
        $comprasAll[] = ['volume'=>$volume, 'preco'=>$preco];
        // Also group per country for non-MI logic
        if (!isset($offersByPais[$pais])) {
            $offersByPais[$pais] = ['compras'=>[], 'vendas'=>[]];
        }
        $offersByPais[$pais]['compras'][] = ['volume'=>$volume, 'preco'=>$preco];
    } elseif ($row['tipo_oferta'] === 'V') {
        $vendasAll[]   = ['volume'=>$volume, 'preco'=>$preco];
        if (!isset($offersByPais[$pais])) {
            $offersByPais[$pais] = ['compras'=>[], 'vendas'=>[]];
        }
        $offersByPais[$pais]['vendas'][]  = ['volume'=>$volume, 'preco'=>$preco];
    }
}

// Function to sort offers and compute cumulative volume -----------------
$processOffers = function (&$offers, bool $ascending) use (&$processOffers) {
    usort($offers, function ($a, $b) use ($ascending) {
        if ($a['preco'] == $b['preco']) return 0;
        return ($ascending ? ($a['preco'] < $b['preco'] ? -1 : 1)
                           : ($a['preco'] > $b['preco'] ? -1 : 1));
    });
    $cum = 0.0;
    foreach ($offers as &$o) {
        $cum += $o['volume'];
        $o['vol_acum'] = $cum;
    }
};

$clearingResults = [];
$chartDatasets   = [];
if ($hasMI) {
    // Use aggregated offers for MI case
    $compras = $comprasAll;
    $vendas  = $vendasAll;
    // Process sorting and cumulative volumes
    $processOffers($compras, false);
    $processOffers($vendas, true);

    // Calculate clearing price (global)
    $clearingPrice  = null;
    $clearingVolume = null;
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

    // Chart datasets for aggregated market
    $comprasColor = '#1f77b4';
    $vendasColor  = '#ff7f0e';
    $bgComprasColor = hexToRgba($comprasColor, 0.1);
    $bgVendasColor  = hexToRgba($vendasColor,  0.1);

    $chartDatasets[] = [
        'label' => "Compras MI",
        'data'  => array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $compras),
        'borderColor' => $comprasColor,
        'backgroundColor'=> $bgComprasColor,
        'pointRadius'=>0
    ];
    $chartDatasets[] = [
        'label' => "Vendas MI",
        'data'  => array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $vendas),
        'borderColor' => $vendasColor,
        'backgroundColor'=> $bgVendasColor,
        'pointRadius'=>0
    ];
    if ($clearingPrice !== null) {
        $chartDatasets[] = [
            'label' => "Clearing MI",
            'data'  => [['x'=>$clearingVolume, 'y'=>$clearingPrice]],
            'borderColor' => 'green',
            'backgroundColor'=> hexToRgba('#00FF00',0.1),
            'showLine'=>false,
            'pointRadius'=>10
        ];
    }
} else {
    foreach ($offersByPais as $pais => $group) {
        $compras = $group['compras'];
        $vendas  = $group['vendas'];
        // Process sorting and cumulative volumes
        $processOffers($compras, false); // descending for compras (higher price first)
        $processOffers($vendas, true);   // ascending for vendas (lower price first)
        // Calculate clearing price per country ----------------------------
        $clearingPrice  = null;
        $clearingVolume = null;
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

        // Prepare chart datasets -------------------------------------------
        // Color mapping per country
        $colorMap = [
            'MI' => ['compras'=>'#1f77b4', 'vendas'=>'#ff7f0e'],
            'PT' => ['compras'=> '#2ca02c', 'vendas'=> '#d62728'],
            'ES' => ['compras'=> '#9467bd', 'vendas'=> '#8c564b']
        ];
        $comprasColor = $colorMap[$pais]['compras'] ?? '#1f77b4';
        $vendasColor  = $colorMap[$pais]['vendas'] ?? '#ff7f0e';
        $bgComprasColor = hexToRgba($comprasColor, 0.1);
        $bgVendasColor  = hexToRgba($vendasColor,  0.1);

        // Compras dataset
        $chartDatasets[] = [
            'label' => "Compras ($pais)",
            'data'  => array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $compras),
            'borderColor' => $comprasColor,
            'backgroundColor'=> $bgComprasColor,
            'pointRadius'=>0
        ];

        // Vendas dataset
        $chartDatasets[] = [
            'label' => "Vendas ($pais)",
            'data'  => array_map(fn($o)=>['x'=>$o['vol_acum'], 'y'=>$o['preco']], $vendas),
            'borderColor' => $vendasColor,
            'backgroundColor'=> $bgVendasColor,
            'pointRadius'=>0
        ];

        // Clearing point dataset if exists
        if ($clearingPrice !== null) {
            $chartDatasets[] = [
                'label' => "Clearing ($pais)",
                'data'  => [['x'=>$clearingVolume, 'y'=>$clearingPrice]],
                'borderColor' => 'green',
                'backgroundColor'=> hexToRgba('#00FF00',0.1),
                'showLine'=>false,
                'pointRadius'=>10
            ];
        }
    }
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
<h1>Resultado do Clearing</h1>
<nav>
	<a href="index.php">Consulta Geral</a> |
	<a href="clearing.php">Curva de Clearing</a> |
	<a href="clearing_fixed.php">Curva de Clearing com PRE fixado</a> |
	<a href="frequency_distribution.php">Distribuição Bid</a> |
	<a href="daily_clearing.php">Clearing price diário</a> |
</nav>
<div class="selection-row"><form method="get" style="margin-bottom:1rem;">
    <label for="dia">Data:</label>
    <input type="date" id="dia" name="dia" value="<?= htmlspecialchars($dia) ?>" required>
    <label for="periodo">Período (1-24):</label>
    <input type="number" id="periodo" name="periodo" min="1" max="24" value="<?= $periodo ?>" required>
    <button type="submit">Consultar</button>
</form>
<div class="clearing-info">
    <?php foreach ($clearingResults as $pais => $res): ?>
        <p><strong>Clearing Price (<?= htmlspecialchars($pais) ?>):</strong> <?= $res['price'] !== null ? htmlspecialchars(number_format($res['price'], 2)) : 'Não encontrado' ?> €</p>
        <p><strong>Volume Comercializado (<?= htmlspecialchars($pais) ?>):</strong> <?= $res['volume'] !== null ? htmlspecialchars(number_format($res['volume'], 2)) : '0' ?> MWh</p>
    <?php endforeach; ?>
</div>
</div>

<canvas id="clearingChart" width="800" height="400"></canvas>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const chartDatasets = <?= json_encode($chartDatasets) ?>;
const ctx = document.getElementById('clearingChart').getContext('2d');
new Chart(ctx, {
    type: 'line',
    data: { datasets: chartDatasets },
    options: {
        elements: { line: { tension: 0.4 } },
        scales: {
            x: {
                type: 'linear',
                title: { display: true, text: 'Volume Acumulado (MWh)' }
            },
            y: {
                type: 'linear',
                title: { display: true, text: 'Preço (€)' }
            }
        }
    }
});
</script>

<h2>Detalhes das Ofertas</h2>
<button id="toggleBtn" class="btn-toggle">Mostrar Tabela</button>
<table id="offersTable" border="1" cellpadding="5">
    <thead>
        <tr>
            <th colspan="3">Compras (Demanda)</th>
            <th colspan="3">Vendas (Oferta)</th>
        </tr>
        <tr>
            <th>Preço (€)</th><th>Volume</th><th>Cum. Volume</th>
            <th>Preço (€)</th><th>Volume</th><th>Cum. Volume</th>
        </tr>
    </thead>
    <tbody>
        <?php
        // For simplicity, display only the first country group (if multiple countries present)
        $firstPais = array_key_first($offersByPais);
        $compras   = $offersByPais[$firstPais]['compras'];
        $vendas    = $offersByPais[$firstPais]['vendas'];
        $maxRows   = max(count($compras), count($vendas));
        for ($i=0;$i<$maxRows;$i++):
            $c = $compras[$i] ?? null;
            $v = $vendas[$i] ?? null;
        ?>
        <tr>
            <?php if ($c): ?>
                <td><?= htmlspecialchars(number_format($c['preco'], 2)) ?></td>
                <td><?= htmlspecialchars(number_format($c['volume'], 2)) ?></td>
                <td><?= htmlspecialchars(number_format($c['vol_acum'] ?? $c['volume'], 2)) ?></td>
            <?php else: ?>
                <td colspan="3">&nbsp;</td>
            <?php endif; ?>

            <?php if ($v): ?>
                <td><?= htmlspecialchars(number_format($v['preco'], 2)) ?></td>
                <td><?= htmlspecialchars(number_format($v['volume'], 2)) ?></td>
                <td><?= htmlspecialchars(number_format($v['vol_acum'] ?? $v['volume'], 2)) ?></td>
            <?php else: ?>
                <td colspan="3">&nbsp;</td>
            <?php endif; ?>
        </tr>
        <?php endfor; ?>
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

</div>
</body>
</html>