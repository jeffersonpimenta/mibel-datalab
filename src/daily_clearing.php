<?php
// daily_clearing.php - Plots clearing price per period for a selected day
$host      = 'clickhouse'; // ClickHouse running locally on port 8123
$port      = 8123;
$user      = 'default';
$password  = '';

// Validate and set default date (today)
if (isset($_GET['dia']) && preg_match('/^\d{4}-\d{2}-\d{2}$/', $_GET['dia'])) {
    $dia = $_GET['dia'];
} else {
    $dia = date('Y-m-d');
}

// Query to fetch offers for the selected day
$query = "SELECT pais, periodo, tipo_oferta, volume, preco FROM ofertas WHERE data = '$dia' AND status IN ('C','O')";
$url   = "http://$host:$port/?user=$user&password=$password&default_format=JSON&query=" . urlencode($query);
$response = @file_get_contents($url);

if ($response === false) {
    http_response_code(500);
    echo '<h1>Erro ao conectar no ClickHouse</h1>';
    exit;
}
$result = json_decode($response, true);
$rows   = $result['data'] ?? [];

// Helper: hex to rgba
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

// Group offers by period and country
$offersByPeriodPais = [];
foreach ($rows as $row) {
    $periodo = (int)$row['periodo'];
    $pais    = $row['pais'];
    if (!isset($offersByPeriodPais[$periodo])) {
        $offersByPeriodPais[$periodo] = [];
    }
    if (!isset($offersByPeriodPais[$periodo][$pais])) {
        $offersByPeriodPais[$periodo][$pais] = ['compras'=>[], 'vendas'=>[]];
    }
    $volume = isset($row['volume']) ? (float)$row['volume'] : 0.0;
    $preco  = isset($row['preco'])  ? (float)$row['preco']  : 0.0;

    if ($row['tipo_oferta'] === 'C') {
        $offersByPeriodPais[$periodo][$pais]['compras'][] = ['volume'=>$volume, 'preco'=>$preco];
    } elseif ($row['tipo_oferta'] === 'V') {
        $offersByPeriodPais[$periodo][$pais]['vendas'][]  = ['volume'=>$volume, 'preco'=>$preco];
    }
}

// Function to sort offers and compute cumulative volume
$processOffers = function (&$offers, bool $ascending) {
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

// Compute clearing price per period and country
$clearingResults = [];

// Calculate clearing price via ClickHouse query
$query = "
WITH total_buy AS (
    SELECT data, periodo, pais, SUM(volume) AS vol
    FROM default.ofertas
    WHERE tipo_oferta = 'C'
      AND data = '$dia'
    GROUP BY data, periodo, pais
),
 sells AS (
    SELECT data, periodo, pais, preco, SUM(volume) AS vol
    FROM default.ofertas
    WHERE tipo_oferta = 'V'
      AND data = '$dia'
    GROUP BY data, periodo, pais, preco
    ORDER BY preco ASC
),
 sell_cum AS (
    SELECT data, periodo, pais, preco,
           SUM(vol) OVER (
               PARTITION BY data, periodo, pais
               ORDER BY preco ASC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS cum_vol
    FROM sells
)
SELECT s.data, s.periodo, s.pais, s.preco AS clearing_price, s.cum_vol
FROM sell_cum s
JOIN total_buy t ON s.data = t.data AND s.periodo = t.periodo AND s.pais = t.pais
WHERE s.cum_vol >= t.vol
QUALIFY row_number() OVER (PARTITION BY s.data,s.periodo,s.pais ORDER BY s.preco ASC) = 1;
";

$url   = "http://$host:$port/?user=$user&password=$password&default_format=JSON&query=" . urlencode($query);
$response = @file_get_contents($url);

if ($response === false) {
    http_response_code(500);
    echo '<h1>Erro ao consultar ClickHouse para cálculo de clearing</h1>';
    exit;
}
$result = json_decode($response, true);
$rows   = $result['data'] ?? [];

foreach ($rows as $row) {
    $periodo = (int)$row['periodo'];
    $pais    = $row['pais'];
    $clearingResults[$periodo][$pais] = isset($row['clearing_price']) ? (float)$row['clearing_price'] : null;
}

// Prepare datasets for chart (one line per country across periods)
$colorMap = [
    'MI' => ['compras'=>'#1f77b4', 'vendas'=>'#ff7f0e'],
    'PT' => ['compras'=> '#d62728', 'vendas'=> '#ff7f0e'],
    'ES' => ['compras'=> '#8c564b', 'vendas'=> '#2ca02c']
];
$chartDatasets = [];
foreach ($colorMap as $pais => $colors) {
    if ($pais === 'MI') {
        // MI dataset with null gaps for PT/ES presence
        $points = [];
        for ($p=1; $p<=24; $p++) {
            $price = isset($clearingResults[$p][$pais]) ? round($clearingResults[$p][$pais],2) : null;
            $points[] = ['x'=>$p, 'y'=>$price];
        }
    } else {
        // Build base points where country has clearing price
        $points = [];
        for ($p=1; $p<=24; $p++) {
            if (isset($clearingResults[$p][$pais]) && $clearingResults[$p][$pais] !== null) {
                $points[] = ['x'=>$p, 'y'=>round($clearingResults[$p][$pais],2)];
            }
        }

        // Identify split periods where MI missing but this country present
        for ($s=1; $s<=24; $s++) {
            if (!isset($clearingResults[$s]['MI']) && isset($clearingResults[$s][$pais])) {
                // Duplicate previous MI point
                if ($s-1 >= 1 && isset($clearingResults[$s-1]['MI'])) {
                    $dupX = $s-1;
                    // Check if already present
                    $exists = false;
                    foreach ($points as $pt) { if ($pt['x']===$dupX){$exists=true;break;} }
                    if (!$exists) {
                        $points[] = ['x'=>$dupX, 'y'=>round($clearingResults[$s-1]['MI'],2)];
                    }
                }
                // Duplicate next MI point
                if ($s+1 <= 24 && isset($clearingResults[$s+1]['MI'])) {
                    $dupX = $s+1;
                    $exists = false;
                    foreach ($points as $pt) { if ($pt['x']===$dupX){$exists=true;break;} }
                    if (!$exists) {
                        $points[] = ['x'=>$dupX, 'y'=>round($clearingResults[$s+1]['MI'],2)];
                    }
                }
            }
        }

        // Sort points by x to maintain order
        usort($points, function ($a,$b){ return $a['x'] <=> $b['x']; });
    }

    if (!empty($points)) {
        $chartDatasets[] = [
            'label' => "Clearing ($pais)",
            'data'  => $points,
            'borderColor' => $colorMap[$pais]['compras'],
            'backgroundColor'=> hexToRgba($colorMap[$pais]['compras'], 0.1),
            'pointRadius'=>3,
            'tension'=>0.2
        ];
    }
}
?>
<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <title>Curtir Clearning Diário – Dia <?= htmlspecialchars($dia) ?></title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
<div class="container">
<h1>Clearing Price Diário – Dia <?= htmlspecialchars($dia) ?></h1>
<nav>
	<a href="index.php">Consulta Geral</a> |
	<a href="clearing.php">Curva de Clearing</a> |
	<a href="clearing_fixed.php">Curva de Clearing com PRE fixado</a> |
	<a href="clearing_uniform.php">Curva de Clearing com Distribuição Uniforme</a> |
	<a href="clearing_lognormal.php">Curva de Clearing com Distribuição LogNormal</a> 
	<a href="clearing_normal.php">Curva de Clearing com Distribuição Normal</a> |
	<a href="frequency_distribution.php">Distribuição Bid</a> |
	<a href="daily_clearing.php">Clearing price diário</a> |
</nav>

<form method="get" style="margin-bottom:1rem;">
    <label for="dia">Data:</label>
    <input type="date" id="dia" name="dia" value="<?= htmlspecialchars($dia) ?>" required>
    <button type="submit">Consultar</button>
</form>

<canvas id="clearingChart" width="800" height="400"></canvas>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const chartDatasets = <?= json_encode($chartDatasets) ?>;
const ctx = document.getElementById('clearingChart').getContext('2d');
new Chart(ctx, {
    type: 'line',
    data: { datasets: chartDatasets },
    options: {
        responsive:true,
        scales:{
            x:{
                type:'linear',
                title:{display:true,text:'Período'}
            },
            y:{
                title:{display:true,text:'Preço (€)'}
            }
        },
        plugins:{
            legend:{position:'top'},
            tooltip:{mode:'index',intersect:false}
        }
    }
});
</script>

<h2>Valores de Clearing por Período e País</h2>
<table border="1" cellpadding="5">
<thead><tr><th>Período</th><th>MI</th><th>PT</th><th>ES</th></tr></thead>
<tbody>
<?php
for ($p=1;$p<=24;$p++){
    echo '<tr>';
    echo '<td>'. $p .'</td>';
    foreach (['MI','PT','ES'] as $pais){
        $val = isset($clearingResults[$p][$pais]) ? number_format($clearingResults[$p][$pais],2) : '-';
        echo '<td>'.$val.'</td>';
    }
    echo '</tr>';
}
?>
</tbody>
</table>

</div>
</body>
</html>
