<?php
// Copied from src/clearing_fixed.php and modified to handle normal distribution for zero-price offers.
// This script calculates clearing price, allows a user-provided normal distribution (mean & stddev) for bids with zero price,
// and displays both the original and modified clearing results on a chart.

$host      = 'clickhouse';
$port      = 8123;
$user      = 'default';
$password  = '';

$dia       = isset($_GET['dia']) && preg_match('/^\\d{4}-\\d{2}-\\d{2}$/', $_GET['dia'])
            ? $_GET['dia'] : '2025-09-01';
$periodo   = isset($_GET['periodo']) && ctype_digit($_GET['periodo']) &&
             (int)$_GET['periodo'] >= 1 && (int)$_GET['periodo'] <= 24
            ? (int)$_GET['periodo'] : 12;
$normal_mean = isset($_GET['mean']) && is_numeric($_GET['mean'])
                ? floatval($_GET['mean']) : null;
$normal_std  = isset($_GET['stddev']) && is_numeric($_GET['stddev'])
                ? floatval($_GET['stddev']) : null;

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
$replacedPrices = [];
if ($normal_mean !== null && $normal_std !== null) {
    // Helper to generate normal random value
    function randNormal($mu, $sigma){
        static $use_last=false,$y1=0.0; 
        if($use_last){$use_last=false;$val=$y1*$sigma+$mu;} else {
            do{ $u1 = mt_rand() / mt_getrandmax(); $u2 = mt_rand() / mt_getrandmax(); }
            while ($u1 <= 1e-10);
            $z0 = sqrt(-2.0 * log($u1)) * cos(2*M_PI*$u2);
            $z1 = sqrt(-2.0 * log($u1)) * sin(2*M_PI*$u2);
            $val=$z0*$sigma+$mu; $y1=$z1; $use_last=true;
        }
        return $val;
    }
    foreach ($rowsOriginal as $row) {
		// aplica normal apenas às COMPRAS a preço zero
		if ($row['tipo_oferta'] === 'V' && isset($row['preco']) && (float)$row['preco'] === 0.0) {
			$row['preco'] = randNormal($normal_mean, $normal_std);
			$replacedPrices[] = $row['preco'];
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

		// Calculate clearing price (global)
		$i = 0;
		$j = 0;
		$iavancou = false; // Controla qual índice avançou por último

		$clearingPrice  = null;
		$clearingVolume = null;

		while ($i < count($compras) && $j < count($vendas)) {
			$buy  = $compras[$i];
			$sell = $vendas[$j];

			// Condição de cruzamento de preços (A compra ficou barata demais ou a venda cara demais)
			if (round($buy['preco'], 2) < round($sell['preco'], 2)) {
				
				if ($iavancou) { // O último incremento foi na COMPRA (i aumentou)
					$clearingVolume = $compras[$i]['vol_acum'];
					$clearingPrice = round($vendas[$j]['preco'], 2);
				} else { // O último incremento foi na VENDA (j aumentou)
					$clearingVolume = $vendas[$j]['vol_acum'];
					$clearingPrice = round($compras[$i]['preco'], 2);
				}
				break;
			}

			// Lógica de avanço normal enquanto há casamento de preço
			if (round($compras[$i]['vol_acum'], 2) < round($vendas[$j]['vol_acum'], 2)) {
				$i++;
				$iavancou = true;
			} else {
				$j++;
				$iavancou = false;
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

			// Calculate clearing price per country ----------------------------
			$i = 0;
			$j = 0;
			$iavancou = false; // Controla qual índice avançou por último

			$clearingPrice  = null;
			$clearingVolume = null;

			while ($i < count($compras) && $j < count($vendas)) {
				$buy  = $compras[$i];
				$sell = $vendas[$j];

				// Condição de cruzamento de preços (A compra ficou barata demais ou a venda cara demais)
				if (round($buy['preco'], 2) < round($sell['preco'], 2)) {
					
					if ($iavancou) { // O último incremento foi na COMPRA (i aumentou)
						$clearingVolume = $compras[$i]['vol_acum'];
						$clearingPrice = round($vendas[$j]['preco'], 2);
					} else { // O último incremento foi na VENDA (j aumentou)
						$clearingVolume = $vendas[$j]['vol_acum'];
						$clearingPrice = round($compras[$i]['preco'], 2);
					}
					break;
				}

				// Lógica de avanço normal enquanto há casamento de preço
				if (round($compras[$i]['vol_acum'], 2) < round($vendas[$j]['vol_acum'], 2)) {
					$i++;
					$iavancou = true;
				} else {
					$j++;
					$iavancou = false;
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

// If normal distribution supplied, run modified calculation
if ($normal_mean !== null && $normal_std !== null) {
    $modified = processClearing($rowsModified ?? []);
    $modifiedResults = $modified['results'];
    $modifiedChart   = $modified['datasets'];
    $modifiedOffersByPais = $modified['offers_by_pais'] ?? [];

// Frequency distribution of replaced bids
// -----------------  CONFIGURAÇÃO -----------------
$bucketSize = 20;   // tamanho do bucket em € (pode ser 1, 2.5, 10, etc.)
$minPrice   = floor(min($replacedPrices));   // preço mínimo real na lista
$maxPrice   = ceil(max($replacedPrices));    // preço máximo real

// -----------------  AGRUPAMENTO -----------------
$frequency = ['labels'=>[], 'data'=>[]];
for ($b=$minPrice; $b <= $maxPrice; $b += $bucketSize) {
    $lower = $b;
    $upper = $b + $bucketSize;

    // conta quantos preços caem nesse intervalo
    $count = 0;
    foreach ($replacedPrices as $price) {
        if ($price >= $lower && $price < $upper) {   // último bucket pode usar <=
            $count++;
        }
    }

    if ($count > 0) {
        $label = sprintf('%.2f–%.2f €', $lower, $upper);
        $frequency['labels'][] = $label;
        $frequency['data'][]   = $count;
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
<h1>Resultado do Clearing (Distribuição normal)</h1>
<nav>
	<a href="index.php">Consulta Geral</a> |
	<a href="clearing.php">Curva de Clearing</a> |
	<a href="clearing_fixed.php">Curva de Clearing com PRE fixado</a> |
	<a href="clearing_uniform.php">Curva de Clearing com Distribuição Uniforme</a> |
	<a href="clearing_lognormal.php">Curva de Clearing com Distribuição LogNormal</a> |
	<a href="clearing_normal.php">Curva de Clearing com Distribuição Normal</a> |
	<a href="frequency_distribution.php">Distribuição Bid</a> |
	<a href="daily_clearing.php">Clearing price diário</a> |
	<a href="frequency_distribution.php">Distribuição de frequência</a>
</nav>
<div class="selection-row"><form method="get" >
    <label for="dia">Data:</label>
    <input type="date" id="dia" name="dia" value="<?= htmlspecialchars($dia) ?>" required>
    <label for="periodo">Período (1-24):</label>
    <input type="number" id="periodo" name="periodo" min="1" max="24" value="<?= $periodo ?>" required>
    <label for="mean">Média (µ):</label>
    <input type="number" step="0.01" id="mean" name="mean" value="<?= htmlspecialchars($normal_mean ?? '') ?>">
    <label for="stddev">Desvio Padrão (σ):</label>
    <input type="number" step="0.01" id="stddev" name="stddev" value="<?= htmlspecialchars($normal_std ?? '') ?>">
    <button type="submit">Consultar</button>
</form></div>
<div class="clearing-info">
<?php foreach ($originalResults as $pais => $res): ?>
    <p><strong>Clearing Price (<?= htmlspecialchars($pais) ?>):</strong> <?= $res['price'] !== null ? htmlspecialchars(number_format($res['price'], 2)) : 'Não encontrado' ?> €</p>
    <p><strong>Volume Comercializado (<?= htmlspecialchars($pais) ?>):</strong> <?= $res['volume'] !== null ? htmlspecialchars(number_format($res['volume'], 2)) : '0' ?> MWh</p>
<?php endforeach; ?>
</div>
<canvas id="clearingChart" width="800" height="400"></canvas>
<!-- Distribuição de Bids Substituídas -->
<h2>Distribuição de Bids Substituídas</h2>
<canvas id="frequencyChart" width="800" height="300"></canvas>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const chartDatasets = <?= json_encode($originalChart) ?>;
<?php if ($normal_mean !== null && $normal_std !== null && !empty($modifiedChart)): ?>
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

// Frequency distribution of replaced bids
const freqLabels = <?= json_encode($frequency['labels'] ?? []) ?>;
const freqData = <?= json_encode($frequency['data'] ?? []) ?>;
if(freqLabels.length>0){
  const freqCtx=document.getElementById('frequencyChart').getContext('2d');
  new Chart(freqCtx,{type:'bar', data:{labels:freqLabels, datasets:[{label:'Bids Replaced',data:freqData,borderColor:'rgba(75,192,192,1)',backgroundColor:'rgba(75,192,192,0.4)'}]}, options:{scales:{x:{title:{display:true,text:'Price (€)'}},y:{title:{display:true,text:'Count'}}}}});
}
</script>
<?php if ($normal_mean !== null && $normal_std !== null): ?>
<h2>Comparação de Clearing (Distribuição Normal = µ <?= htmlspecialchars(number_format($normal_mean, 2)) ?>, σ <?= htmlspecialchars(number_format($normal_std, 2)) ?>)</h2>
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
<button class="btn-download" onclick="downloadTable('offersTable')">Download</button>
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
<?php if ($normal_mean !== null && $normal_std !== null): ?>
<h2>Detalhes das Ofertas (Modificado)</h2>
<button id="toggleBtnMod" class="btn-toggle">Mostrar Tabela</button>
<button class="btn-download" onclick="downloadTable('offersTableMod')">Download Tabela</button>
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
<script>
function downloadTable(tableId){
  const table = document.getElementById(tableId);
  if (!table) return;
  let csv = [];
  const rows = table.querySelectorAll('tr');
  rows.forEach(row => {
    const cols = row.querySelectorAll('th,td');
    const vals = Array.from(cols).map(c=>c.textContent.replace(/"/g,'""'));
    csv.push('"' + vals.join('","') + '"');
  });
  const blob = new Blob([csv.join('\n')], {type: 'text/csv;charset=utf-8;'});
  const link=document.createElement('a');
  link.href=URL.createObjectURL(blob);
  link.download=`${tableId}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}
</script>
</body>
</html>