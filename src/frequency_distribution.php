<?php
// Frequency Distribution of bid values from ClickHouse
// Author: Senior Developer
// ------------------------------------------------------------
$host      = 'clickhouse'; // ClickHouse running locally on port 8123
$port      = 8123;
$user      = 'default';
$password  = '';

// ------------------------------------------------------------------
// Helper to safely escape single quotes for SQL values
function esc($v){return str_replace("'","''",$v);} 

// ------------------------------------------------------------------
// Read and validate filters from GET parameters
$filters = [];

// Country filter – default empty (no filter). Allowed: MI, PT, ES
if(isset($_GET['pais']) && in_array($_GET['pais'], ['MI','PT','ES'])){
    $filters['pais'] = $_GET['pais'];
}

// Periodo – integer 1-25
if(isset($_GET['periodo']) && ctype_digit($_GET['periodo'])){
    $p = (int)$_GET['periodo'];
    if($p>=1 && $p<=25){
        $filters['periodo'] = $p;
    }
}

// Tipo de oferta – C or V
if(isset($_GET['tipo_oferta']) && in_array($_GET['tipo_oferta'], ['C','V'])){
    $filters['tipo_oferta'] = $_GET['tipo_oferta'];
}

// Status – C or O
if(isset($_GET['status']) && in_array($_GET['status'], ['C','O'])){
    $filters['status'] = $_GET['status'];
}

// Data (optional). Validate format YYYY-MM-DD. Default to today.
$dia = '2025-07-01'; // default example date
if(isset($_GET['data']) && preg_match('/^\d{4}-\d{2}-\d{2}$/', $_GET['data'])){
    $dia = $_GET['data'];
}

// ------------------------------------------------------------------
// Build ClickHouse query
// Default bucket parameters – can be overridden via GET
$bucket_min  = isset($_GET['bucket_min']) && is_numeric($_GET['bucket_min']) ? (float)$_GET['bucket_min'] : -10000;
$bucket_max  = isset($_GET['bucket_max']) && is_numeric($_GET['bucket_max']) ? (float)$_GET['bucket_max'] : 10000;
$bucket_bins = isset($_GET['bucket_bins']) && ctype_digit($_GET['bucket_bins']) && (int)$_GET['bucket_bins']>0
                ? (int)$_GET['bucket_bins']
                : 500;
$query  = "SELECT width_bucket(preco, $bucket_min, $bucket_max, $bucket_bins) AS price_bin, COUNT(*) AS freq FROM ofertas WHERE status IN ('C','O')";

if(isset($filters['pais'])){
    $query .= " AND pais='" . esc($filters['pais']) . "'";
}
if(isset($filters['periodo'])){
    $query .= " AND periodo = {$filters['periodo']}";
}
if(isset($filters['tipo_oferta'])){
    $query .= " AND tipo_oferta='" . esc($filters['tipo_oferta']) . "'";
}
if(isset($filters['status'])){
    // override the IN clause if a single status is chosen
    $query = str_replace("status IN ('C','O')", "status='" . esc($filters['status']) . "'", $query);
}
// date filter
$query .= " AND data='" . esc($dia) . "'";

$query .= " GROUP BY price_bin ORDER BY price_bin ASC;";

// ------------------------------------------------------------------
// Execute query against ClickHouse via HTTP interface
$url = "http://$host:$port/?user=$user&password=$password&default_format=JSON&query=" . urlencode($query);
$response = @file_get_contents($url);

if ($response === false) {
    $error = error_get_last();
    http_response_code(500);
    echo "<h1>Erro ao conectar no ClickHouse</h1>";
    if(isset($error['message'])){
        echo '<pre>' . htmlspecialchars($error['message']) . '</pre>';
    }
    exit;
}

$result = json_decode($response, true);
if ($result === null || !isset($result['data'])) {
    http_response_code(500);
    echo '<h1>Formato inesperado da resposta do ClickHouse</h1>';
    exit;
}

$dataRows = $result['data'];
// Prepare chart data arrays
// Compute bin width
$step = ($bucket_max - $bucket_min) / $bucket_bins;
$labels = [];
$scores = [];
foreach($dataRows as $row){
    // Calculate middle value of the bin for X‑axis label
    $binIndex = (int)$row['price_bin'];
    $labelVal = $bucket_min + ($binIndex - 1) * $step + $step / 2;
    $labels[] = round($labelVal, 2); // two decimals
    $scores[] = (int)$row['freq'];
}

// ------------------------------------------------------------------
// Raw data query – same filters but no aggregation
$rawQuery = "SELECT * FROM ofertas WHERE status IN ('C','O')";
if(isset($filters['pais'])){
    $rawQuery .= " AND pais='" . esc($filters['pais']) . "'";
}
if(isset($filters['periodo'])){
    $rawQuery .= " AND periodo = {$filters['periodo']}";
}
if(isset($filters['tipo_oferta'])){
    $rawQuery .= " AND tipo_oferta='" . esc($filters['tipo_oferta']) . "'";
}
if(isset($filters['status'])){
    // override the IN clause if a single status is chosen
    $rawQuery = str_replace("status IN ('C','O')", "status='" . esc($filters['status']) . "'", $rawQuery);
}
$rawQuery .= " AND data='" . esc($dia) . "' ORDER BY preco ASC;";

// Execute raw query
$urlRaw = "http://$host:$port/?user=$user&password=$password&default_format=JSON&query=" . urlencode($rawQuery);
$responseRaw = @file_get_contents($urlRaw);
if ($responseRaw === false) {
    $error = error_get_last();
    http_response_code(500);
    echo "<h1>Erro ao consultar ClickHouse (dados brutos)</h1>";
    if(isset($error['message'])){
        echo '<pre>' . htmlspecialchars($error['message']) . '</pre>';
    }
    exit;
}
$rawResult = json_decode($responseRaw, true);
if ($rawResult === null || !isset($rawResult['data'])) {
    http_response_code(500);
    echo '<h1>Formato inesperado da resposta do ClickHouse (dados brutos)</h1>';
    exit;
}
$rawRows = $rawResult['data'];

?>
<!DOCTYPE html>
<html lang="pt">
<head>
<meta charset="UTF-8">
<title>Distribuição de Bid – <?= htmlspecialchars($dia) ?></title>
<link rel="stylesheet" href="style.css">
</head>
<body>
<div class="container">
<h1>Distribuição de Bid (Preço vs Frequência)</h1>
<nav>
	<a href="index.php">Consulta Geral</a> |
	<a href="clearing.php">Curva de Clearing</a> |
	<a href="clearing_fixed.php">Curva de Clearing com PRE fixado</a> |
	<a href="frequency_distribution.php">Distribuição Bid</a> |
	<a href="daily_clearing.php">Clearing price diário</a> |
</nav>

<form method="get" style="margin-bottom:20px;">
    <label for="pais">País:</label>
    <select id="pais" name="pais">
        <option value="">Todos</option>
        <?php foreach(['MI','PT','ES'] as $c): ?>
            <option value="<?= $c ?>" <?= isset($filters['pais']) && $filters['pais']==$c ? 'selected':'' ?>><?= $c ?></option>
        <?php endforeach; ?>
    </select>

    <label for="periodo">Período:</label>
    <input type="number" id="periodo" name="periodo" min="1" max="25" value="<?= isset($filters['periodo']) ? $filters['periodo'] : '' ?>">

    <label for="tipo_oferta">Tipo de Oferta:</label>
    <select id="tipo_oferta" name="tipo_oferta">
        <option value="">Todos</option>
        <?php foreach(['C','V'] as $t): ?>
            <option value="<?= $t ?>" <?= isset($filters['tipo_oferta']) && $filters['tipo_oferta']==$t ? 'selected':'' ?>><?= $t ?></option>
        <?php endforeach; ?>
    </select>

    <label for="status">Status:</label>
    <select id="status" name="status">
        <option value="">Todos</option>
        <?php foreach(['C','O'] as $s): ?>
            <option value="<?= $s ?>" <?= isset($filters['status']) && $filters['status']==$s ? 'selected':'' ?>><?= $s ?></option>
        <?php endforeach; ?>
    </select>

    <label for="data">Data:</label>
    <input type="date" id="data" name="data" value="<?= htmlspecialchars($dia) ?>">

    <label for="bucket_bins">Nº de Bins:</label>
<input type="number" id="bucket_bins" name="bucket_bins" min="1" value="<?= isset($_GET['bucket_bins']) ? (int)$_GET['bucket_bins'] : '' ?>">
<button type="submit">Filtrar</button>
</form>

<div style="width:100%; height:600px;"> <canvas id="freqChart" style="height:100%; width:100%;"></canvas></div>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const ctx = document.getElementById('freqChart').getContext('2d');
new Chart(ctx, {
    type: 'bar',
    data: {
        labels: <?= json_encode($labels) ?>,
        datasets: [{
            label: 'Frequência',
            data: <?= json_encode($scores) ?>,
            backgroundColor: 'rgba(54, 162, 235, 0.5)',
            borderColor: 'rgb(54, 162, 235)',
            borderWidth: 1
        }]
    },
    options: {
        responsive:true,
        scales:{
            x:{type:'linear', position:'bottom', title:{display:true, labelString:'Preço (€)'}},
            y:{beginAtZero:true, title:{display:true, labelString:'Frequência'}}
        }
    }
});
</script>
<script>
window.addEventListener('DOMContentLoaded', function(){
    var toggleBtn = document.getElementById('toggleTableBtn');
    var tbl = document.getElementById('freqTable');
    if(!tbl) return;
    // Initially hide table
    tbl.style.display = 'none';
    toggleBtn.textContent = 'Mostrar Tabela';
    toggleBtn.addEventListener('click', function(){
        var computedDisplay = window.getComputedStyle(tbl).display;
        if (computedDisplay === 'none' || tbl.style.display === 'none') {
            tbl.style.display = '';
            this.textContent = 'Ocultar Tabela';
        } else {
            tbl.style.display = 'none';
            this.textContent = 'Mostrar Tabela';
        }
    });
});
</script>

<h2>Tabela de Frequências <button id="toggleTableBtn" style="margin-left:1rem;">Mostrar Tabela</button></h2>
<table id="freqTable" border="1">
<thead><tr><th>Bin</th><th>Frequência</th></tr></thead>
<tbody>
<?php foreach($dataRows as $row): ?>
<tr><td><?= htmlspecialchars((int)$row['price_bin']) ?></td><td><?= htmlspecialchars((int)$row['freq']) ?></td></tr>
<?php endforeach; ?>
</tbody>
</table>

<?php if (!empty($rawRows)): ?>
<h2>Dados Brutos <button id="toggleRawBtn" style="margin-left:1rem;">Mostrar Tabela</button></h2>
<table id="rawTable" border="1">
<thead><tr><?php foreach($rawRows[0] as $col => $val): ?><th><?= htmlspecialchars($col) ?></th><?php endforeach; ?></tr></thead>
<tbody>
<?php foreach($rawRows as $row): ?>
<tr><?php foreach($row as $cell): ?><td><?= htmlspecialchars($cell) ?></td><?php endforeach; ?></tr>
<?php endforeach; ?>
</tbody>
</table>

<script>
window.addEventListener('DOMContentLoaded', function(){
    var toggleRawBtn = document.getElementById('toggleRawBtn');
    var tblRaw = document.getElementById('rawTable');
    if(!tblRaw) return;
    // Initially hide table
    tblRaw.style.display = 'none';
    toggleRawBtn.textContent = 'Mostrar Tabela';
    toggleRawBtn.addEventListener('click', function(){
        var computedDisplay = window.getComputedStyle(tblRaw).display;
        if (computedDisplay === 'none' || tblRaw.style.display === 'none') {
            tblRaw.style.display = '';
            this.textContent = 'Ocultar Tabela';
        } else {
            tblRaw.style.display = 'none';
            this.textContent = 'Mostrar Tabela';
        }
    });
});
</script>
<?php endif; ?>

</div>
</body>
</html>