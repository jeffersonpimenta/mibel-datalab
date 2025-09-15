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
$query  = "SELECT width_bucket(preco, -10000, 10000, 500) AS price_bin, COUNT(*) AS freq FROM ofertas WHERE status IN ('C','O')";

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
$labels = [];
$scores = [];
foreach($dataRows as $row){
    $labels[] = (int)$row['price_bin'];
    $scores[] = (int)$row['freq'];
}

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

    <button type="submit">Filtrar</button>
</form>

<canvas id="freqChart"></canvas>
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
            x:{title:{display:true,labelString:'Bin de Preço'}},
            y:{beginAtZero:true,title:{display:true,labelString:'Frequência'}}
        }
    }
});
</script>

<h2>Tabela de Frequências</h2>
<table id="freqTable" border="1">
<thead><tr><th>Bin</th><th>Frequência</th></tr></thead>
<tbody>
<?php foreach($dataRows as $row): ?>
<tr><td><?= htmlspecialchars((int)$row['price_bin']) ?></td><td><?= htmlspecialchars((int)$row['freq']) ?></td></tr>
<?php endforeach; ?>
</tbody>
</table>
</div>
</body>
</html>