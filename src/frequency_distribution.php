<?php
// Frequency distribution of bid prices from ClickHouse offers
$host      = 'clickhouse';
$port      = 8123;
$user      = 'default';
$password  = '';

// Read filters from GET, with defaults
$pais        = isset($_GET['pais']) ? $_GET['pais'] : 'MI'; // MI default
$periodo     = isset($_GET['periodo']) && ctype_digit($_GET['periodo']) && (int)$_GET['periodo'] >= 1 && (int)$_GET['periodo'] <= 25 ? (int)$_GET['periodo'] : null;
$tipo_oferta = isset($_GET['tipo_oferta']) ? $_GET['tipo_oferta'] : null; // C V
$status      = isset($_GET['status']) ? $_GET['status'] : null;
$data        = isset($_GET['data']) && preg_match('/^\\d{4}-\\d{2}-\\d{2}$/', $_GET['data']) ? $_GET['data'] : date('Y-m-d');

// Helper to build IN clause from comma separated list or single value
function build_in_clause(string $field, ?string $value): string {
    if ($value === null) return '';
    $parts = array_map('trim', explode(',', $value));
    // Escape each part for SQL injection safety (simple replace of single quote)
    $escaped = array_map(fn($v)=>str_replace("'", "''", $v), $parts);
    if (count($escaped) === 1) {
        return "$field = '$escaped[0]'";
    }
    return "$field IN ('" . implode("','", $escaped) . ")";
}

// Build query
$filters = [];
$filters[] = build_in_clause('pais', $pais);
if ($periodo !== null) {
    $filters[] = "periodo = $periodo";
}
$filters[] = "data = '$data'";
$filters[] = build_in_clause('tipo_oferta', $tipo_oferta);
$filters[] = build_in_clause('status', $status);

// Remove empty filters
$filters = array_filter($filters, fn($f)=>trim($f)!=='');
$whereClause = count($filters) ? 'WHERE ' . implode(' AND ', $filters) : '';

$query = "SELECT width_bucket(preco, -10000, 10000, 500) AS price_bin, COUNT(*) AS freq FROM ofertas \n$whereClause GROUP BY price_bin ORDER BY price_bin ASC";

$url = "http://$host:$port/?user=$user&password=$password&default_format=JSON&query=" . urlencode($query);
$response = @file_get_contents($url);
if ($response === false) {
    http_response_code(500);
    echo '<h1>Erro ao consultar ClickHouse para distribuição de frequências</h1>';
    exit;
}
$result = json_decode($response, true);
$rows   = $result['data'] ?? [];

// Prepare chart data: labels and frequencies
$labels = [];
$dataY  = [];
foreach ($rows as $row) {
    $bin   = (int)$row['price_bin'];
    // Calculate bin center value for label
    $width = 20000 / 500; // from -10000 to 10000
    $center = -10000 + ($bin - 0.5) * $width;
    $labels[] = number_format($center, 2);
    $dataY[]   = (int)$row['freq'];
}
?>
<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <title>Distribuição de Frequência de Preços - <?= htmlspecialchars($data) ?></title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
<div class="container">
<h1>Distribuição de Frequência de Preços (Bid Prices)</h1>
<form method="get" style="margin-bottom: 1rem; display:flex; flex-wrap:wrap; gap:0.5rem; align-items:center;">
    <label for="pais">País:</label>
    <select id="pais" name="pais">
        <?php
        $countries = ['MI', 'PT', 'ES'];
        foreach ($countries as $c) {
            echo '<option value="'.$c.'"'.($pais===$c?' selected':'').'>'.$c.'</option>';
        }
        ?>
    </select>

    <label for="periodo">Período:</label>
    <input type="number" id="periodo" name="periodo" min="1" max="25" value="<?= $periodo ?? '' ?>">

    <label for="tipo_oferta">Tipo de Oferta:</label>
    <select id="tipo_oferta" name="tipo_oferta">
        <option value="" <?= empty($tipo_oferta) ? 'selected' : '' ?>>Todos</option>
        <option value="C" <?= $tipo_oferta==='C'?'selected':'' ?>>Compras (C)</option>
        <option value="V" <?= $tipo_oferta==='V'?'selected':'' ?>>Vendas (V)</option>
    </select>

    <label for="status">Status:</label>
    <select id="status" name="status">
        <option value="" <?= empty($status) ? 'selected' : '' ?>>Todos</option>
        <option value="C" <?= $status==='C'?'selected':'' ?>>Compras (C)</option>
        <option value="O" <?= $status==='O'?'selected':'' ?>>Operacional (O)</option>
    </select>

    <label for="data">Data:</label>
    <input type="date" id="data" name="data" value="<?= htmlspecialchars($data) ?>">

    <button type="submit">Consultar</button>
</form>

<canvas id="frequencyChart" width="800" height="400"></canvas>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
const ctx = document.getElementById('frequencyChart').getContext('2d');
new Chart(ctx, {
    type: 'bar',
    data: {
        labels: <?= json_encode($labels) ?>,
        datasets: [{
            label: 'Frequência',
            data: <?= json_encode($dataY) ?>,
            backgroundColor: 'rgba(54, 162, 235, 0.5)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1
        }]
    },
    options: {
        responsive: true,
        scales: {
            x: { title: { display: true, text: 'Preço (€)' } },
            y: { beginAtZero: true, title: { display: true, text: 'Frequência' } }
        }
    }
});
</script>

<h2>Detalhes da Distribuição</h2>
<table border="1" cellpadding="5">
<thead><tr><th>Bin (Preço)</th><th>Frequência</th></tr></thead>
<tbody>
<?php foreach ($rows as $row): ?>
    <?php
    $bin   = (int)$row['price_bin'];
    $width = 20000 / 500;
    $center = -10000 + ($bin - 0.5) * $width;
    ?>
    <tr><td><?= number_format($center,2) ?></td><td><?= (int)$row['freq'] ?></td></tr>
<?php endforeach; ?>
</tbody>
</table>
</div>
</body>
</html>