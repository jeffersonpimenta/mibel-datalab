<?php
// Frequency Distribution of Bid Prices with Filters
// -------------------------------------------------
// This page allows the user to filter offers by country (pais), date, and period
// and visualises a histogram of bid prices.
//
// Parameters are passed via GET: pais, data, periodo
// Example: frequency_distribution.php?pais=MI&data=2024-01-15&periodo=12

$host = 'clickhouse';
$port = 8123;
$user = 'default';
$password = '';

// Read filter values from GET with defaults
$pais    = isset($_GET['pais']) ? $_GET['pais'] : '';
$data    = isset($_GET['data']) ? $_GET['data'] : '';
$periodo = isset($_GET['periodo']) ? (int)$_GET['periodo'] : 0; // 0 means no filter
$offerType = isset($_GET['offer_type']) ? $_GET['offer_type'] : ''; // C or V, empty for all

// Build the base query for bid offers
$sql = "SELECT round(preco,2) AS price_bin, COUNT(*) AS freq\n";
$sql .= "FROM default.ofertas\n";
$sql .= "WHERE status='C'"; // only active offers
if ($offerType !== '') {
    $sql .= " AND tipo_oferta = '" . addslashes($offerType) . "'";
}


// Apply optional filters
if ($pais !== '') {
    $sql .= " AND pais = '" . addslashes($pais) . "'";
}
if ($data !== '') {
    $sql .= " AND data = '" . addslashes($data) . "'";
}
if ($periodo > 0 && $periodo <= 24) {
    $sql .= " AND periodo = {$periodo}";
}

$sql .= "\nGROUP BY round(preco,2)\nORDER BY price_bin ASC\nFORMAT JSONCompact";

// Execute query via HTTP
$url = "http://{$host}:{$port}/?user={$user}&password={$password}&query=" . urlencode($sql);
$response = @file_get_contents($url);
$chartData = [];
if ($response !== false) {
    $dataArr = json_decode($response, true);
    if (is_array($dataArr) && isset($dataArr['data'])) {
        foreach ($dataArr['data'] as $row) {
            // Ensure keys exist
            if (isset($row['price_bin']) && isset($row['freq'])) {
                $chartData[] = [
                    'label' => number_format((float)$row['price_bin'], 2),
                    'value' => (int)$row['freq']
                ];
            }
        }
    }
}
?>
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <title>Distribuição de Frequência do Preço Bid</title>
    <link rel="stylesheet" href="style.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
<div class="container">
    <h1>Distribuição de Frequência do Preço Bid</h1>
    <nav>
        <a href="clearing.php">Curva de Clearing</a> |
        <a href="index.php">Consulta Geral</a> |
        <a href="frequency_distribution.php">Distribuição Bid</a>
    </nav>

    <form method="get" id="filterForm">
        <label for="pais">País:</label>
        <select name="pais" id="pais">
            <option value=""<?php echo $pais==='' ? ' selected' : ''; ?>>Todos</option>
            <?php
            // Populate country options from distinct query
            $countries = [];
            $sqlCountries = "SELECT DISTINCT pais FROM default.ofertas FORMAT JSONCompact";
            $urlCountries = "http://{$host}:{$port}/?user={$user}&password={$password}&query=" . urlencode($sqlCountries);
            $resC = @file_get_contents($urlCountries);
            if ($resC !== false) {
                $arrC = json_decode($resC, true);
                if (is_array($arrC) && isset($arrC['data'])) {
                    foreach ($arrC['data'] as $rowC) {
                        if (isset($rowC['pais'])) {
                            $countries[] = $rowC['pais'];
                        }
                    }
                }
            }
            sort($countries);
            foreach ($countries as $c) {
                echo '<option value="' . htmlspecialchars($c) . '"' . ($pais===$c?' selected':'') . '>' . htmlspecialchars($c) . '</option>';
            }
            ?>
        </select>

        <label for="offer_type">Tipo de Oferta:</label>
        <select name="offer_type" id="offer_type">
            <option value=""<?php echo $offerType==='' ? ' selected' : ''; ?>>Todos</option>
            <option value="C" <?php echo $offerType==='C' ? ' selected' : ''; ?>>Compra (C)</option>
            <option value="V" <?php echo $offerType==='V' ? ' selected' : ''; ?>>Venda (V)</option>
        </select>

        <label for="data">Data:</label>
        <input type="date" id="data" name="data" value="<?php echo htmlspecialchars($data); ?>">

        <label for="periodo">Período (1-24):</label>
        <select id="periodo" name="periodo">
            <option value="0"<?php echo $periodo===0?' selected':''; ?>>Todos</option>
            <?php for ($i=1;$i<=24;$i++) {
                echo '<option value="'.$i.'"'.($periodo===$i?' selected':'').'>'.$i.'</option>';
            } ?>
        </select>

        <input type="submit" value="Filtrar">
    </form>

    <?php if (!empty($chartData)) : ?>
        <canvas id="frequencyChart"></canvas>
        <script>
            const ctx = document.getElementById('frequencyChart').getContext('2d');
            const labels = <?php echo json_encode(array_column($chartData, 'label')); ?>;
            const dataValues = <?php echo json_encode(array_column($chartData, 'value')); ?>;

            new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Frequência',
                        data: dataValues,
                        backgroundColor: 'rgba(54, 162, 235, 0.5)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }]
                },
                options: {
                    scales: {
                        x: { title: { display: true, text: 'Preço (R$)' } },
                        y: { beginAtZero: true, title: { display: true, text: 'Frequência' } }
                    }
                }
            });
        </script>
    <?php else : ?>
        <p>Não há dados para os filtros selecionados.</p>
    <?php endif; ?>

</div>
</body>
</html>
