<?php
$output = '';
if ($_SERVER['REQUEST_METHOD'] === 'POST' && isset($_POST['query'])) {
    $query = $_POST['query'];
    // Connection details (same as clearing.php)
    $host = 'clickhouse';
    $port = 8123;
    $user = 'default';
    $password = '';
    $url = "http://$host:$port/?user=$user&password=$password&default_format=JSON&query=" . urlencode($query);
    $response = @file_get_contents($url);
    if ($response === false) {
        $output = '<p>Erro ao conectar ao ClickHouse.</p>';
    } else {
        $result = json_decode($response, true);
        if (!isset($result['data'])) {
            $output = '<p>Formato inesperado da resposta do ClickHouse.</p>';
        } else {
            $rows = $result['data'];
            $cols = array_keys(reset($rows));
            $table = '<table border="1"><thead><tr>';
            foreach ($cols as $c) { $table .= "<th>$c</th>"; }
            $table .= '</tr></thead><tbody>';
            foreach ($rows as $row) {
                $table .= '<tr>';
                foreach ($cols as $c) { $table .= "<td>{$row[$c]}</td>"; }
                $table .= '</tr>';
            }
            $table .= '</tbody></table>';
            $output = '<h2>Resultado:</h2>' . $table;
        }
    }
}
?>
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <title>ClickHouse UI</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <div class="container">
    <h1>Consulta ao ClickHouse</h1>
    <nav>
	    <a href="index.php">Consulta Geral</a> |
        <a href="clearing.php">Curva de Clearing</a> |
        <a href="clearing_fixed.php">Curva de Clearing com PRE fixado</a> |
        <a href="frequency_distribution.php">Distribuição Bid</a> |
		<a href="daily_clearing.php">Clearing price diário</a> |
    </nav>

    <div id="presetQueries">
	    <button type="button" onclick="setAndSubmit('DESCRIBE TABLE default.ofertas')">Descrever tabela</button>
        <button type="button" onclick="setAndSubmit('SELECT * FROM default.ofertas LIMIT 10')">Primeiras 10 linhas</button>
        <button type="button" onclick="setAndSubmit('SELECT pais, COUNT(*) AS cnt FROM default.ofertas GROUP BY pais ORDER BY cnt DESC')">Contagem por país</button>
        <button type="button" onclick="setAndSubmit('SELECT pais, AVG(preco) AS avg_price FROM default.ofertas WHERE status = \'O\' GROUP BY pais')">Preço médio</button>
        <button type="button" onclick="setAndSubmit('SELECT tipo_oferta, SUM(volume) AS total_volume FROM default.ofertas GROUP BY tipo_oferta')">Volume total por bid</button>
        <button type="button" onclick="setAndSubmit('SELECT status, COUNT(*) AS cnt FROM default.ofertas GROUP BY status ORDER BY cnt DESC')">Contagem por status</button>
		<button type="button" onclick="setAndSubmit('SELECT DISTINCT periodo FROM default.ofertas')">Distinct Periodo</button>
		<button type="button" onclick="setAndSubmit('SELECT DISTINCT data FROM default.ofertas')">Distinct Data</button>
		<button type="button" onclick="setAndSubmit('SELECT DISTINCT pais FROM default.ofertas')">Distinct Pais</button>
		<button type="button" onclick="setAndSubmit('SELECT DISTINCT tipo_oferta FROM default.ofertas')">Distinct Tipo Oferta</button>
		<button type="button" onclick="setAndSubmit('SELECT DISTINCT status FROM default.ofertas')">Distinct Status</button>
		<button type="button" onclick="setAndSubmit('SELECT DISTINCT tipologia FROM default.ofertas')">Distinct Tipologia</button>
		<button type="button" onclick="setAndSubmit('SELECT DISTINCT arquivo FROM default.ofertas')">Distinct Arquivo</button>
		<button type="button" onclick="setAndSubmit('SELECT pais, SUM(volume) AS total_volume FROM default.ofertas GROUP BY pais ORDER BY total_volume DESC')">Volume total por país</button>
		<button type="button" onclick="setAndSubmit('SELECT pais, MAX(preco) AS max_price FROM default.ofertas WHERE status = \'O\' GROUP BY pais ORDER BY max_price DESC')">Valor máximo por país</button>
		<button type="button" onclick="setAndSubmit('SELECT tipo_oferta, AVG(volume) AS avg_volume FROM default.ofertas GROUP BY tipo_oferta')">Volume médio por tipo de oferta</button>
		<button type="button" onclick="setAndSubmit('SELECT toYYYYMM(data) AS year_month, COUNT(*) AS cnt FROM default.ofertas GROUP BY year_month ORDER BY year_month DESC LIMIT 12')">Ofertas mensais</button>
    </div>

    <form id="queryForm" method="post">
        <label for="query">Consulta SQL:</label>
        <textarea id="query" name="query" rows="4" cols="50"><?php echo htmlspecialchars($_POST['query'] ?? '', ENT_QUOTES, 'UTF-8'); ?></textarea><br>
        <input type="submit" value="Executar Consulta">
    </form>
    <div id="result"><?php echo $output; ?></div>

    <script>
        function setAndSubmit(q){
            document.getElementById('query').value=q;
            document.forms[0].submit();
        }
    </script>
</div>
</body>
</html>