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
        <a href="clearing.php">Curva de Clearing</a> |
        <a href="index.php">Consulta Geral</a> |
        <a href="frequency_distribution.php">Distribuição Bid</a>
    </nav>

    <div id="presetQueries">
        <button type="button" onclick="setAndSubmit('SELECT * FROM default.ofertas LIMIT 10')">First 10 Rows</button>
        <button type="button" onclick="setAndSubmit('SELECT pais, COUNT(*) AS cnt FROM default.ofertas GROUP BY pais ORDER BY cnt DESC')">Count per Country</button>
        <button type="button" onclick="setAndSubmit('SELECT pais, AVG(preco) AS avg_price FROM default.ofertas WHERE status = \'O\' GROUP BY pais')">Avg Price (Active)</button>
        <button type="button" onclick="setAndSubmit('SELECT tipo_oferta, SUM(volume) AS total_volume FROM default.ofertas GROUP BY tipo_oferta')">Total Volume per Offer Type</button>
        <button type="button" onclick="setAndSubmit('SELECT status, COUNT(*) AS cnt FROM default.ofertas GROUP BY status ORDER BY cnt DESC')">Count per Status</button>
        <button type="button" onclick="setAndSubmit('SELECT tipologia, COUNT(*) AS cnt FROM default.ofertas GROUP BY tipologia ORDER BY cnt DESC LIMIT 5')">Top 5 Tipologias</button>
        <button type="button" onclick="setAndSubmit('DESCRIBE TABLE default.ofertas')">Describe Table</button>
<button type="button" onclick="setAndSubmit('SELECT DISTINCT id FROM default.ofertas')">Distinct ID</button>
<button type="button" onclick="setAndSubmit('SELECT DISTINCT periodo FROM default.ofertas')">Distinct Periodo</button>
<button type="button" onclick="setAndSubmit('SELECT DISTINCT data FROM default.ofertas')">Distinct Data</button>
<button type="button" onclick="setAndSubmit('SELECT DISTINCT pais FROM default.ofertas')">Distinct Pais</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT DISTINCT tipo_oferta FROM default.ofertas')\">Distinct Tipo Oferta</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT DISTINCT volume FROM default.ofertas')\">Distinct Volume</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT DISTINCT preco FROM default.ofertas')\">Distinct Pre\xC3\xA7o</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT DISTINCT status FROM default.ofertas')\">Distinct Status</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT DISTINCT tipologia FROM default.ofertas')\">Distinct Tipologia</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT DISTINCT arquivo FROM default.ofertas')\">Distinct Arquivo</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT pais, SUM(volume) AS total_volume FROM default.ofertas GROUP BY pais ORDER BY total_volume DESC')\">Total Volume per Country</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT pais, MAX(preco) AS max_price FROM default.ofertas WHERE status = \'O\' GROUP BY pais ORDER BY max_price DESC')\">Max Price per Country (Active)</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT tipo_oferta, AVG(volume) AS avg_volume FROM default.ofertas GROUP BY tipo_oferta')\">Avg Volume per Offer Type</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT data FROM default.ofertas ORDER BY data DESC LIMIT 1')\">Most Recent Offer Date</button>
<button type=\"button\" onclick=\"setAndSubmit('SELECT toYYYYMM(data) AS year_month, COUNT(*) AS cnt FROM default.ofertas GROUP BY year_month ORDER BY year_month DESC LIMIT 12')\">Offers per Month (Last Year)</button>
    </div>

    <form id="queryForm">
        <label for="query">Consulta SQL:</label>
        <textarea id="query" name="query" rows="4" cols="50"></textarea><br>
        <input type="submit" value="Executar Consulta">
    </form>
    <div id="result"></div>

    <script>
        function setAndSubmit(q){
            document.getElementById('query').value=q;
            document.getElementById('queryForm').dispatchEvent(new Event('submit'));
        }

        document.getElementById('queryForm').addEventListener('submit', function(event) {
            event.preventDefault();
            var query = document.getElementById('query').value;
            var xhr = new XMLHttpRequest();
            xhr.open('POST', 'process_query.php', true);
            xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
            xhr.onreadystatechange = function () {
                if (xhr.readyState === 4 && xhr.status === 200) {
                    var response = JSON.parse(xhr.responseText);
                    var resultDiv = document.getElementById('result');
                    resultDiv.innerHTML = '';
                    if (response.error) {
                        resultDiv.innerHTML = '<p>' + response.error + '</p>';
                    } else {
                        resultDiv.innerHTML = '<h2>Resposta do ClickHouse:</h2><pre>' + response.result + '</pre>';
                    }
                }
            };
            xhr.send('query=' + encodeURIComponent(query));
        });
    </script>
</div>
</body>
</html>