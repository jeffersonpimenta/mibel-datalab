<?php
if ($_SERVER["REQUEST_METHOD"] == "POST") {
    $host = "clickhouse";
    $port = 8123;
    $user = "default";
    $password = "";

    $query = $_POST['query'];
    $url = "http://$host:$port/?user=$user&password=$password&query=" . urlencode($query);

    $response = file_get_contents($url);

    if ($response === false) {
        echo json_encode(["error" => "Erro ao conectar no ClickHouse"]);
    } else {
        echo json_encode(["result" => htmlspecialchars($response)]);
    }
}
