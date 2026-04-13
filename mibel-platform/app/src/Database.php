<?php
/**
 * MIBEL Platform - ClickHouse Database Access
 *
 * Provides HTTP-based access to ClickHouse database.
 */

declare(strict_types=1);

class Database
{
    private string $base;
    private static ?self $instance = null;

    private const TIMEOUT = 300;
    private const BATCH_SIZE = 5000;

    public function __construct(
        string $host = 'clickhouse',
        int    $port = 8123,
        string $db   = 'mibel'
    ) {
        $this->base = "http://{$host}:{$port}/?database=" . urlencode($db);
    }

    /**
     * Get singleton instance
     */
    public static function getInstance(): self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    /**
     * Execute a SELECT query and return results as associative arrays
     */
    public function query(string $sql): array
    {
        $url = $this->base . '&default_format=JSONEachRow';

        $response = $this->httpRequest($url, $sql);

        if (empty(trim($response))) {
            return [];
        }

        $results = [];
        $lines = explode("\n", trim($response));

        foreach ($lines as $line) {
            if (empty(trim($line))) {
                continue;
            }
            $decoded = json_decode($line, true);
            if ($decoded !== null) {
                $results[] = $decoded;
            }
        }

        return $results;
    }

    /**
     * Execute a non-SELECT query (INSERT, CREATE, ALTER, etc.)
     */
    public function execute(string $sql): bool
    {
        $this->httpRequest($this->base, $sql);
        return true;
    }

    /**
     * Insert multiple rows efficiently using JSONEachRow format
     * Splits into batches of BATCH_SIZE
     */
    public function insertRows(string $table, array $rows): bool
    {
        if (empty($rows)) {
            return true;
        }

        $batches = array_chunk($rows, self::BATCH_SIZE);

        foreach ($batches as $batch) {
            $jsonLines = [];
            foreach ($batch as $row) {
                $jsonLines[] = json_encode($row, JSON_UNESCAPED_UNICODE);
            }

            $sql = "INSERT INTO {$table} FORMAT JSONEachRow\n" . implode("\n", $jsonLines);
            $this->httpRequest($this->base, $sql);
        }

        return true;
    }

    /**
     * Check if ClickHouse is responding
     */
    public function ping(): bool
    {
        try {
            $parts = parse_url($this->base);
            $pingUrl = "{$parts['scheme']}://{$parts['host']}:{$parts['port']}/ping";

            $ch = curl_init();
            curl_setopt_array($ch, [
                CURLOPT_URL => $pingUrl,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => 5,
                CURLOPT_CONNECTTIMEOUT => 5,
            ]);

            $response = curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            return $httpCode === 200 && trim($response) === 'Ok.';
        } catch (\Throwable $e) {
            return false;
        }
    }

    /**
     * Execute HTTP request to ClickHouse
     */
    private function httpRequest(string $url, string $body): string
    {
        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_POST => true,
            CURLOPT_POSTFIELDS => $body,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => [
                'Content-Type: text/plain; charset=UTF-8',
            ],
            CURLOPT_TIMEOUT => self::TIMEOUT,
            CURLOPT_CONNECTTIMEOUT => 10,
        ]);

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $curlError = curl_error($ch);
        curl_close($ch);

        if ($curlError) {
            throw new \RuntimeException("ClickHouse connection error: {$curlError}");
        }

        if ($httpCode >= 400) {
            throw new \RuntimeException("ClickHouse error (HTTP {$httpCode}): {$response}");
        }

        return $response;
    }
}
