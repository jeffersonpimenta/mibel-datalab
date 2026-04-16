<?php
/**
 * MIBEL Platform - Job Queue Management
 *
 * SQLite-based job queue for managing analysis tasks.
 */

declare(strict_types=1);

class Jobs
{
    private \PDO $pdo;

    public function __construct(string $path = '/data/jobs.db')
    {
        $this->pdo = new \PDO("sqlite:{$path}");
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $this->pdo->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC);
    }

    /**
     * Generate UUID v4
     */
    private function generateUuid(): string
    {
        $data = random_bytes(16);
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40); // Version 4
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80); // Variant

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }

    /**
     * Create a new job and return its UUID
     */
    public function create(
        string $tipo,
        string $dataInicio,
        string $dataFim,
        string $observacoes,
        int $workers
    ): string {
        $id = $this->generateUuid();

        $stmt = $this->pdo->prepare("
            INSERT INTO jobs (id, tipo, data_inicio, data_fim, observacoes, workers_n, status)
            VALUES (:id, :tipo, :data_inicio, :data_fim, :observacoes, :workers_n, 'PENDING')
        ");

        $stmt->execute([
            ':id' => $id,
            ':tipo' => $tipo,
            ':data_inicio' => $dataInicio,
            ':data_fim' => $dataFim,
            ':observacoes' => $observacoes,
            ':workers_n' => $workers,
        ]);

        return $id;
    }

    /**
     * Update job status with optional result/error data
     */
    public function updateStatus(
        string $id,
        string $status,
        string $resultado = '',
        string $erro = ''
    ): void {
        $sql = "UPDATE jobs SET status = :status";
        $params = [':id' => $id, ':status' => $status];

        if ($resultado !== '') {
            $sql .= ", resultado = :resultado";
            $params[':resultado'] = $resultado;
        }

        if ($erro !== '') {
            $sql .= ", erro = :erro";
            $params[':erro'] = $erro;
        }

        if ($status === 'DONE' || $status === 'FAILED') {
            $sql .= ", finished_at = datetime('now')";
        }

        $sql .= " WHERE id = :id";

        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
    }

    /**
     * Mark job as RUNNING and set started_at timestamp
     */
    public function markRunning(string $id): void
    {
        $stmt = $this->pdo->prepare("
            UPDATE jobs
            SET status = 'RUNNING', started_at = datetime('now')
            WHERE id = :id
        ");
        $stmt->execute([':id' => $id]);
    }

    /**
     * Get a single job by ID
     */
    public function get(string $id): ?array
    {
        $stmt = $this->pdo->prepare("SELECT * FROM jobs WHERE id = :id");
        $stmt->execute([':id' => $id]);
        $result = $stmt->fetch();

        return $result ?: null;
    }

    /**
     * List jobs ordered by created_at DESC
     */
    public function list(int $limit = 30): array
    {
        $stmt = $this->pdo->prepare("
            SELECT * FROM jobs
            ORDER BY created_at DESC
            LIMIT :limit
        ");
        $stmt->bindValue(':limit', $limit, \PDO::PARAM_INT);
        $stmt->execute();

        return $stmt->fetchAll();
    }

    /**
     * Mark a date as ingested with bid count
     */
    public function markIngerido(string $dataFicheiro, int $nBids): void
    {
        $stmt = $this->pdo->prepare("
            INSERT OR REPLACE INTO bids_ingeridos (data_ficheiro, n_bids, ingerido_em)
            VALUES (:data_ficheiro, :n_bids, datetime('now'))
        ");
        $stmt->execute([
            ':data_ficheiro' => $dataFicheiro,
            ':n_bids' => $nBids,
        ]);
    }

    /**
     * Get all ingested dates
     */
    public function getIngeridos(): array
    {
        $stmt = $this->pdo->query("
            SELECT data_ficheiro, n_bids, ingerido_em
            FROM bids_ingeridos
            ORDER BY data_ficheiro DESC
        ");

        return $stmt->fetchAll();
    }

    /**
     * Check if a specific date has been ingested
     */
    public function isIngerido(string $dataFicheiro): bool
    {
        $stmt = $this->pdo->prepare("
            SELECT 1 FROM bids_ingeridos WHERE data_ficheiro = :data_ficheiro
        ");
        $stmt->execute([':data_ficheiro' => $dataFicheiro]);

        return $stmt->fetch() !== false;
    }

    /**
     * Delete a job by ID (PENDING, FAILED or DONE — not RUNNING)
     */
    public function delete(string $id): bool
    {
        $stmt = $this->pdo->prepare("
            DELETE FROM jobs
            WHERE id = :id AND status IN ('PENDING', 'FAILED', 'DONE')
        ");
        $stmt->execute([':id' => $id]);

        return $stmt->rowCount() > 0;
    }

    /**
     * Update the observations text of a job
     */
    public function updateObservacoes(string $id, string $observacoes): void
    {
        $stmt = $this->pdo->prepare("UPDATE jobs SET observacoes = :obs WHERE id = :id");
        $stmt->execute([':obs' => $observacoes, ':id' => $id]);
    }

    /**
     * Get jobs by status
     */
    public function getByStatus(string $status): array
    {
        $stmt = $this->pdo->prepare("
            SELECT * FROM jobs
            WHERE status = :status
            ORDER BY created_at ASC
        ");
        $stmt->execute([':status' => $status]);

        return $stmt->fetchAll();
    }
}
