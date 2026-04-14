#!/bin/sh
set -e

# ── Garantir que os directórios de dados existem e são graváveis por www-data ──
mkdir -p /data/outputs /data/config /data/bids
chmod 777 /data/outputs /data/config /data/bids

# ── Dar acesso ao socket Docker ao processo www-data (necessário para docker exec) ──
if [ -S /var/run/docker.sock ]; then
    chmod 666 /var/run/docker.sock 2>/dev/null || true
    echo "[entrypoint] Docker socket disponível"
else
    echo "[entrypoint] AVISO: Docker socket não encontrado em /var/run/docker.sock"
fi

echo "[entrypoint] A executar migrate.php..."
php /app/src/migrate.php || true

echo "[entrypoint] A iniciar php-fpm..."
exec php-fpm
