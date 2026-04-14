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

# ── Aguardar ClickHouse ficar disponível (máx 60s) ────────────────────────────
echo "[entrypoint] A aguardar ClickHouse..."
TRIES=0
until php -r "exit(@file_get_contents('http://clickhouse:8123/ping')==='Ok.'?0:1);" 2>/dev/null; do
    TRIES=$((TRIES + 1))
    if [ "$TRIES" -ge 30 ]; then
        echo "[entrypoint] AVISO: ClickHouse não respondeu após 60s — a continuar sem ele"
        break
    fi
    echo "[entrypoint] ClickHouse não disponível, nova tentativa em 2s... ($TRIES/30)"
    sleep 2
done
[ "$TRIES" -lt 30 ] && echo "[entrypoint] ClickHouse disponível (tentativa $TRIES)"

# ── Executar migração ──────────────────────────────────────────────────────────
echo "[entrypoint] A executar migrate.php..."
php /app/src/migrate.php || true

echo "[entrypoint] A iniciar php-fpm..."
exec php-fpm
