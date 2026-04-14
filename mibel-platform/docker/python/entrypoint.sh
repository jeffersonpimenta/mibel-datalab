#!/bin/sh
set -e

# ── 1. Aguardar ClickHouse ficar disponível ───────────────────────────────────
echo "[init] A aguardar ClickHouse (${CLICKHOUSE_HOST:-clickhouse}:${CLICKHOUSE_PORT:-9000})..."
python - <<'EOF'
import sys, time, os
from clickhouse_driver import Client

host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
port = int(os.getenv('CLICKHOUSE_PORT', '9000'))

for i in range(60):
    try:
        Client(host=host, port=port).execute('SELECT 1')
        print(f"[init] ClickHouse disponível (tentativa {i+1}).")
        sys.exit(0)
    except Exception as e:
        print(f"[init] tentativa {i+1}/60: {e}", flush=True)
        time.sleep(2)

print("[init] ERRO: ClickHouse não respondeu após 120s.")
sys.exit(1)
EOF

# ── 2. Garantir que a tabela mibel.unidades existe ───────────────────────────
echo "[init] A verificar tabela mibel.unidades..."
python - <<'EOF'
import os
from clickhouse_driver import Client

host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
ch = Client(host=host, port=port, database='mibel')

ch.execute("""
    CREATE TABLE IF NOT EXISTS mibel.unidades (
        codigo          String,
        descricao       String,
        agente          String,
        tipo_unidad     String,
        zona_frontera   String,
        tecnologia      String,
        regime          String,
        categoria       String,
        atualizado_em   DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(atualizado_em)
    ORDER BY (codigo)
""")
print("[init] Tabela mibel.unidades pronta.")
EOF

# ── 3. Carregar classificação de unidades (LISTA_UNIDADES.csv → mibel.unidades) ──
LISTA_CSV="/scripts/unidades/LISTA_UNIDADES.csv"
SCRIPT_PY="/scripts/unidades/carrega_unidades_ch.py"

if [ -f "$LISTA_CSV" ] && [ -f "$SCRIPT_PY" ]; then
    echo "[init] A carregar classificação de unidades..."
    python "$SCRIPT_PY" \
        --csv  "$LISTA_CSV" \
        --host "${CLICKHOUSE_HOST:-clickhouse}" \
        --port "${CLICKHOUSE_PORT:-9000}"
    echo "[init] Classificação carregada com sucesso."
else
    echo "[init] AVISO: $LISTA_CSV ou $SCRIPT_PY não encontrado — mibel.unidades não foi populada."
    echo "[init]        Monte ./scripts/unidades em /scripts/unidades para activar a ingestão automática."
fi

# ── 4. Iniciar processo principal ─────────────────────────────────────────────
echo "[init] A iniciar worker..."
exec "$@"
