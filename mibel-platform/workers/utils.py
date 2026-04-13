"""
MIBEL Platform - Worker Utilities

Shared utilities for Python workers: ClickHouse access, config loading,
logging, and data transformation helpers.
"""

import json
import os
import re
import glob
from datetime import date, datetime
from typing import Optional, Union
from clickhouse_driver import Client

# ============================================================================
# Configuration
# ============================================================================

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000'))
CONFIG_DIR = '/data/config'
BIDS_DIR = '/data/bids'
OUTPUTS_DIR = '/data/outputs'

# ============================================================================
# ClickHouse Connection
# ============================================================================

def get_ch() -> Client:
    """Get ClickHouse client connection."""
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database='mibel',
        settings={'use_numpy': False}
    )


def ch_insert_batch(ch: Client, table: str, rows: list, batch_size: int = 5000) -> int:
    """Insert rows in batches. Returns total inserted count."""
    if not rows:
        return 0

    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        ch.execute(f'INSERT INTO {table} VALUES', batch)
        total += len(batch)

    return total

# ============================================================================
# Configuration Loading
# ============================================================================

def load_json(nome: str) -> Union[dict, list]:
    """Load /data/config/{nome}.json"""
    path = f'{CONFIG_DIR}/{nome}.json'
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def carrega_escaloes() -> dict:
    """
    Load parametros.json and return ESCALOES dict compatible with original scripts.

    Structure:
    {
        "PRE": {
            "SOLAR_FOT_ES": {"escala": 2.2297, "escaloes": [{"preco": 0, "pct_bids": 0.3}, ...]},
            ...
        },
        "PRO": {...},
        ...
    }
    """
    return load_json('parametros')


def carrega_classificacao() -> list:
    """Load classificacao.json - list of {tecnologia, regime, categoria}."""
    return load_json('classificacao')


def carrega_excecoes() -> list:
    """Load excecoes.json - list of {codigo, categoria_zona, motivo}."""
    return load_json('excecoes')


def carrega_mapa_unidades() -> dict:
    """
    Build unit mapping from classificacao.json and excecoes.json.

    Returns:
        {
            'mapa_tec': {tecnologia: (regime, categoria_base), ...},
            'excecoes': {CODIGO_UPPER: categoria_zona, ...}
        }

    Note: The zona suffix (_ES, _PT, _EXT) is applied dynamically based on
    the 'Pais' field in bid data, since classificacao.json only has base categories.
    """
    classificacao = carrega_classificacao()
    excecoes = carrega_excecoes()

    mapa_tec = {
        e['tecnologia']: (e['regime'], e['categoria'])
        for e in classificacao
    }

    excecoes_map = {
        e['codigo'].upper(): e['categoria_zona']
        for e in excecoes
    }

    return {'mapa_tec': mapa_tec, 'excecoes': excecoes_map}

# ============================================================================
# Logging
# ============================================================================

def log(nivel: str, mensagem: str, job_id: str = '', ch: Optional[Client] = None):
    """
    Print log line and optionally insert into worker_logs table.

    Levels: OK, ERRO, INFO, AVISO, STATUS
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    linha = f'[{timestamp}] [{nivel}] {mensagem}'
    print(linha, flush=True)

    if ch and job_id:
        try:
            ch.execute(
                'INSERT INTO mibel.worker_logs (job_id, nivel, mensagem) VALUES',
                [{'job_id': job_id, 'nivel': nivel, 'mensagem': mensagem}]
            )
        except Exception as e:
            print(f'[AVISO] Failed to insert log: {e}', flush=True)

# ============================================================================
# Data Transformation Helpers
# ============================================================================

def normaliza_hora(valor: str) -> tuple:
    """
    Normalize hour value from OMIE bid files.

    Args:
        valor: Hour string like "1", "24", "H1Q1", "H24Q1"

    Returns:
        (hora_raw, hora_num, periodo_formato)
        - hora_raw: original string
        - hora_num: normalized 1-24 integer
        - periodo_formato: "NUM" or "HxQy"
    """
    v = str(valor).strip()

    # Match HxQy format (e.g., H1Q1, H24Q1)
    m = re.match(r'^H(\d+)Q\d+$', v, re.IGNORECASE)
    if m:
        return v, int(m.group(1)), 'HxQy'

    # Numeric format
    try:
        return v, int(v), 'NUM'
    except ValueError:
        return v, 0, 'UNK'


def extrai_data(nome_ficheiro: str) -> str:
    """
    Extract date from filename.

    'curva_pbc_uof_20260301.1' → '2026-03-01'
    """
    m = re.search(r'(\d{8})', nome_ficheiro)
    if m:
        d = m.group(1)
        return f'{d[:4]}-{d[4:6]}-{d[6:8]}'
    return '1970-01-01'


def sufixo_de_pais(pais: str) -> str:
    """
    Get category suffix based on country code.

    MI (Iberian market) → _ES (default, majority)
    ES → _ES
    PT → _PT
    Others → _EXT
    """
    p = str(pais).strip().upper()
    if p == 'PT':
        return '_PT'
    if p in ('ES', 'MI'):
        return '_ES'
    return '_EXT'


def get_categoria_zona(
    unidade: str,
    tecnologia: str,
    pais: str,
    mapa: dict,
    escaloes: dict
) -> tuple:
    """
    Get (regime, categoria_zona) for a unit.

    Priority:
    1. Exception by unit code (has full categoria_zona)
    2. Technology mapping + country suffix
    3. NAO_CLASSIFICADO

    Args:
        unidade: Unit code (will be uppercased)
        tecnologia: Technology name from OMIE
        pais: Country code (ES, PT, MI, etc.)
        mapa: Mapping dict from carrega_mapa_unidades()
        escaloes: Parameters dict from carrega_escaloes()

    Returns:
        (regime, categoria_zona) or ('NAO_CLASSIFICADO', 'NAO_CLASSIFICADO')
    """
    unidade_upper = unidade.strip().upper()

    # 1. Check exceptions by unit code
    if unidade_upper in mapa['excecoes']:
        cat_zona = mapa['excecoes'][unidade_upper]
        # Infer regime from categoria_zona
        for regime, cats in escaloes.items():
            if isinstance(cats, dict) and cat_zona in cats:
                return regime, cat_zona
        return 'NAO_CLASSIFICADO', cat_zona

    # 2. Check technology mapping
    if tecnologia in mapa['mapa_tec']:
        regime, categoria_base = mapa['mapa_tec'][tecnologia]
        sufixo = sufixo_de_pais(pais)
        cat_zona = categoria_base + sufixo

        # Verify categoria_zona exists in escaloes
        if regime in escaloes and isinstance(escaloes[regime], dict):
            if cat_zona in escaloes[regime]:
                return regime, cat_zona
            # Try without suffix for special categories
            if categoria_base in escaloes[regime]:
                return regime, categoria_base

    return 'NAO_CLASSIFICADO', 'NAO_CLASSIFICADO'

# ============================================================================
# File Discovery
# ============================================================================

def zip_files_no_intervalo(data_inicio: str, data_fim: str) -> list:
    """
    List ZIP files in /data/bids/ within date range.

    Args:
        data_inicio: Start date (YYYY-MM-DD)
        data_fim: End date (YYYY-MM-DD)

    Returns:
        Sorted list of full paths to matching ZIP files
    """
    inicio = date.fromisoformat(data_inicio)
    fim = date.fromisoformat(data_fim)

    result = []
    patterns = [
        f'{BIDS_DIR}/curva_pbc_uof_*.zip',
        f'{BIDS_DIR}/curva_pbc_uof_*.ZIP',
    ]

    for pattern in patterns:
        for path in glob.glob(pattern):
            nome = os.path.basename(path)
            data_str = extrai_data(nome)

            if data_str == '1970-01-01':
                continue

            d = date.fromisoformat(data_str)
            if inicio <= d <= fim:
                result.append(path)

    return sorted(set(result))


def ensure_output_dir():
    """Ensure /data/outputs directory exists."""
    os.makedirs(OUTPUTS_DIR, exist_ok=True)

# ============================================================================
# Clearing Algorithm Helpers
# ============================================================================

def calcula_clearing(ofertas_compra: list, ofertas_venda: list) -> tuple:
    """
    Simple market clearing calculation.

    Args:
        ofertas_compra: List of (preco, quantidade) sorted by preco DESC
        ofertas_venda: List of (preco, quantidade) sorted by preco ASC

    Returns:
        (preco_clearing, volume_clearing) or (None, None) if no intersection
    """
    if not ofertas_compra or not ofertas_venda:
        return None, None

    # Build cumulative curves
    # Demand: sorted by price DESC, cumulative volume
    demanda = sorted(ofertas_compra, key=lambda x: -x[0])
    cum_demanda = []
    vol_acum = 0
    for preco, vol in demanda:
        vol_acum += vol
        cum_demanda.append((preco, vol_acum))

    # Supply: sorted by price ASC, cumulative volume
    oferta = sorted(ofertas_venda, key=lambda x: x[0])
    cum_oferta = []
    vol_acum = 0
    for preco, vol in oferta:
        vol_acum += vol
        cum_oferta.append((preco, vol_acum))

    # Find intersection
    preco_clearing = None
    volume_clearing = None

    # Iterate through supply curve
    for i, (p_oferta, v_oferta) in enumerate(cum_oferta):
        # Find demand at this price
        v_demanda = 0
        for p_dem, v_dem in cum_demanda:
            if p_dem >= p_oferta:
                v_demanda = v_dem
            else:
                break

        if v_oferta >= v_demanda and v_demanda > 0:
            preco_clearing = p_oferta
            volume_clearing = v_demanda
            break

    return preco_clearing, volume_clearing


def aplica_substituicao_pre(
    bids_venda: list,
    categoria_zona: str,
    config_cat: dict
) -> tuple:
    """
    Apply PRE substitution to sell bids.

    Args:
        bids_venda: List of bid dicts with keys: unidade, energia, precio
        categoria_zona: Category+zone key (e.g., SOLAR_FOT_ES)
        config_cat: Config dict with 'escala' and optionally 'escaloes'

    Returns:
        (bids_modificados, log_substituicoes)
        - bids_modificados: List of modified bids
        - log_substituicoes: List of substitution log entries
    """
    escala = config_cat.get('escala', 1.0)
    escaloes = config_cat.get('escaloes', [])

    if not escaloes:
        # No escaloes - just apply scale to volume
        bids_mod = []
        for bid in bids_venda:
            bid_mod = bid.copy()
            bid_mod['energia'] = bid['energia'] * escala
            bids_mod.append(bid_mod)
        return bids_mod, []

    # Sort escaloes by price
    escaloes_sorted = sorted(escaloes, key=lambda x: x['preco'])

    # Calculate total energy to distribute
    energia_total = sum(b['energia'] for b in bids_venda) * escala

    # Generate new bids based on escaloes
    bids_mod = []
    logs = []

    for esc in escaloes_sorted:
        preco_esc = esc['preco']
        pct = esc['pct_bids']
        energia_esc = energia_total * pct

        if energia_esc > 0:
            # Create aggregated bid at this price level
            bids_mod.append({
                'unidade': f'PRE_AGG_{categoria_zona}',
                'energia': energia_esc,
                'precio': preco_esc,
                'categoria': categoria_zona,
                'tipo_oferta': 'V'
            })

            logs.append({
                'categoria': categoria_zona,
                'escalao_preco': preco_esc,
                'energia_mw': energia_esc
            })

    return bids_mod, logs
