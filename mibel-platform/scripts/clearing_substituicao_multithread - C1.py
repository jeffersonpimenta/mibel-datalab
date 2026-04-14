import pandas as pd
import glob
import zipfile
import os
import re
import threading
from io import StringIO
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from clearing import clearing

# ─────────────────────────────────────────────────────────────────────────────
#  PRINT THREAD-SAFE
# ─────────────────────────────────────────────────────────────────────────────

_print_lock = threading.Lock()

def tprint(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO DE ESCALÕES PRE
#
#  Estrutura por categoria:
#    categoria → dicionário com "escala" e, para PRE, "escaloes".
#
#  Campos comuns a TODAS as classes:
#    "escala"    : float  → factor multiplicativo aplicado ao volume (Energia)
#                           de todos os bids desta categoria.
#                           1.0 = sem alteração; 0.5 = metade do volume.
#
#  Campos exclusivos da classe "PRE":
#    "escaloes"  : lista de escalões, em ordem crescente de preço.
#                  Cada escalão tem:
#                    "preco"    : float → preço fixo a aplicar (€/MWh)
#                    "pct_bids" : float → fracção do VOLUME total (Energia) que
#                                 recebe este preço. Os bids são ordenados por
#                                 Energia ascendente e acumulados até atingir
#                                 pct_bids × volume_total. O último escalão
#                                 absorve o volume restante (garante cobertura
#                                 total independentemente de arredondamentos).
#                                 A soma deve ser 1.0.
#
#  Para as classes PRO, CONSUMO, COMERCIALIZADOR, GENERICA e PORFOLIO
#  apenas a "escala" é usada (não há substituição de preço).
#
#  Exemplo: SOLAR_FOT com escala=0.8 → volume de cada bid multiplicado por
#  0.8; os primeiros 60% do VOLUME recebem 20 €/MWh, os restantes 40% → 35 €/MWh.
#
#  Categorias PRE disponíveis (do classificacao_pre.py):
#    SOLAR_FOT, SOLAR_TER, EOLICA, EOLICA_MARINA, HIDRICA,
#    TERMICA_RENOV, TERMICA_NREN, GEOTERMICA, HIBRIDA_RENOV,
#    RE_TARIFA_CUR, RE_OUTRO
# ─────────────────────────────────────────────────────────────────────────────

ESCALOES = {
    "PRE": {
        "SOLAR_FOT_ES": {
            "escala": 2.2297,
            "escaloes": [
                {"preco": 0.0  , "pct_bids": 0.30},
                {"preco": 20.0  , "pct_bids": 0.30},
                {"preco": 35.0  , "pct_bids": 0.40}
            ],
        },
        "SOLAR_FOT_PT": {
            "escala": 3.6879,
            "escaloes": [
                {"preco": 0.0  , "pct_bids": 0.30},
                {"preco": 20.0  , "pct_bids": 0.30},
                {"preco": 35.0  , "pct_bids": 0.40}
            ],
        },
        "SOLAR_TER_ES": {
            "escala": 2.0869,
            "escaloes": [
                {"preco": 40.0  , "pct_bids": 1.00}
            ],
        },
        "EOLICA_ES": {
            "escala": 1.8857,
            "escaloes": [
                {"preco": 50.0  , "pct_bids": 0.50},
                {"preco": 70.0  , "pct_bids": 0.50}
            ],
        },
        "EOLICA_PT": {
            "escala": 2.1379,
            "escaloes": [
                {"preco": 50.0  , "pct_bids": 0.50},
                {"preco": 70.0  , "pct_bids": 0.50}
            ],
        },
        "EOLICA_MARINA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 50.0  , "pct_bids": 0.50},
                {"preco": 70.0  , "pct_bids": 0.50}
            ],
        },
        "HIDRICA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 15.0  , "pct_bids": 1.00}
            ],
        },
        "TERMICA_RENOV_ES": {
            "escala": 1.4,
            "escaloes": [
                {"preco": 10.0  , "pct_bids": 1.00}
            ],
        },
        "TERMICA_RENOV_PT": {
            "escala": 1.5348,
            "escaloes": [
                {"preco": 10.0  , "pct_bids": 1.00}
            ],
        },
        "TERMICA_NREN_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 60.0  , "pct_bids": 1.00}
            ],
        },
        "GEOTERMICA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 30.0  , "pct_bids": 1.00}
            ],
        },
        "HIBRIDA_RENOV_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 45.0  , "pct_bids": 1.00}
            ],
        },
        "RE_TARIFA_CUR_ES": {
            "escala": 1.7,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "RE_TARIFA_CUR_PT": {
            "escala": 1.7,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "RE_OUTRO_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "ARMAZENAMENTO_VENDA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "ARMAZENAMENTO_VENDA_PT": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
    }, 
    "PRO": {
        "BOMBEO_PURO_PRO_ES": {
            "escala": 1.0,
        },
        "CARVAO_ES": {
            "escala": 1.0,
        },
        "CICLO_COMBINADO_ES": {
            "escala": 1,
            # Exemplo delta_preco: desloca o Precio de todos os bids +10 €/MWh.
            # Remover ou definir 0.0 para desactivar.
            # "delta_preco": 10.0,
        },
        "CICLO_COMBINADO_PT": {
            "escala": 0.7143,
            # Exemplo delta_preco: desloca o Precio de todos os bids +10 €/MWh.
            # Remover ou definir 0.0 para desactivar.
            # "delta_preco": 10.0,
        },
        "GAS_ES": {
            "escala": 1.0,
        },
        "HIDRICA_PRO_ES": {
            "escala": 1.0,
        },
        "HIDRICA_PRO_PT": {
            "escala": 1.0,
        },
        "NUCLEAR_ES": {
            "escala": 0.4470,
        },
    }, 
    "CONSUMO": {
        "ARMAZENAMENTO_COMPRA_ES": {
            "escala": 1.0,
        },
        "ARMAZENAMENTO_COMPRA_PT": {
            "escala": 1.0,
        },
        "BOMBEO_CONSUMO_ES": {
            "escala": 1.6926,
            # "perfil_hora": {
                 # 1: 0.90,  2: 0.90,  3: 0.90,  4: 0.90,  5: 0.90,
                 # 6: 0.90,  7: 0.90,  8: 0.90,  9: 1.10, 10: 1.10,
                # 11: 1.10, 12: 1.10, 13: 1.10, 14: 1.10, 15: 1.10,
                # 16: 1.10, 17: 1.10, 18: 0.90, 19: 0.90, 20: 0.90,
                # 21: 0.90, 22: 0.90, 23: 0.90, 24: 0.90,
            # },
        },
        "BOMBEO_CONSUMO_PT": {
            "escala": 1.3871,
            # "perfil_hora": {
                 # 1: 0.90,  2: 0.90,  3: 0.90,  4: 0.90,  5: 0.90,
                 # 6: 0.90,  7: 0.90,  8: 0.90,  9: 1.10, 10: 1.10,
                # 11: 1.10, 12: 1.10, 13: 1.10, 14: 1.10, 15: 1.10,
                # 16: 1.10, 17: 1.10, 18: 0.90, 19: 0.90, 20: 0.90,
                # 21: 0.90, 22: 0.90, 23: 0.90, 24: 0.90,
            # },
        },
        "CONS_AUXILIARES_ES": {
            "escala": 1.0,
        },
        "CONS_DIRECTO_ES": {
            "escala": 1.6926,
            # "perfil_hora": {
                 # 1: 0.90,  2: 0.90,  3: 0.90,  4: 0.90,  5: 0.90,
                 # 6: 0.90,  7: 0.90,  8: 0.90,  9: 1.10, 10: 1.10,
                # 11: 1.10, 12: 1.10, 13: 1.10, 14: 1.10, 15: 1.10,
                # 16: 1.10, 17: 1.10, 18: 0.90, 19: 0.90, 20: 0.90,
                # 21: 0.90, 22: 0.90, 23: 0.90, 24: 0.90,
            # },
        },
        "CONS_DIRECTO_EXT": {
            "escala": 1.6926,
            # "perfil_hora": {
                 # 1: 0.90,  2: 0.90,  3: 0.90,  4: 0.90,  5: 0.90,
                 # 6: 0.90,  7: 0.90,  8: 0.90,  9: 1.10, 10: 1.10,
                # 11: 1.10, 12: 1.10, 13: 1.10, 14: 1.10, 15: 1.10,
                # 16: 1.10, 17: 1.10, 18: 0.90, 19: 0.90, 20: 0.90,
                # 21: 0.90, 22: 0.90, 23: 0.90, 24: 0.90,
            # },
        },
        "CONS_PRODUTOR_ES": {
            "escala": 1.0,
        },
    },
    "COMERCIALIZADOR": {

        "COMERC_ES": {
            "escala": 1.3871,
            # "perfil_hora": {
                 # 1: 0.90,  2: 0.90,  3: 0.90,  4: 0.90,  5: 0.90,
                 # 6: 0.90,  7: 0.90,  8: 0.90,  9: 1.10, 10: 1.10,
                # 11: 1.10, 12: 1.10, 13: 1.10, 14: 1.10, 15: 1.10,
                # 16: 1.10, 17: 1.10, 18: 0.90, 19: 0.90, 20: 0.90,
                # 21: 0.90, 22: 0.90, 23: 0.90, 24: 0.90,
            # },
        },
        "COMERC_EXT": {
            "escala": 1.0,
        },
        "COMERC_NR_EXT": {
            "escala": 1.0,
        },
        "COMERC_PT": {
            "escala": 1.6926,
            # "perfil_hora": {
                 # 1: 0.90,  2: 0.90,  3: 0.90,  4: 0.90,  5: 0.90,
                 # 6: 0.90,  7: 0.90,  8: 0.90,  9: 1.10, 10: 1.10,
                # 11: 1.10, 12: 1.10, 13: 1.10, 14: 1.10, 15: 1.10,
                # 16: 1.10, 17: 1.10, 18: 0.90, 19: 0.90, 20: 0.90,
                # 21: 0.90, 22: 0.90, 23: 0.90, 24: 0.90,
            # },
        },
        "COMERC_ULT_REC_ES": {
            "escala": 1.0,
        },
        "COMERC_ULT_REC_PT": {
            "escala": 1.0,
        },
    },
    "GENERICA": {
        "GENERICA_ES": {
            "escala": 1.3871,
        },
        "GENERICA_PT": {
            "escala": 1.6926,
        },
        "GENERICA_VENDA_ES": {
            "escala": 1.3871,
        },
        "GENERICA_VENDA_PT": {
            "escala": 1.6926,
        },
    },
    "PORFOLIO": {
        "PORTF_COMERC_ES": {
            "escala": 1.0,
        },
        "PORTF_PROD_ES": {
            "escala": 1.0,
        },
        "PORTF_PROD_PT": {
            "escala": 1.0,
        },
    }, 
}


# ─────────────────────────────────────────────────────────────────────────────
#  CONJUNTO FIXO DE TECNOLOGIAS BASE (derivado de ESCALOES)
#
#  Gerado automaticamente a partir de todas as categorias de ESCALOES,
#  removendo o sufixo de país (_ES, _PT, _EXT). Usado para garantir que
#  as colunas de volume por tecnologia existem sempre no output (valor 0.0
#  quando não há despacho), evitando NaN no DataFrame final.
# ─────────────────────────────────────────────────────────────────────────────
 
TECNOLOGIAS_BASE = sorted({
    re.sub(r"_(ES|PT|EXT)$", "", categoria)
    for categorias_dict in ESCALOES.values()
    for categoria in categorias_dict
})
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  MAPEAMENTO DE COLUNAS DOS FICHEIROS DE BIDS
# ─────────────────────────────────────────────────────────────────────────────
 
MAPA_COLUNAS = {
    'Energía Compra/Venta': 'Energia',
    'Energia Compra/Venta': 'Energia',
    'Potencia Compra/Venta': 'Energia',   # cabeçalho real nos ficheiros CSV
    'Potencia':              'Energia',
    'Energia':               'Energia',
    'Precio Compra/Venta':   'Precio',
    'Precio':                'Precio',
    'Hora':                  'Hora',
    'Periodo':               'Hora',
    'Pais':                  'Pais',
    'Tipo Oferta':           'Tipo Oferta',
    'Unidad':                'Unidad',    # coluna de identificação da unidade
}
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  CARREGAMENTO DAS UNIDADES CLASSIFICADAS
# ─────────────────────────────────────────────────────────────────────────────
 
def carrega_unidades(path_csv: str) -> dict:
    """
    Lê o ficheiro unidades_classificadas.csv e devolve um dicionário com
    TODAS as unidades presentes:
        { CODIGO_upper → (regime, categoria) }
 
    Colunas esperadas no CSV: CODIGO, regime, categoria.
    Unidades cujo regime não esteja em ESCALOES são incluídas no mapa mas
    serão ignoradas em tempo de execução (sem escala nem substituição).
    """
    df = pd.read_csv(path_csv, sep='\t', encoding='utf-8', dtype=str)
 
    # Normalizar nomes de coluna (pode vir com separador ; ou \t)
    if len(df.columns) == 1:
        df = pd.read_csv(path_csv, sep=';', encoding='utf-8', dtype=str)
 
    df.columns = [c.strip() for c in df.columns]
 
    mapa = {}
    for _, row in df.iterrows():
        codigo    = str(row['CODIGO']).strip().upper()
        regime    = str(row['regime']).strip()
        categoria = str(row['categoria']).strip()
        if codigo and categoria not in ('NAN', ''):
            mapa[codigo] = (regime, categoria)
 
    contagem = Counter(regime for regime, _ in mapa.values())
    tprint(f"Unidades carregadas: {len(mapa)}  |  " +
           "  ".join(f"{r}={n}" for r, n in sorted(contagem.items())))
    return mapa
 
 
 
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  CÁLCULO DE VOLUMES DIÁRIOS (para escalonamento horário não uniforme)
# ─────────────────────────────────────────────────────────────────────────────
 
def calcula_volumes_diarios(
    df: pd.DataFrame,
    mapa_unidades: dict,
    escaloes: dict,
) -> dict:
    """
    Pré-calcula o volume total por hora para cada (classe, categoria) que
    tenha "perfil_hora" definido. Necessário para normalizar o factor horário
    de modo a que o crescimento total diário respeite "escala".
 
    Devolve
    ───────
    { (classe, categoria): { hora_int: volume_original_da_hora } }
    """
    volumes_diarios = {}
    unidades_upper = df['Unidad'].astype(str).str.strip().str.upper()
 
    for classe, categorias_dict in escaloes.items():
        for categoria, cfg in categorias_dict.items():
            if "perfil_hora" not in cfg:
                continue
 
            codigos = {
                cod for cod, (reg, cat) in mapa_unidades.items()
                if reg == classe and cat == categoria
            }
            if not codigos:
                continue
 
            mask_cat = unidades_upper.isin(codigos)
            if not mask_cat.any():
                continue
 
            df_cat = df[mask_cat].copy()
            df_cat['_hora_int'] = pd.to_numeric(df_cat['Hora'], errors='coerce')
 
            vol_por_hora = (
                df_cat.groupby('_hora_int')['Energia']
                .sum()
                .to_dict()
            )
            volumes_diarios[(classe, categoria)] = vol_por_hora
 
    return volumes_diarios
 
 
def calcula_factor_horario(
    hora,
    cfg: dict,
    volumes_diarios: dict,
    classe: str,
    categoria: str,
) -> float:
    """
    Calcula o factor efectivo a aplicar na hora `hora` para uma categoria com
    "perfil_hora", garantindo que o crescimento total diário seja igual a
    cfg["escala"].
 
    Derivação:
        volume_total_alvo  = Σ_h V_h × escala
        volume_hora_alvo_h = V_h × perfil_h × k
        k = (Σ_h V_h × escala) / (Σ_h V_h × perfil_h)
        factor_efectivo_h  = perfil_h × k
 
    Fallback para cfg["escala"] quando não há dados diários disponíveis.
    """
    perfil_hora = cfg["perfil_hora"]
    escala      = cfg.get("escala", 1.0)
 
    try:
        hora_int = int(hora)
    except (ValueError, TypeError):
        return escala
 
    vol_por_hora = volumes_diarios.get((classe, categoria), {})
    if not vol_por_hora:
        return escala
 
    soma_ponderada = sum(
        vol_por_hora.get(h, 0.0) * perfil_hora.get(h, 1.0)
        for h in vol_por_hora
    )
    if soma_ponderada == 0:
        return escala
 
    volume_total_orig = sum(vol_por_hora.values())
    k = (volume_total_orig * escala) / soma_ponderada
 
    return perfil_hora.get(hora_int, 1.0) * k
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  APLICAÇÃO DE ESCALA E ESCALÕES A TODAS AS CLASSES
# ─────────────────────────────────────────────────────────────────────────────
 
def aplica_escalao(
    df: pd.DataFrame,
    mapa_unidades: dict,
    escaloes: dict,
    internal_file: str = "",
    Hora: str = "",
    pais: str = "",
    volumes_diarios: dict | None = None,
) -> tuple[pd.DataFrame, list]:
    """
    Função unificada que aplica, para TODAS as classes (PRE, PRO, CONSUMO,
    COMERCIALIZADOR, GENERICA, PORFOLIO):
 
      • "escala"      — factor multiplicativo uniforme ao volume (Energia).
                        Se ausente, o volume não é modificado.
 
      • "perfil_hora" — (opcional) distribuição horária não uniforme.
                        Quando presente, o factor efectivo para a hora `Hora`
                        é calculado por calcula_factor_horario(), garantindo
                        que o crescimento total diário seja igual a "escala".
                        Se ausente, aplica "escala" de forma uniforme.
                        Requer volumes_diarios pré-calculado.
 
      • "escaloes"    — (exclusivo PRE) substitui o Precio dos bids com
                        Precio ≈ 0.0 conforme os escalões configurados.
                        Se ausente, o Precio não é modificado.
 
    Regra geral: se o parâmetro não existir no mapa, o bid não é alterado.
 
    Parâmetros
    ──────────
    df               : DataFrame do período/país (compras ou vendas).
    mapa_unidades    : { CODIGO_upper → (regime, categoria) } — mapa completo.
    escaloes         : dicionário ESCALOES completo.
    internal_file    : nome do ficheiro interno (para log; opcional).
    Hora             : período/hora actual (para log e perfil horário).
    pais             : país (para log; opcional).
    volumes_diarios  : saída de calcula_volumes_diarios(); necessário para
                       aplicar "perfil_hora". Se None, usa "escala" uniforme.
 
    Devolve
    ───────
    (df_modificado, lista_de_logs)
 
    Cada entrada do log corresponde a um bid cujo Precio foi substituído
    por um escalão, e contém os detalhes da substituição.
    """
    df   = df.copy()
    logs = []
 
    unidades_upper = df['Unidad'].astype(str).str.strip().str.upper()
 
    for classe, categorias_dict in escaloes.items():
        for categoria, cfg in categorias_dict.items():
 
            # Codigos desta (classe, categoria)
            codigos = {
                cod for cod, (reg, cat) in mapa_unidades.items()
                if reg == classe and cat == categoria
            }
            if not codigos:
                continue
 
            mask_cat = unidades_upper.isin(codigos)
            if not mask_cat.any():
                continue
 
            # ── 1. Aplicar escala ao volume (se presente) ─────────────────
            if "escala" in cfg:
                # Se a categoria tem perfil horário e temos os volumes diários,
                # calcula factor efectivo para esta hora; caso contrário usa
                # a escala uniforme como fallback.
                if "perfil_hora" in cfg and volumes_diarios is not None:
                    factor = calcula_factor_horario(
                        Hora, cfg, volumes_diarios, classe, categoria
                    )
                else:
                    factor = cfg["escala"]
 
                if factor != 1.0:
                    df.loc[mask_cat, 'Energia'] = df.loc[mask_cat, 'Energia'] * factor
                    unidades_upper = df['Unidad'].astype(str).str.strip().str.upper()
 
            # ── 2. Aplicar escalões ao preço (se presente) ────────────────
            if "escaloes" in cfg:
                escalonamento = cfg["escaloes"]
 
                mask_zero = mask_cat & df['Precio'].between(-0.001, 0.001)
                df_zero   = df[mask_zero]
 
                if df_zero.empty:
                    continue
 
                vol_total = df_zero['Energia'].sum()
                vol_acum  = 0.0
                esc_idx   = 0
 
                # Pré-calcular limiares de volume (MWh) para cada escalão.
                # O último escalão não tem limiar — absorve o volume restante.
                limiares = []
                acum_lim = 0.0
                for esc in escalonamento[:-1]:
                    acum_lim += esc["pct_bids"] * vol_total
                    limiares.append(acum_lim)
                limiares.append(float('inf'))
 
                for idx in df_zero.index:
                    energia_bid = df.at[idx, 'Energia']
                    vol_acum   += energia_bid
 
                    # Avançar para o próximo escalão se o volume acumulado
                    # ultrapassou o limiar corrente.
                    while esc_idx < len(limiares) - 1 and vol_acum > limiares[esc_idx] + 1e-9:
                        esc_idx += 1
 
                    escalao    = escalonamento[esc_idx]
                    preco_novo = escalao["preco"]
 
                    # Escalão com preco == 0 → bid não é modificado
                    if preco_novo == 0:
                        continue
 
                    preco_original       = df.at[idx, 'Precio']
                    df.at[idx, 'Precio'] = preco_novo
 
                    logs.append({
                        'data_ficheiro':  internal_file,
                        'Hora':           Hora,
                        'pais':           pais,
                        'Unidad':         df.at[idx, 'Unidad'],
                        'classe':         classe,
                        'categoria':      categoria,
                        'escalao_preco':  preco_novo,
                        'pct_escalao':    escalao["pct_bids"],
                        'preco_original': preco_original,
                        'Energia_MW':     energia_bid,
                        'delta_preco':    None,
                    })
 
            # ── 3. Aplicar delta_preco ao preço (se presente) ────────────────
            if "delta_preco" in cfg:
                delta = cfg["delta_preco"]
                if delta != 0.0:
                    df.loc[mask_cat, 'Precio'] = df.loc[mask_cat, 'Precio'] + delta            
 
    return df, logs
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  WORKERS (unidade mínima de trabalho paralelizável)
# ─────────────────────────────────────────────────────────────────────────────
 
def _vol_por_tecnologia(
    vendas_df:     pd.DataFrame,
    preco_clearing,
    mapa_unidades: dict,
) -> dict:
    """
    Calcula o volume despachado (aceite no clearing) por tecnologia base,
    para um único par (Hora, Pais) e um cenário de clearing.
 
    Um bid de venda é considerado aceite quando Precio <= preco_clearing.
    A tecnologia base é obtida removendo o sufixo de país (_ES, _PT, _EXT)
    da categoria registada em mapa_unidades.
 
    Todas as tecnologias de TECNOLOGIAS_BASE são sempre devolvidas,
    com valor 0.0 quando não há despacho — garante ausência de NaN
    no DataFrame final.
 
    Parâmetros
    ──────────
    vendas_df      : DataFrame de vendas já ordenado (cenário orig ou sub).
    preco_clearing : preço de equilíbrio calculado pelo clearing(); None se
                     o clearing não convergiu.
    mapa_unidades  : { CODIGO_upper → (regime, categoria) }
 
    Devolve
    ───────
    { tecnologia_base: volume_MWh }  — todas as chaves de TECNOLOGIAS_BASE
    presentes, valor 0.0 por omissão.
    """
    resultado = {tec: 0.0 for tec in TECNOLOGIAS_BASE}
 
    if preco_clearing is None or vendas_df.empty:
        return resultado
 
    aceites = vendas_df[vendas_df["Precio"] <= preco_clearing]
 
    for _, bid in aceites.iterrows():
        cod = str(bid["Unidad"]).strip().upper()
        if cod not in mapa_unidades:
            continue
        _, categoria = mapa_unidades[cod]
        tec = re.sub(r"_(ES|PT|EXT)$", "", categoria)
        if tec in resultado:
            resultado[tec] += bid["Energia"]
 
    return resultado
 
 
def _processa_hora_pais(
    df:              pd.DataFrame,
    internal_file:   str,
    Hora:            str,
    pais:            str,
    mapa_unidades:   dict,
    escaloes:        dict,
    volumes_diarios: dict,
) -> tuple[dict | None, list]:
    """
    Nível 3 — calcula os dois clearings (original + substituição PRE)
    para um único par (Hora, Pais). Função pura e thread-safe.
    """
    compras = df[
        (df["Hora"] == Hora) & (df["Tipo Oferta"] == "C") & (df["Pais"] == pais)
    ].copy()
    vendas = df[
        (df["Hora"] == Hora) & (df["Tipo Oferta"] == "V") & (df["Pais"] == pais)
    ].copy()
 
    if compras.empty or vendas.empty:
        tprint(f"  [VAZIO] {internal_file} | {Hora} | {pais}")
        return None, []
 
    # ── Clearing ORIGINAL ────────────────────────────────────────────────────
    compras_orig = compras.sort_values("Precio", ascending=False).reset_index(drop=True)
    vendas_orig  = vendas.sort_values("Precio",  ascending=True).reset_index(drop=True)
    compras_orig["Volume_Acumulado"] = compras_orig["Energia"].cumsum()
    vendas_orig["Volume_Acumulado"]  = vendas_orig["Energia"].cumsum()
 
    preco_orig, volume_orig = clearing(compras_df=compras_orig, vendas_df=vendas_orig)
    vols_orig = _vol_por_tecnologia(vendas_orig, preco_orig, mapa_unidades)
 
    # ── Escala + escalões: aplica a compras e vendas ────────────────────────
    # Nota: escalões de preço (PRE) fazem sentido apenas na curva de oferta,
    # mas aplica_escalao é segura para compras (sem bids com Precio ≈ 0).
    compras_scaled, _         = aplica_escalao(
        compras, mapa_unidades, escaloes,
        Hora=Hora,
        volumes_diarios=volumes_diarios,
    )
    vendas_sub, logs_este     = aplica_escalao(
        vendas, mapa_unidades, escaloes,
        internal_file=internal_file, Hora=Hora, pais=pais,
        volumes_diarios=volumes_diarios,
    )
 
    # ── Clearing COM SUBSTITUIÇÃO ────────────────────────────────────────────
    compras_sub = compras_scaled.sort_values("Precio", ascending=False).reset_index(drop=True)
    vendas_sub  = vendas_sub.sort_values("Precio", ascending=True).reset_index(drop=True)
    compras_sub["Volume_Acumulado"] = compras_sub["Energia"].cumsum()
    vendas_sub["Volume_Acumulado"]  = vendas_sub["Energia"].cumsum()
 
    preco_sub, volume_sub = clearing(compras_df=compras_sub, vendas_df=vendas_sub)
    vols_sub = _vol_por_tecnologia(vendas_sub, preco_sub, mapa_unidades)
 
    if preco_orig and preco_sub:
        tprint(f"  [OK] {internal_file} | {Hora} | {pais} "
               f"| orig={preco_orig} | sub={preco_sub} "
               f"| dif={((preco_sub / preco_orig) - 1) * 100:.2f}%")
    else:
        tprint(f"  [OK] {internal_file} | {Hora} | {pais} "
               f"| orig={preco_orig} | sub={preco_sub}")
 
    row = {
        'data_ficheiro':        internal_file,
        'Hora':                 Hora,
        'pais':                 pais,
        # ── Clearing original ─────────────────────────────────────────────
        'preco_clearing_orig':  preco_orig,
        'volume_clearing_orig': volume_orig,
        # ── Clearing com substituição PRE ─────────────────────────────────
        'preco_clearing_sub':   preco_sub,
        'volume_clearing_sub':  volume_sub,
        # ── Delta ─────────────────────────────────────────────────────────
        'delta_preco': (
            (preco_sub - preco_orig)
            if preco_sub is not None and preco_orig is not None
            else None
        ),
        # ── Contagem de bids substituídos ─────────────────────────────────
        'n_bids_substituidos': len(logs_este),
        # ── Volume despachado por tecnologia — cenário original ───────────
        **{f"vol_orig_{tec}": v for tec, v in vols_orig.items()},
        # ── Volume despachado por tecnologia — cenário com substituição ───
        **{f"vol_sub_{tec}":  v for tec, v in vols_sub.items()},
    }
    return row, logs_este
 
 
def _processa_internal_file(
    zip_path:          str,
    internal_file:     str,
    mapa_unidades:     dict,
    escaloes:          dict,
    workers_hora_pais: int,
) -> tuple[list, list]:
    """
    Nível 2 — lê e parseia um ficheiro CSV dentro de um ZIP e paraleliza
    o clearing por (Hora, Pais) com um sub-pool de threads.
    """
    tprint(f"  [CSV] {os.path.basename(zip_path)} → {internal_file}")
 
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open(internal_file) as f:
                content = f.read().decode('latin-1')
 
        df = pd.read_csv(StringIO(content), sep=";", dtype=str, skiprows=2)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns=MAPA_COLUNAS)
        df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
 
        df["Energia"] = (
            df["Energia"]
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .astype(float)
        )
        df["Precio"] = (
            df["Precio"]
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .astype(float)
        )
 
    except Exception as e:
        tprint(f"  [ERRO leitura] {internal_file}: {e}")
        return [], []
 
    # Pré-calcula volumes diários para escalonamento horário não uniforme.
    # Feito aqui (uma vez por ficheiro) e não dentro de cada thread hora/pais
    # para evitar cálculos redundantes e condições de corrida.
    volumes_diarios = calcula_volumes_diarios(df, mapa_unidades, escaloes)
 
    # Combinações únicas de (Hora, Pais)
    combinacoes = [
        (Hora, pais)
        for Hora in sorted(df['Hora'].unique())
        for pais in sorted(df[df['Hora'] == Hora]['Pais'].unique())
    ]
 
    rows = []
    logs = []
 
    # Nível 3 — paralelizar por (Hora, Pais)
    with ThreadPoolExecutor(max_workers=workers_hora_pais) as ex:
        futures = {
            ex.submit(
                _processa_hora_pais,
                df, internal_file, Hora, pais, mapa_unidades, escaloes,
                volumes_diarios
            ): (Hora, pais)
            for Hora, pais in combinacoes
        }
        for fut in as_completed(futures):
            Hora, pais = futures[fut]
            try:
                row, log_este = fut.result()
                if row is not None:
                    rows.append(row)
                logs.extend(log_este)
            except Exception as e:
                tprint(f"  [ERRO clearing] {internal_file} | {Hora} | {pais}: {e}")
 
    return rows, logs
 
 
def _processa_zip(
    zip_path:          str,
    mapa_unidades:     dict,
    escaloes:          dict,
    workers_interno:   int,
    workers_hora_pais: int,
) -> tuple[list, list]:
    """
    Nível 1 — abre um ZIP e paraleliza o processamento dos seus ficheiros
    CSV internos.
    """
    tprint(f"\n[ZIP] {os.path.basename(zip_path)}")
 
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            internal_files = z.namelist()
    except Exception as e:
        tprint(f"[ERRO ZIP] {zip_path}: {e}")
        return [], []
 
    rows_zip = []
    logs_zip = []
 
    # Nível 2 — paralelizar ficheiros internos do ZIP
    with ThreadPoolExecutor(max_workers=workers_interno) as ex:
        futures = {
            ex.submit(
                _processa_internal_file,
                zip_path, ifile, mapa_unidades, escaloes, workers_hora_pais
            ): ifile
            for ifile in internal_files
        }
        for fut in as_completed(futures):
            ifile = futures[fut]
            try:
                rows, logs = fut.result()
                rows_zip.extend(rows)
                logs_zip.extend(logs)
            except Exception as e:
                tprint(f"  [ERRO interno] {ifile}: {e}")
 
    tprint(f"[FIM ZIP] {os.path.basename(zip_path)} → {len(rows_zip)} registos")
    return rows_zip, logs_zip
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  PROCESSAMENTO PRINCIPAL COM MULTITHREADING
# ─────────────────────────────────────────────────────────────────────────────
 
def processa_datasets(
    folder_path:       str,
    mapa_unidades:     dict,
    escaloes:          dict = ESCALOES,
    workers_zip:       int  = 4,   # threads paralelas para ZIPs distintos
    workers_interno:   int  = 2,   # threads por ZIP para ficheiros internos
    workers_hora_pais: int  = 4,   # threads por ficheiro para pares (Hora, Pais)
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processa todos os ZIPs em folder_path com multithreading em 3 níveis:
      Nível 1 – ZIPs em paralelo           (workers_zip)
      Nível 2 – ficheiros internos por ZIP  (workers_interno)
      Nível 3 – pares (Hora, Pais)/ficheiro (workers_hora_pais)
 
    Parâmetros
    ──────────
    folder_path       : pasta com os ficheiros curva_pbc_uof_*.zip
    mapa_unidades     : saída de carrega_unidades() — { CODIGO_upper → (regime, categoria) }
    escaloes          : dicionário ESCALOES (pode ser substituído por outro)
    workers_zip       : threads paralelas para ZIPs distintos
    workers_interno   : threads por ZIP para ficheiros CSV internos
    workers_hora_pais : threads por ficheiro para pares (Hora, Pais)
 
    Devolve (df_resultado, df_logs).
    """
    zip_files = glob.glob(os.path.join(folder_path, "curva_pbc_uof_*.zip"))
 
    if not zip_files:
        tprint("Nenhum ficheiro ZIP encontrado.")
        return pd.DataFrame(), pd.DataFrame()
 
    tprint(f"ZIPs encontrados: {len(zip_files)}")
 
    todos_rows = []
    todos_logs = []
 
    # Nível 1 — paralelizar ZIPs
    with ThreadPoolExecutor(max_workers=workers_zip) as ex:
        futures = {
            ex.submit(
                _processa_zip,
                zp, mapa_unidades, escaloes, workers_interno, workers_hora_pais
            ): zp
            for zp in zip_files
        }
        for fut in as_completed(futures):
            zp = futures[fut]
            try:
                rows, logs = fut.result()
                todos_rows.extend(rows)
                todos_logs.extend(logs)
            except Exception as e:
                tprint(f"[ERRO ZIP NIVEL1] {zp}: {e}")
 
    # Ordenação determinística (independente da ordem de chegada dos threads)
    df_resultado = pd.DataFrame()
    df_logs      = pd.DataFrame()
 
    if todos_rows:
        df_resultado = (
            pd.DataFrame(todos_rows)
            .sort_values(['data_ficheiro', 'Hora', 'pais'])
            .reset_index(drop=True)
        )
 
    if todos_logs:
        df_logs = (
            pd.DataFrame(todos_logs)
            .sort_values(['data_ficheiro', 'Hora', 'pais', 'Unidad'])
            .reset_index(drop=True)
        )
 
    return df_resultado, df_logs
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  GUARDAR RESULTADOS
# ─────────────────────────────────────────────────────────────────────────────
 
def guarda_resultados(df: pd.DataFrame, nome_arquivo: str) -> None:
    if df is None or df.empty:
        tprint(f"Aviso: DataFrame vazio. '{nome_arquivo}' não será guardado.")
        return
    df.to_csv(nome_arquivo, sep=';', index=False, encoding='utf-8-sig', decimal=',')
    tprint(f"Guardado: {os.path.abspath(nome_arquivo)}  ({len(df)} registos)")
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
 
if __name__ == "__main__":
    import time
 
    t0 = time.perf_counter()
 
    # 1. Carregar todas as unidades classificadas
    mapa_unidades = carrega_unidades('./unidades/unidades_classificadas.csv')
 
    # 2. Processar bids e calcular clearing original + com substituição
    df_resultado, df_logs = processa_datasets(
        folder_path       = './bids',
        mapa_unidades     = mapa_unidades,
        escaloes          = ESCALOES,
        workers_zip       = 4,   # ajustar ao nº de CPUs disponíveis
        workers_interno   = 4,
        workers_hora_pais = 4,
    )
 
    elapsed = time.perf_counter() - t0
    tprint(f"\nTempo total: {elapsed:.1f}s")
 
    # 3. Guardar resultados
    guarda_resultados(df_resultado, 'historico_clearing_comparado-c1.csv')
    guarda_resultados(df_logs,      'log_substituicoes_pre-c1.csv')
 
    # 4. Resumo
    if not df_resultado.empty:
        tprint("\n── Resumo ──────────────────────────────────────")
        tprint(f"  Períodos processados:      {len(df_resultado)}")
        tprint(f"  Bids PRE substituídos:     {len(df_logs) if not df_logs.empty else 0}")
        if 'delta_preco' in df_resultado.columns:
            delta = df_resultado['delta_preco'].dropna()
            tprint(f"  Delta preço médio:         {delta.mean():.2f} €/MWh")
            tprint(f"  Delta preço máx:           {delta.max():.2f} €/MWh")
            tprint(f"  Delta preço mín:           {delta.min():.2f} €/MWh")
        tprint("────────────────────────────────────────────────")