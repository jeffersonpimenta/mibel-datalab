import pandas as pd
import numpy as np
import glob
import zipfile
import os
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
#                                 recebe este preço, na ordem original do ficheiro.
#                                 O volume acumula bid a bid até atingir
#                                 pct_bids × volume_total; o último escalão
#                                 absorve o volume restante.
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
            # "escaloes": [
                # {"preco": 0.0  , "pct_bids": 0.30},
                # {"preco": 20.0  , "pct_bids": 0.30},
                # {"preco": 35.0  , "pct_bids": 0.40}
            # ],
        },
        "SOLAR_FOT_PT": {
            "escala": 3.6879,
            # "escaloes": [
                # {"preco": 0.0  , "pct_bids": 0.30},
                # {"preco": 20.0  , "pct_bids": 0.30},
                # {"preco": 35.0  , "pct_bids": 0.40}
            # ],
        },
        "SOLAR_TER_ES": {
            "escala": 2.0869,
            # "escaloes": [
                # {"preco": 40.0  , "pct_bids": 1.00}
            # ],
        },
        "EOLICA_ES": {
            "escala": 1.8857,
            # "escaloes": [
                # {"preco": 50.0  , "pct_bids": 0.50},
                # {"preco": 70.0  , "pct_bids": 0.50}
            # ],
        },
        "EOLICA_PT": {
            "escala": 2.1379,
            # "escaloes": [
                # {"preco": 50.0  , "pct_bids": 0.50},
                # {"preco": 70.0  , "pct_bids": 0.50}
            # ],
        },
        "EOLICA_MARINA_ES": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 50.0  , "pct_bids": 0.50},
                # {"preco": 70.0  , "pct_bids": 0.50}
            # ],
        },
        "HIDRICA_ES": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 15.0  , "pct_bids": 1.00}
            # ],
        },
        "TERMICA_RENOV_ES": {
            "escala": 1.4,
            # "escaloes": [
                # {"preco": 10.0  , "pct_bids": 1.00}
            # ],
        },
        "TERMICA_RENOV_PT": {
            "escala": 1.5348,
            # "escaloes": [
                # {"preco": 10.0  , "pct_bids": 1.00}
            # ],
        },
        "TERMICA_NREN_ES": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 60.0  , "pct_bids": 1.00}
            # ],
        },
        "GEOTERMICA_ES": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 30.0  , "pct_bids": 1.00}
            # ],
        },
        "HIBRIDA_RENOV_ES": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 45.0  , "pct_bids": 1.00}
            # ],
        },
        "RE_TARIFA_CUR_ES": {
            "escala": 1.7,
            # "escaloes": [
                # {"preco": 0.0   , "pct_bids": 1.00}
            # ],
        },
        "RE_TARIFA_CUR_PT": {
            "escala": 1.7,
            # "escaloes": [
                # {"preco": 0.0   , "pct_bids": 1.00}
            # ],
        },
        "RE_OUTRO_ES": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 0.0   , "pct_bids": 1.00}
            # ],
        },
        "ARMAZENAMENTO_VENDA_ES": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 0.0   , "pct_bids": 1.00}
            # ],
        },
        "ARMAZENAMENTO_VENDA_PT": {
            "escala": 1.0,
            # "escaloes": [
                # {"preco": 0.0   , "pct_bids": 1.00}
            # ],
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

PRECO_REMOCAO = 5_000.0   # €/MWh — desloca um bid PRE para o fim da curva de venda


# ─────────────────────────────────────────────────────────────────────────────
#  CLEARING ANALÍTICO (substitui chamadas a clearing() no loop de optimização)
# ─────────────────────────────────────────────────────────────────────────────

def _build_step_arrays(
    compras_s: pd.DataFrame,
    vendas_s:  pd.DataFrame,
) -> tuple:
    """
    Comprime as curvas de compra e venda em tabelas de passos (step tables):
    um registo por preço único, com a energia total desse preço e o volume
    acumulado até esse passo.

    Devolve seis arrays numpy:
      cp   — preços de compra (DESC)
      cv   — volumes acumulados de compra
      vp   — preços de venda  (ASC)
      ve   — energia por passo de venda
      vv   — volumes acumulados de venda (sem qualquer remoção)
      j_shift — índice a partir do qual o shift de volume PRE se aplica
                (primeiro passo com Precio >= -0.001)
    """
    c_df = (
        compras_s.groupby("Precio", sort=False)["Energia"].sum()
        .reset_index()
        .sort_values("Precio", ascending=False)
        .reset_index(drop=True)
    )
    c_df["V_acum"] = c_df["Energia"].cumsum()

    v_df = (
        vendas_s.groupby("Precio", sort=True)["Energia"].sum()
        .reset_index()
        .sort_values("Precio", ascending=True)
        .reset_index(drop=True)
    )
    v_df["V_acum"] = v_df["Energia"].cumsum()

    cp = c_df["Precio"].to_numpy(dtype=float)
    cv = c_df["V_acum"].to_numpy(dtype=float)
    vp = v_df["Precio"].to_numpy(dtype=float)
    ve = v_df["Energia"].to_numpy(dtype=float)
    vv = v_df["V_acum"].to_numpy(dtype=float)

    # Primeiro índice da curva de venda com Precio >= -0.001
    # A partir daqui o volume acumulado é deslocado para a esquerda por vol_rem.
    j_shift = int(np.searchsorted(vp, -0.001 - 1e-9, side="right"))

    return cp, cv, vp, ve, vv, j_shift


def _clearing_analitico(
    cp:      np.ndarray,
    cv:      np.ndarray,
    vp:      np.ndarray,
    ve:      np.ndarray,
    vv:      np.ndarray,
    j_shift: int,
    vol_rem: float = 0.0,
) -> tuple[float | None, float | None]:
    """
    Clearing analítico sobre step tables (arrays numpy).

    Replica exactamente o algoritmo de dois ponteiros de clearing.py mas
    opera sobre tabelas de passos comprimidas (um registo por preço único)
    em vez de sobre o DataFrame completo de bids individuais.

    Complexidade: O(n_preços_únicos) em vez de O(n_bids).
    Com vol_rem=0 reproduz o clearing base; com vol_rem>0 simula a remoção
    de vol_rem MWh de bids PRE (Precio≈0) sem reconstruir nenhum DataFrame.

    Efeito de vol_rem na curva de venda
    ────────────────────────────────────
    Remover bids de preço zero desloca horizontalmente para a esquerda toda a
    porção da curva de venda a partir de Precio >= -0.001:

      vv_efectivo[j] = vv[j] − vol_rem    se j >= j_shift
      vv_efectivo[j] = vv[j]              se j <  j_shift  (bids negativos, inalterados)

    O offset é aplicado inline com aritmética escalar — sem copiar arrays.

    Regras de preço (idênticas a clearing.py)
    ──────────────────────────────────────────
      vc == vv  →  preço = (pc + pv) / 2 ;  vol = vc
      vc >  vv  →  preço = pc             ;  vol = vv   (curva de compra avançou mais)
      vc <  vv  →  preço = pv             ;  vol = vc   (curva de venda avançou mais)
                   (com tratamento de aresta para last_j == 0)
    """
    n_c, n_v = len(cp), len(vp)
    i = j = 0
    last_i = last_j = -1

    while i < n_c and j < n_v:
        pc = round(cp[i], 2)
        pv = round(vp[j], 2)

        if pc < pv:
            break

        last_i, last_j = i, j

        vc    = cv[i]
        vv_ef = vv[j] - (vol_rem if j >= j_shift else 0.0)

        vc_r = round(vc,    2)
        vv_r = round(vv_ef, 2)
        if   vc_r < vv_r: i += 1
        elif vc_r > vv_r: j += 1
        else:             i += 1; j += 1

    if last_i < 0:
        return None, None

    pc_last = cp[last_i]
    pv_last = vp[last_j]
    vc_last = cv[last_i]
    vv_last = vv[last_j] - (vol_rem if last_j >= j_shift else 0.0)

    if   round(vc_last, 2) == round(vv_last, 2):
        return round((pc_last + pv_last) / 2.0, 2), vc_last
    elif round(vc_last, 2) >  round(vv_last, 2):
        return pc_last, vv_last
    elif last_j > 0:
        return pv_last, vc_last
    else:
        i_next = last_i + 1
        if (i_next < n_c
                and round(cp[i_next], 2) < round(pv_last, 2)):
            return pv_last, vc_last
        return pc_last, vc_last


def _identifica_codigos_pre(mapa_unidades: dict, escaloes: dict) -> set:
    """Devolve o conjunto de códigos (upper) classificados como regime PRE."""
    categorias_pre = set(escaloes.get("PRE", {}).keys())
    return {
        cod for cod, (reg, cat) in mapa_unidades.items()
        if reg == "PRE" and cat in categorias_pre
    }


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
    Nível 3 — clearing original + optimização analítica do lucro PRE,
    para um único par (Hora, Pais). Função pura e thread-safe.

    Algoritmo
    ─────────
    1. Aplica a escala de volumes (aplica_escalao) a compras e vendas.
    2. Comprime as curvas escaladas em step tables (um registo por preço único)
       e corre o clearing analítico base — O(n_preços_únicos).
    3. Identifica os bids PRE com Precio≈0 e ordena-os por Energia ASC
       (candidatos à remoção, menores primeiro).
    4. Para cada escalão de venda acima do clearing base:
         a. Calcula analiticamente o volume mínimo de bids PRE a remover
            para que a intersecção salte para esse escalão:
              vol_min = max(0, sell_vol(P⁻) − buy_vol(P))
         b. Acumula bids PRE (menores primeiro) até cobrir vol_min.
         c. Corre o clearing analítico com vol_rem como offset escalar
            sobre os volumes acumulados da curva de venda — sem reconstruir
            nenhum DataFrame nem array.
         d. Calcula o lucro PRE = vol_PRE_despachado × clearing_price.
    5. Regista o cenário de lucro máximo.

    O lucro PRE é calculado exclusivamente sobre bids de venda com Precio≈0
    pertencentes a unidades classificadas como PRE, que ficaram despachados
    (Volume_Acumulado ≤ volume de clearing após a remoção).

    Complexidade por hora/país: O(n_escalões × n_preços_únicos)
    em vez de O(n_escalões × n_bids) com clearing() completo.
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

    # ── Clearing ORIGINAL (sem escala, chama clearing() uma única vez) ───────
    compras_orig = compras.sort_values("Precio", ascending=False).reset_index(drop=True)
    vendas_orig  = vendas.sort_values("Precio",  ascending=True).reset_index(drop=True)
    compras_orig["Volume_Acumulado"] = compras_orig["Energia"].cumsum()
    vendas_orig["Volume_Acumulado"]  = vendas_orig["Energia"].cumsum()

    preco_orig, volume_orig = clearing(compras_df=compras_orig, vendas_df=vendas_orig)

    # ── Aplicar escala de volumes (sem modificar preços) ─────────────────────
    compras_scaled, _ = aplica_escalao(compras, mapa_unidades, escaloes,
                                    Hora=Hora, volumes_diarios=volumes_diarios)
    vendas_scaled, _  = aplica_escalao(vendas,  mapa_unidades, escaloes,
                                    Hora=Hora, volumes_diarios=volumes_diarios)

    compras_s = compras_scaled.sort_values("Precio", ascending=False).reset_index(drop=True)
    vendas_s  = vendas_scaled.sort_values("Precio",  ascending=True).reset_index(drop=True)
    compras_s["Volume_Acumulado"] = compras_s["Energia"].cumsum()
    vendas_s["Volume_Acumulado"]  = vendas_s["Energia"].cumsum()

    # ── Construir step tables e clearing analítico BASE ───────────────────────
    # A partir daqui, clearing() nunca mais é chamado no loop de optimização.
    cp, cv, vp, ve, vv, j_shift = _build_step_arrays(compras_s, vendas_s)

    preco_base, volume_base = _clearing_analitico(cp, cv, vp, ve, vv, j_shift, vol_rem=0.0)

    if preco_base is None:
        tprint(f"  [FALHA] {internal_file} | {Hora} | {pais} | clearing base falhou")
        return None, []

    # ── Identificar bids PRE com Precio ≈ 0 ──────────────────────────────────
    codigos_pre = _identifica_codigos_pre(mapa_unidades, escaloes)
    unids_upper = vendas_s["Unidad"].astype(str).str.strip().str.upper()
    mask_pre_zero = unids_upper.isin(codigos_pre) & vendas_s["Precio"].between(-0.001, 0.001)

    # ── Lucro PRE analítico ───────────────────────────────────────────────────
    # Os arrays de bids PRE são extraídos já ordenados por Energia ASC —
    # a mesma ordem em que os bids são acumulados para remoção (pre_candidatos).
    # Isto permite usar o índice n_bids_acum como fronteira directa:
    #   bids [0 .. n_bids_acum-1]  → removidos (Precio deslocado para PRECO_REMOCAO)
    #   bids [n_bids_acum .. N-1]  → presentes na curva de venda
    #
    # Um bid presente k está despachado se o seu V_acum efectivo ≤ vol_clearing:
    #   V_acum_eff[k] = V_acum_orig[k] − vol_rem  ≤  vol_clearing + ε
    #   ⟺  V_acum_orig[k]  ≤  vol_clearing + vol_rem + ε
    #
    # Os bids removidos NÃO entram no cálculo independentemente da condição
    # de volume — estão a Precio=5000 e nunca são despachados.

    pre_candidatos = (
        vendas_s[mask_pre_zero]
        .sort_values("Energia", ascending=True)
        .reset_index(drop=True)
    )
    # Arrays indexados na mesma ordem de remoção (Energia ASC)
    pre_vacum_ord  = pre_candidatos["Volume_Acumulado"].values
    pre_energy_ord = pre_candidatos["Energia"].values
    pre_unidad_ord = pre_candidatos["Unidad"].values

    def _lucro_pre_analitico(vol_clearing: float, vol_rem: float,
                              preco_clearing: float, n_rem: int) -> float:
        """
        Calcula o lucro PRE sem reconstruir nenhum DataFrame.

        Parâmetros
        ──────────
        vol_clearing  : volume de clearing do cenário actual
        vol_rem       : volume total de bids PRE removidos (deslocados para 5000)
        preco_clearing: preço de clearing do cenário actual
        n_rem         : número de bids PRE removidos (os primeiros n_rem de
                        pre_candidatos, ordenado por Energia ASC)

        Lógica
        ──────
        Apenas os bids a partir do índice n_rem são considerados presentes.
        De entre esses, estão despachados os que têm:
          V_acum_orig[k] ≤ vol_clearing + vol_rem + ε
        (equivalente a V_acum_eff[k] = V_acum_orig[k] − vol_rem ≤ vol_clearing)
        """
        # Bids presentes: índice n_rem em diante
        vacum_presentes  = pre_vacum_ord[n_rem:]
        energy_presentes = pre_energy_ord[n_rem:]
        desp_mask = vacum_presentes <= vol_clearing + vol_rem + 1e-6
        return float(energy_presentes[desp_mask].sum()) * preco_clearing

    # ── Estado inicial ────────────────────────────────────────────────────────
    vol_pre_total   = pre_candidatos["Energia"].sum()
    energias_pre    = pre_energy_ord            # alias — mesma array
    n_bids_acum     = 0   # quantos bids PRE já acumulados para remoção
    vol_rem_acum    = 0.0

    lucro_base    = _lucro_pre_analitico(volume_base, 0.0, preco_base, n_rem=0)
    lucro_melhor  = lucro_base
    preco_melhor  = preco_base
    volume_melhor = volume_base
    vol_rem_melhor    = 0.0
    n_bids_rem_melhor = 0

    logs_cenarios = [{
        "data_ficheiro":    internal_file,
        "Hora":             Hora,
        "pais":             pais,
        "cenario":          "base",
        "preco_clearing":   preco_base,
        "volume_clearing":  volume_base,
        "lucro_pre":        lucro_base,
        "n_bids_removidos": 0,
        "vol_removido":     0.0,
    }]

    if pre_candidatos.empty:
        pass  # sem bids PRE — resultado é o base
    else:
        # ── Escalões de venda acima do clearing base ──────────────────────────
        escaloes_acima = sorted(
            vendas_s[vendas_s["Precio"] > preco_base + 1e-6]["Precio"].unique()
        )

        for p_alvo in escaloes_acima:
            # Volume mínimo a remover para que o clearing salte para p_alvo:
            #   sell_vol(P⁻) = Σ Energia de vendas com Precio < p_alvo
            #   buy_vol(P)   = Σ Energia de compras com Precio ≥ p_alvo
            #   vol_min      = max(0, sell_vol(P⁻) − buy_vol(P))
            sell_abaixo = vendas_s[vendas_s["Precio"] < p_alvo - 1e-6]["Energia"].sum()
            buy_acima   = compras_s[compras_s["Precio"] >= p_alvo - 1e-6]["Energia"].sum()
            vol_min     = max(0.0, sell_abaixo - buy_acima)

            if vol_min > vol_pre_total + 1e-6:
                continue   # escalão inatingível com o PRE disponível

            # Acumular bids PRE (menores primeiro) até cobrir vol_min
            while n_bids_acum < len(energias_pre) and vol_rem_acum < vol_min - 1e-6:
                vol_rem_acum += energias_pre[n_bids_acum]
                n_bids_acum  += 1

            # Clearing analítico: aplica vol_rem como offset escalar nos V_acum
            # da curva de venda — sem copiar nenhum array.
            preco_iter, volume_iter = _clearing_analitico(
                cp, cv, vp, ve, vv, j_shift, vol_rem=vol_rem_acum
            )
            if preco_iter is None:
                continue

            # Lucro: apenas bids PRE com índice >= n_bids_acum (não removidos)
            lucro_iter = _lucro_pre_analitico(volume_iter, vol_rem_acum, preco_iter,
                                              n_rem=n_bids_acum)

            logs_cenarios.append({
                "data_ficheiro":    internal_file,
                "Hora":             Hora,
                "pais":             pais,
                "cenario":          f"esc_{p_alvo:.4f}",
                "preco_clearing":   preco_iter,
                "volume_clearing":  volume_iter,
                "lucro_pre":        lucro_iter,
                "n_bids_removidos": n_bids_acum,
                "vol_removido":     round(vol_rem_acum, 4),
            })

            if lucro_iter > lucro_melhor + 1e-6:
                lucro_melhor      = lucro_iter
                preco_melhor      = preco_iter
                volume_melhor     = volume_iter
                vol_rem_melhor    = vol_rem_acum
                n_bids_rem_melhor = n_bids_acum

    # ── Resultado do cenário óptimo ───────────────────────────────────────────
    # Bids PRE presentes (não removidos): índice >= n_bids_rem_melhor
    vacum_opt  = pre_vacum_ord[n_bids_rem_melhor:]
    energy_opt = pre_energy_ord[n_bids_rem_melhor:]
    unidad_opt = pre_unidad_ord[n_bids_rem_melhor:]
    desp_mask_opt      = vacum_opt <= volume_melhor + vol_rem_melhor + 1e-6
    vol_pre_despachado = float(energy_opt[desp_mask_opt].sum())
    unidades_pre_desp  = list(dict.fromkeys(unidad_opt[desp_mask_opt]))

    # Volume PRE despachado no base (n_rem=0: todos os bids presentes)
    # No cenário base, vol_rem=0, logo o threshold é simplesmente volume_base.
    desp_mask_base      = pre_vacum_ord <= volume_base + 1e-6
    vol_pre_desp_base   = float(pre_energy_ord[desp_mask_base].sum())

    tprint(
        f"  [OK] {internal_file} | {Hora} | {pais} "
        f"| orig={preco_orig} | base={preco_base} | opt={preco_melhor} "
        f"| lucro_base={lucro_base:.0f} € | lucro_opt={lucro_melhor:.0f} € "
        f"| delta={lucro_melhor - lucro_base:+.0f} € "
        f"| bids_rem={n_bids_rem_melhor}"
    )

    row = {
        "data_ficheiro":            internal_file,
        "Hora":                     Hora,
        "pais":                     pais,
        # ── Clearing original (sem escala) ────────────────────────────────
        "preco_clearing_orig":      preco_orig,
        "volume_clearing_orig":     volume_orig,
        # ── Clearing base (com escala, sem remoções PRE) ──────────────────
        "preco_clearing_base":      preco_base,
        "volume_clearing_base":     volume_base,
        # ── Clearing óptimo (com remoção PRE) ─────────────────────────────
        "preco_clearing_opt":       preco_melhor,
        "volume_clearing_opt":      volume_melhor,
        # ── Lucro PRE — base ──────────────────────────────────────────────
        # lucro = vol_PRE_despachado_a_zero × clearing_price
        "vol_pre_despachado_base":  round(vol_pre_desp_base, 4),
        "lucro_pre_base":           round(lucro_base, 4),
        # ── Lucro PRE — óptimo ────────────────────────────────────────────
        "vol_pre_despachado_opt":   round(vol_pre_despachado, 4),
        "lucro_pre_opt":            round(lucro_melhor, 4),
        # ── Comparação base → óptimo ──────────────────────────────────────
        "delta_preco":              round(preco_melhor - preco_base, 4)
                                    if preco_melhor is not None and preco_base is not None else None,
        "delta_vol_pre_despachado": round(vol_pre_despachado - vol_pre_desp_base, 4),
        "delta_lucro_pre":          round(lucro_melhor - lucro_base, 4),
        "delta_lucro_pre_pct":      round((lucro_melhor / lucro_base - 1) * 100, 4)
                                    if lucro_base and lucro_base > 1e-9 else None,
        # ── Detalhes do cenário óptimo ────────────────────────────────────
        "vol_pre_removido_opt":     round(vol_rem_melhor, 4),
        "n_bids_pre_removidos":     n_bids_rem_melhor,
        "unidades_pre_despachadas": ";".join(unidades_pre_desp),
        "n_cenarios_testados":      len(logs_cenarios),
    }
    return row, logs_cenarios


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
            .sort_values(['data_ficheiro', 'Hora', 'pais', 'cenario'])
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

    # 2. Processar bids e calcular clearing original + optimização PRE
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
    guarda_resultados(df_resultado, 'historico_clearing_otimizacao-c5.csv')
    guarda_resultados(df_logs,      'log_cenarios_otimizacao-c5.csv')

    # 4. Resumo
    if not df_resultado.empty:
        tprint("\n── Resumo ──────────────────────────────────────")
        tprint(f"  Períodos processados:        {len(df_resultado)}")
        if 'delta_preco_base_opt' in df_resultado.columns:
            delta = df_resultado['delta_preco_base_opt'].dropna()
            tprint(f"  Delta preço médio (opt):     {delta.mean():.2f} €/MWh")
            tprint(f"  Delta preço máx  (opt):      {delta.max():.2f} €/MWh")
            tprint(f"  Delta preço mín  (opt):      {delta.min():.2f} €/MWh")
        if 'delta_lucro_pre' in df_resultado.columns:
            dl = df_resultado['delta_lucro_pre'].dropna()
            tprint(f"  Delta lucro PRE médio:       {dl.mean():.0f} €")
            tprint(f"  Delta lucro PRE total:       {dl.sum():.0f} €")
        if 'n_bids_pre_removidos' in df_resultado.columns:
            tprint(f"  Bids PRE removidos (médio):  "
                   f"{df_resultado['n_bids_pre_removidos'].mean():.1f}")
        tprint("────────────────────────────────────────────────")