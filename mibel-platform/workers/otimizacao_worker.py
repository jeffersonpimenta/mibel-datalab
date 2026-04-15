#!/usr/bin/env python3
"""
MIBEL Platform — Optimização Worker
=====================================
Implementa a lógica de clearing_otimizacao_pre_multithread.py, adaptada para
a plataforma:
  • Dados lidos de mibel.bids_raw (ClickHouse), não de ZIPs em disco
  • Resultados inseridos em mibel.clearing_otimizacao + _logs
  • Logging compreensivo para stdout e para worker_logs

Algoritmo (por par Hora × País)
────────────────────────────────
  1. Clearing ORIGINAL (sem escala) — baseline OMIE real.
  2. Aplica escala de volumes (aplica_escalao) a compras e vendas.
  3. Comprime curvas em step tables (arrays numpy, um registo por preço único)
     e calcula o clearing BASE analítico — O(n_preços_únicos).
  4. Para cada nível de preço de venda acima do clearing base:
       a. Calcula o volume mínimo de bids PRE (Precio≈0) a remover para que
          o clearing salte para esse nível.
       b. Remove bids PRE acumulando os de menor energia primeiro.
       c. Recalcula o clearing analítico com vol_rem como offset escalar
          sobre o volume acumulado da curva de venda — sem reconstruir arrays.
       d. Calcula o lucro PRE = volume_pre_despachado × preco_clearing.
  5. Regista o cenário de lucro máximo.

Uso:
    python otimizacao_worker.py \\
        --job_id  <UUID> \\
        --data_inicio YYYY-MM-DD \\
        --data_fim    YYYY-MM-DD \\
        [--workers N]
"""

import argparse
import os
import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, '/app')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from clearing import clearing
from utils import (
    get_ch, ch_insert_batch,
    carrega_escaloes,
    carrega_mapa_unidades_ch,
    normaliza_hora,
    extrai_data,
    ensure_output_dir,
)

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING THREAD-SAFE
# ══════════════════════════════════════════════════════════════════════════════

_print_lock = threading.Lock()


def log(nivel: str, mensagem: str, job_id: str = '', ch=None) -> None:
    ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    line = f'[{ts}] [{nivel}] {mensagem}'
    with _print_lock:
        print(line, flush=True)

    if ch and job_id:
        try:
            ch.execute(
                'INSERT INTO mibel.worker_logs (job_id, nivel, mensagem) VALUES',
                [{'job_id': job_id, 'nivel': nivel, 'mensagem': mensagem}]
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  CONSTRUÇÃO DO MAPA DE UNIDADES  (idêntico ao substituicao_worker)
# ══════════════════════════════════════════════════════════════════════════════

def build_mapa_unidades(
    df: pd.DataFrame,
    mapa_unidades_ch: dict,
    escaloes: dict,
) -> dict:
    """
    Filtra o mapa pré-classificado da tabela mibel.unidades pelas unidades
    presentes neste DataFrame.  Devolve {CODIGO_upper → (regime, categoria_zona)}.
    """
    unidades_no_ficheiro = set(
        df['Unidad'].astype(str).str.strip().str.upper().unique()
    )
    mapa: dict = {}
    for unidade in unidades_no_ficheiro:
        if unidade not in mapa_unidades_ch:
            continue
        regime, cat_zona = mapa_unidades_ch[unidade]
        if regime in escaloes and isinstance(escaloes[regime], dict):
            if cat_zona in escaloes[regime]:
                mapa[unidade] = (regime, cat_zona)
    return mapa


# ══════════════════════════════════════════════════════════════════════════════
#  VOLUMES DIÁRIOS (suporte a perfil_hora)
# ══════════════════════════════════════════════════════════════════════════════

def calcula_volumes_diarios(
    df: pd.DataFrame,
    mapa_unidades: dict,
    escaloes: dict,
) -> dict:
    volumes: dict = {}
    unidades_upper = df['Unidad'].astype(str).str.strip().str.upper()

    for classe, cats_dict in escaloes.items():
        for categoria, cfg in cats_dict.items():
            if 'perfil_hora' not in cfg:
                continue
            codigos = {
                cod for cod, (reg, cat) in mapa_unidades.items()
                if reg == classe and cat == categoria
            }
            if not codigos:
                continue
            mask = unidades_upper.isin(codigos)
            if not mask.any():
                continue
            df_cat = df[mask].copy()
            df_cat['_h'] = pd.to_numeric(df_cat['Hora'], errors='coerce')
            volumes[(classe, categoria)] = (
                df_cat.groupby('_h')['Energia'].sum().to_dict()
            )
    return volumes


def calcula_factor_horario(
    hora,
    cfg: dict,
    volumes_diarios: dict,
    classe: str,
    categoria: str,
) -> float:
    perfil = cfg['perfil_hora']
    escala = cfg.get('escala', 1.0)
    try:
        hora_int = int(hora)
    except (ValueError, TypeError):
        return escala
    vol_hora = volumes_diarios.get((classe, categoria), {})
    if not vol_hora:
        return escala
    soma_pond = sum(vol_hora.get(h, 0.0) * perfil.get(h, 1.0) for h in vol_hora)
    if soma_pond == 0:
        return escala
    k = sum(vol_hora.values()) * escala / soma_pond
    return perfil.get(hora_int, 1.0) * k


# ══════════════════════════════════════════════════════════════════════════════
#  APLICAÇÃO DE ESCALA  (sem substituição de preço — apenas volume)
# ══════════════════════════════════════════════════════════════════════════════

def aplica_escalao(
    df: pd.DataFrame,
    mapa_unidades: dict,
    escaloes: dict,
    Hora: str = '',
    pais: str = '',
    internal_file: str = '',
    volumes_diarios: Optional[dict] = None,
) -> tuple[pd.DataFrame, list]:
    """
    Aplica escala de volume (e opcionalmente escalões de preço e delta_preco)
    a todas as classes.  Idêntico ao substituicao_worker — usado aqui apenas
    para a componente de escala; a substituição de preço é ignorada na
    optimização (o preço óptimo é calculado analiticamente).
    """
    df   = df.copy()
    logs = []
    unidades_upper = df['Unidad'].astype(str).str.strip().str.upper()

    for classe, cats_dict in escaloes.items():
        for categoria, cfg in cats_dict.items():
            codigos = {
                cod for cod, (reg, cat) in mapa_unidades.items()
                if reg == classe and cat == categoria
            }
            if not codigos:
                continue
            mask_cat = unidades_upper.isin(codigos)
            if not mask_cat.any():
                continue

            # ── Escala de volume ──────────────────────────────────────────────
            if 'escala' in cfg:
                if 'perfil_hora' in cfg and volumes_diarios is not None:
                    factor = calcula_factor_horario(Hora, cfg, volumes_diarios, classe, categoria)
                else:
                    factor = cfg['escala']
                if factor != 1.0:
                    df.loc[mask_cat, 'Energia'] = df.loc[mask_cat, 'Energia'] * factor
                    unidades_upper = df['Unidad'].astype(str).str.strip().str.upper()

            # ── Escalões de preço (apenas para registo; não altera o preço
            #    na optimização — o algoritmo trabalha com Precio≈0) ──────────
            if 'escaloes' in cfg:
                escalonamento = cfg['escaloes']
                mask_zero = mask_cat & df['Precio'].between(-0.001, 0.001)
                df_zero   = df[mask_zero]
                if df_zero.empty:
                    continue
                vol_total = df_zero['Energia'].sum()
                vol_acum  = 0.0
                esc_idx   = 0
                limiares: list[float] = []
                acum = 0.0
                for esc in escalonamento[:-1]:
                    acum += esc['pct_bids'] * vol_total
                    limiares.append(acum)
                limiares.append(float('inf'))
                for idx in df_zero.index:
                    energia_bid = df.at[idx, 'Energia']
                    vol_acum   += energia_bid
                    while (esc_idx < len(limiares) - 1
                           and vol_acum > limiares[esc_idx] + 1e-9):
                        esc_idx += 1
                    preco_novo = escalonamento[esc_idx]['preco']
                    if preco_novo == 0:
                        continue
                    preco_orig           = df.at[idx, 'Precio']
                    df.at[idx, 'Precio'] = preco_novo
                    logs.append({
                        'internal_file': internal_file,
                        'Hora':          Hora,
                        'pais':          pais,
                        'Unidad':        df.at[idx, 'Unidad'],
                        'classe':        classe,
                        'categoria':     categoria,
                        'escalao_preco': preco_novo,
                        'preco_original': preco_orig,
                        'Energia_MW':    energia_bid,
                    })

            # ── Delta de preço ────────────────────────────────────────────────
            if 'delta_preco' in cfg:
                delta = cfg['delta_preco']
                if delta != 0.0:
                    df.loc[mask_cat, 'Precio'] = df.loc[mask_cat, 'Precio'] + delta

    return df, logs


# ══════════════════════════════════════════════════════════════════════════════
#  CLEARING ANALÍTICO — step tables (numpy)
# ══════════════════════════════════════════════════════════════════════════════

def _build_step_arrays(
    compras_s: pd.DataFrame,
    vendas_s:  pd.DataFrame,
) -> tuple:
    """
    Comprime curvas de compra e venda em step tables (um registo por preço único).

    Devolve (cp, cv, vp, ve, vv, j_shift):
      cp, cv — preços e volumes acumulados de compra (DESC)
      vp, ve, vv — preços, energia e volumes acumulados de venda (ASC)
      j_shift — primeiro índice da curva de venda com Precio >= -0.001
    """
    c_df = (
        compras_s.groupby('Precio', sort=False)['Energia'].sum()
        .reset_index()
        .sort_values('Precio', ascending=False)
        .reset_index(drop=True)
    )
    c_df['V_acum'] = c_df['Energia'].cumsum()

    v_df = (
        vendas_s.groupby('Precio', sort=True)['Energia'].sum()
        .reset_index()
        .sort_values('Precio', ascending=True)
        .reset_index(drop=True)
    )
    v_df['V_acum'] = v_df['Energia'].cumsum()

    cp = c_df['Precio'].to_numpy(dtype=float)
    cv = c_df['V_acum'].to_numpy(dtype=float)
    vp = v_df['Precio'].to_numpy(dtype=float)
    ve = v_df['Energia'].to_numpy(dtype=float)
    vv = v_df['V_acum'].to_numpy(dtype=float)

    j_shift = int(np.searchsorted(vp, -0.001 - 1e-9, side='right'))

    return cp, cv, vp, ve, vv, j_shift


def _clearing_analitico(
    cp: np.ndarray,
    cv: np.ndarray,
    vp: np.ndarray,
    ve: np.ndarray,
    vv: np.ndarray,
    j_shift: int,
    vol_rem: float = 0.0,
) -> tuple[Optional[float], Optional[float]]:
    """
    Clearing analítico sobre step tables (arrays numpy).

    Replica o algoritmo de dois ponteiros de clearing.py sobre tabelas
    comprimidas — O(n_preços_únicos) em vez de O(n_bids).

    vol_rem desloca horizontalmente para a esquerda os volumes acumulados da
    curva de venda a partir de j_shift (bids com Precio >= -0.001):
        vv_ef[j] = vv[j] - vol_rem    se j >= j_shift
        vv_ef[j] = vv[j]              se j <  j_shift
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
    categorias_pre = set(escaloes.get('PRE', {}).keys())
    return {
        cod for cod, (reg, cat) in mapa_unidades.items()
        if reg == 'PRE' and cat in categorias_pre
    }


# ══════════════════════════════════════════════════════════════════════════════
#  NÍVEL 3 — OPTIMIZAÇÃO ANALÍTICA POR (Hora, País)
# ══════════════════════════════════════════════════════════════════════════════

def _processa_hora_pais(
    df: pd.DataFrame,
    internal_file: str,
    Hora: str,
    pais: str,
    mapa_unidades: dict,
    escaloes: dict,
    volumes_diarios: dict,
) -> tuple[Optional[dict], list]:
    """
    Clearing original + optimização analítica do lucro PRE para um par (Hora, Pais).
    Função pura e thread-safe.

    Algoritmo
    ─────────
    1. Clearing ORIGINAL (sem escala).
    2. Aplica escala de volumes → step tables → clearing BASE analítico.
    3. Remove bids PRE (Precio≈0), menores primeiro, para cada nível de preço
       de venda acima do base → recalcula clearing analítico com offset escalar.
    4. Regista o cenário de lucro máximo.
    """
    compras = df[
        (df['Hora'] == Hora) & (df['Tipo Oferta'] == 'C') & (df['Pais'] == pais)
    ].copy()
    vendas = df[
        (df['Hora'] == Hora) & (df['Tipo Oferta'] == 'V') & (df['Pais'] == pais)
    ].copy()

    if compras.empty or vendas.empty:
        return None, []

    # ── Clearing ORIGINAL (sem escala) ───────────────────────────────────────
    compras_orig = compras.sort_values('Precio', ascending=False).reset_index(drop=True)
    vendas_orig  = vendas.sort_values( 'Precio', ascending=True ).reset_index(drop=True)
    compras_orig['Volume_Acumulado'] = compras_orig['Energia'].cumsum()
    vendas_orig ['Volume_Acumulado'] = vendas_orig ['Energia'].cumsum()

    preco_orig, volume_orig = clearing(compras_df=compras_orig, vendas_df=vendas_orig)

    # ── Aplicar escala de volumes ────────────────────────────────────────────
    compras_scaled, _ = aplica_escalao(
        compras, mapa_unidades, escaloes,
        Hora=Hora, volumes_diarios=volumes_diarios,
    )
    vendas_scaled, _ = aplica_escalao(
        vendas, mapa_unidades, escaloes,
        Hora=Hora, volumes_diarios=volumes_diarios,
    )

    compras_s = compras_scaled.sort_values('Precio', ascending=False).reset_index(drop=True)
    vendas_s  = vendas_scaled.sort_values( 'Precio', ascending=True ).reset_index(drop=True)
    compras_s['Volume_Acumulado'] = compras_s['Energia'].cumsum()
    vendas_s ['Volume_Acumulado'] = vendas_s ['Energia'].cumsum()

    # ── Step tables + clearing BASE analítico ────────────────────────────────
    cp, cv, vp, ve, vv, j_shift = _build_step_arrays(compras_s, vendas_s)
    preco_base, volume_base = _clearing_analitico(cp, cv, vp, ve, vv, j_shift, vol_rem=0.0)

    if preco_base is None:
        return None, []

    # ── Identificar bids PRE com Precio ≈ 0 ─────────────────────────────────
    codigos_pre  = _identifica_codigos_pre(mapa_unidades, escaloes)
    unids_upper  = vendas_s['Unidad'].astype(str).str.strip().str.upper()
    mask_pre_zero = unids_upper.isin(codigos_pre) & vendas_s['Precio'].between(-0.001, 0.001)

    pre_candidatos = (
        vendas_s[mask_pre_zero]
        .sort_values('Energia', ascending=True)
        .reset_index(drop=True)
    )

    pre_vacum_ord  = pre_candidatos['Volume_Acumulado'].values
    pre_energy_ord = pre_candidatos['Energia'].values
    pre_unidad_ord = pre_candidatos['Unidad'].values

    def _lucro_pre(vol_clearing: float, vol_rem: float,
                   preco_clearing: float, n_rem: int) -> float:
        """Lucro PRE sem reconstruir DataFrames."""
        vacum_presentes  = pre_vacum_ord[n_rem:]
        energy_presentes = pre_energy_ord[n_rem:]
        desp_mask = vacum_presentes <= vol_clearing + vol_rem + 1e-6
        return float(energy_presentes[desp_mask].sum()) * preco_clearing

    # ── Estado inicial ────────────────────────────────────────────────────────
    vol_pre_total      = float(pre_candidatos['Energia'].sum())
    n_bids_acum        = 0
    vol_rem_acum       = 0.0

    lucro_base    = _lucro_pre(volume_base, 0.0, preco_base, n_rem=0)
    lucro_melhor  = lucro_base
    preco_melhor  = preco_base
    volume_melhor = volume_base
    vol_rem_melhor    = 0.0
    n_bids_rem_melhor = 0

    logs_cenarios = [{
        'data_ficheiro':    internal_file,
        'Hora':             Hora,
        'pais':             pais,
        'cenario':          'base',
        'preco_clearing':   preco_base,
        'volume_clearing':  volume_base,
        'lucro_pre':        lucro_base,
        'n_bids_removidos': 0,
        'vol_removido':     0.0,
    }]

    if not pre_candidatos.empty:
        escaloes_acima = sorted(
            vendas_s[vendas_s['Precio'] > preco_base + 1e-6]['Precio'].unique()
        )

        for p_alvo in escaloes_acima:
            sell_abaixo = vendas_s[vendas_s['Precio'] < p_alvo - 1e-6]['Energia'].sum()
            buy_acima   = compras_s[compras_s['Precio'] >= p_alvo - 1e-6]['Energia'].sum()
            vol_min     = max(0.0, sell_abaixo - buy_acima)

            if vol_min > vol_pre_total + 1e-6:
                continue

            # Acumular bids PRE (menores primeiro) até cobrir vol_min
            while (n_bids_acum < len(pre_energy_ord)
                   and vol_rem_acum < vol_min - 1e-6):
                vol_rem_acum += pre_energy_ord[n_bids_acum]
                n_bids_acum  += 1

            preco_iter, volume_iter = _clearing_analitico(
                cp, cv, vp, ve, vv, j_shift, vol_rem=vol_rem_acum
            )
            if preco_iter is None:
                continue

            lucro_iter = _lucro_pre(volume_iter, vol_rem_acum, preco_iter,
                                    n_rem=n_bids_acum)

            logs_cenarios.append({
                'data_ficheiro':    internal_file,
                'Hora':             Hora,
                'pais':             pais,
                'cenario':          f'esc_{p_alvo:.4f}',
                'preco_clearing':   preco_iter,
                'volume_clearing':  volume_iter,
                'lucro_pre':        lucro_iter,
                'n_bids_removidos': n_bids_acum,
                'vol_removido':     round(vol_rem_acum, 4),
            })

            if lucro_iter > lucro_melhor + 1e-6:
                lucro_melhor      = lucro_iter
                preco_melhor      = preco_iter
                volume_melhor     = volume_iter
                vol_rem_melhor    = vol_rem_acum
                n_bids_rem_melhor = n_bids_acum

    # ── Resultado do cenário óptimo ───────────────────────────────────────────
    vacum_opt  = pre_vacum_ord[n_bids_rem_melhor:]
    energy_opt = pre_energy_ord[n_bids_rem_melhor:]
    unidad_opt = pre_unidad_ord[n_bids_rem_melhor:]
    desp_mask_opt      = vacum_opt <= volume_melhor + vol_rem_melhor + 1e-6
    vol_pre_despachado = float(energy_opt[desp_mask_opt].sum())
    unidades_pre_desp  = list(dict.fromkeys(unidad_opt[desp_mask_opt].tolist()))

    desp_mask_base    = pre_vacum_ord <= volume_base + 1e-6
    vol_pre_desp_base = float(pre_energy_ord[desp_mask_base].sum())

    log('OK',
        f'{internal_file}|H{Hora}|{pais} '
        f'orig={preco_orig} base={preco_base} opt={preco_melhor} '
        f'lucro_base={lucro_base:.0f}€ lucro_opt={lucro_melhor:.0f}€ '
        f'Δ={lucro_melhor - lucro_base:+.0f}€ '
        f'bids_rem={n_bids_rem_melhor}')

    row = {
        'Hora':                     Hora,
        'pais':                     pais,
        'internal_file':            internal_file,
        'preco_clearing_orig':      preco_orig,
        'volume_clearing_orig':     volume_orig,
        'preco_clearing_base':      preco_base,
        'volume_clearing_base':     volume_base,
        'preco_clearing_opt':       preco_melhor,
        'volume_clearing_opt':      volume_melhor,
        'vol_pre_despachado_base':  round(vol_pre_desp_base, 4),
        'lucro_pre_base':           round(lucro_base, 4),
        'vol_pre_despachado_opt':   round(vol_pre_despachado, 4),
        'lucro_pre_opt':            round(lucro_melhor, 4),
        'delta_preco':              round(preco_melhor - preco_base, 4)
                                    if preco_melhor is not None and preco_base is not None else None,
        'delta_vol_pre_despachado': round(vol_pre_despachado - vol_pre_desp_base, 4),
        'delta_lucro_pre':          round(lucro_melhor - lucro_base, 4),
        'delta_lucro_pre_pct':      round((lucro_melhor / lucro_base - 1) * 100, 4)
                                    if lucro_base and lucro_base > 1e-9 else None,
        'vol_pre_removido_opt':     round(vol_rem_melhor, 4),
        'n_bids_pre_removidos':     n_bids_rem_melhor,
        'unidades_pre_despachadas': ';'.join(unidades_pre_desp),
        'n_cenarios_testados':      len(logs_cenarios),
    }
    return row, logs_cenarios


# ══════════════════════════════════════════════════════════════════════════════
#  NÍVEL 2 — PROCESSAMENTO DE UMA DATA A PARTIR DO CLICKHOUSE
# ══════════════════════════════════════════════════════════════════════════════

def _processa_data_ch(
    data_str: str,
    mapa_unidades_ch: dict,
    escaloes: dict,
    workers_hora_pais: int,
    job_id: str,
    ch,
) -> tuple[list, list]:
    """
    Carrega todos os bids de uma data a partir de mibel.bids_raw e paraleliza
    o clearing/optimização por (Hora, Pais).  Cada thread cria a sua própria
    ligação ao ClickHouse para a leitura.
    """
    internal_file = f'bids_{data_str.replace("-", "")}'
    log('INFO', f'A carregar data {data_str} de mibel.bids_raw…', job_id, ch)

    ch_local = get_ch()
    try:
        rows_ch, cols_meta = ch_local.execute(
            """
            SELECT
                hora_raw       AS Hora,
                pais           AS Pais,
                tipo_oferta    AS `Tipo Oferta`,
                unidade        AS Unidad,
                energia        AS Energia,
                precio         AS Precio
            FROM mibel.bids_raw
            WHERE data_ficheiro = toDate(%(data)s)
            """,
            {'data': data_str},
            with_column_types=True,
        )
    finally:
        try:
            ch_local.disconnect()
        except Exception:
            pass

    if not rows_ch:
        log('AVISO', f'{data_str}: sem dados em mibel.bids_raw', job_id, ch)
        return [], []

    col_names = [c[0] for c in cols_meta]
    df = pd.DataFrame(rows_ch, columns=col_names)

    n_rows  = len(df)
    n_units = df['Unidad'].nunique()
    paises  = sorted(df['Pais'].unique().tolist())
    horas   = sorted(df['Hora'].unique().tolist())
    log('INFO',
        f'{data_str}: {n_rows} bids | {n_units} unidades | '
        f'países={paises} | horas {horas[0]}–{horas[-1]}',
        job_id, ch)

    # Construção do mapa de unidades para esta data
    mapa_unidades = build_mapa_unidades(df, mapa_unidades_ch, escaloes)

    contagem_regime: dict[str, int] = {}
    for reg, _ in mapa_unidades.values():
        contagem_regime[reg] = contagem_regime.get(reg, 0) + 1
    resumo_reg = '  '.join(f'{r}={n}' for r, n in sorted(contagem_regime.items()))
    log('INFO',
        f'{data_str}: {len(mapa_unidades)} unidades classificadas  |  {resumo_reg}',
        job_id, ch)

    # Volumes diários (para perfil_hora)
    volumes_diarios = calcula_volumes_diarios(df, mapa_unidades, escaloes)

    # Combinações (Hora, Pais)
    combinacoes = [
        (h, p)
        for h in sorted(df['Hora'].unique())
        for p in sorted(df[df['Hora'] == h]['Pais'].unique())
    ]
    log('INFO', f'{data_str}: {len(combinacoes)} combinações (Hora × País)', job_id, ch)

    rows: list = []
    logs: list = []

    with ThreadPoolExecutor(max_workers=workers_hora_pais) as ex:
        futures = {
            ex.submit(
                _processa_hora_pais,
                df, internal_file, h, p,
                mapa_unidades, escaloes, volumes_diarios,
            ): (h, p)
            for h, p in combinacoes
        }
        for fut in as_completed(futures):
            h, p = futures[fut]
            try:
                row, log_este = fut.result()
                if row is not None:
                    rows.append(row)
                    logs.extend(log_este)
            except Exception as e:
                log('ERRO', f'{data_str}|H{h}|{p}: {e}', job_id, ch)

    if rows:
        deltas = [r['delta_lucro_pre'] for r in rows if r['delta_lucro_pre'] is not None]
        avg_d  = sum(deltas) / len(deltas) if deltas else None
        log('INFO',
            f'{data_str}: concluído — {len(rows)} períodos | '
            f'{sum(r["n_bids_pre_removidos"] for r in rows)} bids rem. | '
            f'avg_Δlucro={avg_d:+.0f}€' if avg_d is not None else
            f'{data_str}: concluído — {len(rows)} períodos',
            job_id, ch)
    else:
        log('AVISO', f'{data_str}: sem resultados', job_id, ch)

    return rows, logs


# ══════════════════════════════════════════════════════════════════════════════
#  ORQUESTRADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_worker(
    job_id: str,
    data_inicio: str,
    data_fim: str,
    n_workers: int = 4,
) -> bool:
    ch = None

    try:
        ensure_output_dir()
        ch = get_ch()

        log('INFO', '═' * 60, job_id, ch)
        log('INFO', f'Job ID       : {job_id}', job_id, ch)
        log('INFO', f'Intervalo    : {data_inicio} → {data_fim}', job_id, ch)
        log('INFO', f'Workers      : {n_workers}', job_id, ch)
        log('INFO', '═' * 60, job_id, ch)

        # ── 1. Carregar configuração ─────────────────────────────────────────
        log('INFO', 'A carregar configuração (escalões + mapa de unidades)…', job_id, ch)
        escaloes         = carrega_escaloes()
        mapa_unidades_ch = carrega_mapa_unidades_ch(ch)

        n_pre    = len(escaloes.get('PRE', {}))
        n_outras = sum(len(v) for k, v in escaloes.items() if k != 'PRE')
        log('INFO',
            f'{len(mapa_unidades_ch)} unidades | '
            f'{n_pre} categorias PRE | {n_outras} categorias outras',
            job_id, ch)

        # ── 2. Descobrir datas em mibel.bids_raw ─────────────────────────────
        rows_datas = ch.execute(
            "SELECT DISTINCT toString(data_ficheiro) "
            "FROM mibel.bids_raw "
            "WHERE data_ficheiro >= toDate(%(ini)s) "
            "  AND data_ficheiro <= toDate(%(fim)s) "
            "ORDER BY data_ficheiro",
            {'ini': data_inicio, 'fim': data_fim},
        )
        datas = [r[0] for r in rows_datas]

        if not datas:
            log('AVISO',
                f'Nenhum dado em mibel.bids_raw para {data_inicio} → {data_fim}. '
                'Ingira os ficheiros ZIP primeiro.',
                job_id, ch)
            log('STATUS', 'DONE', job_id, ch)
            return True

        log('INFO', f'Encontradas {len(datas)} data(s) em mibel.bids_raw:', job_id, ch)
        for d in datas[:10]:
            log('INFO', f'  • {d}', job_id, ch)
        if len(datas) > 10:
            log('INFO', f'  … e mais {len(datas) - 10} data(s)', job_id, ch)

        # ── 3. Processar todas as datas ───────────────────────────────────────
        workers_data      = n_workers
        workers_hora_pais = max(2, n_workers)

        log('INFO',
            f'Paralelismo: datas={workers_data} | hora/país={workers_hora_pais}',
            job_id, ch)

        all_rows: list = []
        all_logs: list = []
        erros: list    = []

        with ThreadPoolExecutor(max_workers=workers_data) as ex:
            futures = {
                ex.submit(
                    _processa_data_ch,
                    d, mapa_unidades_ch, escaloes,
                    workers_hora_pais,
                    job_id, None,
                ): d
                for d in datas
            }
            concluidos = 0
            for fut in as_completed(futures):
                d = futures[fut]
                concluidos += 1
                try:
                    rows, logs = fut.result()
                    all_rows.extend(rows)
                    all_logs.extend(logs)
                    log('INFO',
                        f'[{concluidos}/{len(datas)}] {d} — '
                        f'{len(rows)} períodos | acumulados: {len(all_rows)}',
                        job_id, ch)
                except Exception as e:
                    erros.append(d)
                    log('ERRO', f'{d}: {e}', job_id, ch)

        # ── 4. Inserir resultados no ClickHouse ──────────────────────────────
        log('INFO', '─' * 60, job_id, ch)
        log('INFO',
            f'Total: {len(all_rows)} períodos | {len(all_logs)} cenários testados',
            job_id, ch)

        if all_rows:
            log('INFO', f'A inserir {len(all_rows)} linhas em clearing_otimizacao…', job_id, ch)

            rows_ch = []
            for r in all_rows:
                hora_raw, hora_num, _ = normaliza_hora(r['Hora'])
                data_str = extrai_data(r['internal_file'])
                try:
                    data_date = date.fromisoformat(data_str)
                except ValueError:
                    data_date = date(1970, 1, 1)

                rows_ch.append({
                    'job_id':                   job_id,
                    'data_ficheiro':             data_str,
                    'data_date':                 data_date,
                    'hora_raw':                  hora_raw,
                    'hora_num':                  hora_num,
                    'pais':                      r['pais'],
                    'preco_clearing_orig':        r['preco_clearing_orig'],
                    'volume_clearing_orig':       r['volume_clearing_orig'],
                    'preco_clearing_base':        r['preco_clearing_base'],
                    'volume_clearing_base':       r['volume_clearing_base'],
                    'preco_clearing_opt':         r['preco_clearing_opt'],
                    'volume_clearing_opt':        r['volume_clearing_opt'],
                    'vol_pre_despachado_base':    r['vol_pre_despachado_base'],
                    'lucro_pre_base':             r['lucro_pre_base'],
                    'vol_pre_despachado_opt':     r['vol_pre_despachado_opt'],
                    'lucro_pre_opt':              r['lucro_pre_opt'],
                    'delta_preco':               r['delta_preco'],
                    'delta_vol_pre_despachado':  r['delta_vol_pre_despachado'],
                    'delta_lucro_pre':           r['delta_lucro_pre'],
                    'delta_lucro_pre_pct':       r['delta_lucro_pre_pct'],
                    'vol_pre_removido_opt':      r['vol_pre_removido_opt'],
                    'n_bids_pre_removidos':      r['n_bids_pre_removidos'],
                    'unidades_pre_despachadas':  r['unidades_pre_despachadas'],
                    'n_cenarios_testados':       r['n_cenarios_testados'],
                })

            inserted = ch_insert_batch(ch, 'mibel.clearing_otimizacao', rows_ch)
            log('INFO', f'Inseridos {inserted} registos em clearing_otimizacao', job_id, ch)
        else:
            log('AVISO', 'Sem resultados para inserir', job_id, ch)

        if all_logs:
            log('INFO', f'A inserir {len(all_logs)} cenários em clearing_otimizacao_logs…',
                job_id, ch)

            logs_ch = []
            for l in all_logs:
                hora_raw, hora_num, _ = normaliza_hora(l.get('Hora', '0'))
                data_str = extrai_data(l.get('data_ficheiro', ''))
                try:
                    data_date = date.fromisoformat(data_str)
                except ValueError:
                    data_date = date(1970, 1, 1)

                logs_ch.append({
                    'job_id':           job_id,
                    'data_ficheiro':    data_str,
                    'data_date':        data_date,
                    'hora_raw':         hora_raw,
                    'hora_num':         hora_num,
                    'pais':             l.get('pais', ''),
                    'cenario':          l.get('cenario', ''),
                    'preco_clearing':   l.get('preco_clearing'),
                    'volume_clearing':  l.get('volume_clearing'),
                    'lucro_pre':        float(l.get('lucro_pre', 0) or 0),
                    'n_bids_removidos': int(l.get('n_bids_removidos', 0) or 0),
                    'vol_removido':     float(l.get('vol_removido', 0) or 0),
                })

            inserted = ch_insert_batch(ch, 'mibel.clearing_otimizacao_logs', logs_ch)
            log('INFO', f'Inseridos {inserted} cenários em clearing_otimizacao_logs', job_id, ch)

        # ── 5. Resumo final ──────────────────────────────────────────────────
        log('INFO', '═' * 60, job_id, ch)
        lucros_b = [r['lucro_pre_base'] for r in all_rows if r.get('lucro_pre_base') is not None]
        lucros_o = [r['lucro_pre_opt']  for r in all_rows if r.get('lucro_pre_opt')  is not None]
        deltas_l = [r['delta_lucro_pre'] for r in all_rows if r.get('delta_lucro_pre') is not None]

        if lucros_b:
            log('INFO', f'Lucro PRE base    : {sum(lucros_b):.0f} € total  '
                        f'(avg {sum(lucros_b)/len(lucros_b):.2f} €/período)',
                job_id, ch)
        if lucros_o:
            log('INFO', f'Lucro PRE óptimo  : {sum(lucros_o):.0f} € total  '
                        f'(avg {sum(lucros_o)/len(lucros_o):.2f} €/período)',
                job_id, ch)
        if deltas_l:
            log('INFO', f'Delta lucro PRE   : {sum(deltas_l):+.0f} € total  '
                        f'(avg {sum(deltas_l)/len(deltas_l):+.2f} €/período)',
                job_id, ch)

        log('INFO', f'Períodos processados : {len(all_rows)}', job_id, ch)
        log('INFO', f'Datas com erro       : {len(erros)}', job_id, ch)

        if erros:
            for e in erros:
                log('AVISO', f'  Data com erro: {e}', job_id, ch)

        log('INFO', '═' * 60, job_id, ch)
        log('STATUS', 'DONE', job_id, ch)
        return True

    except Exception as e:
        msg = f'Erro fatal: {e}\n{traceback.format_exc()}'
        log('ERRO', msg, job_id, ch)
        log('STATUS', 'FAILED', job_id, ch)
        return False

    finally:
        if ch:
            try:
                ch.disconnect()
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description='MIBEL Optimização Worker — clearing base + optimização analítica PRE'
    )
    parser.add_argument('--job_id',      required=True, help='UUID do job')
    parser.add_argument('--data_inicio', required=True, help='Data início YYYY-MM-DD')
    parser.add_argument('--data_fim',    required=True, help='Data fim YYYY-MM-DD')
    parser.add_argument('--workers',     type=int, default=4,
                        help='Threads paralelas (default: 4)')
    args = parser.parse_args()

    try:
        date.fromisoformat(args.data_inicio)
        date.fromisoformat(args.data_fim)
    except ValueError as e:
        print(f'[ERRO] Formato de data inválido: {e}', flush=True)
        sys.exit(1)

    ok = run_worker(
        job_id      = args.job_id,
        data_inicio = args.data_inicio,
        data_fim    = args.data_fim,
        n_workers   = args.workers,
    )
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
