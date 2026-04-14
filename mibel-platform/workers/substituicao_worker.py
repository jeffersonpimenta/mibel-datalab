#!/usr/bin/env python3
"""
MIBEL Platform — Substituição Worker
=====================================
Implementa exactamente a lógica do script
clearing_substituicao_multithread - C1.py, adaptada para a plataforma:
  • Configuração lida de /data/config/parametros.json (escalões)
  • Mapa de unidades lido de mibel.unidades (ClickHouse), populado por
    scripts/unidades/carrega_unidades_ch.py a partir de LISTA_UNIDADES.csv
  • Resultados inseridos no ClickHouse (clearing_substituicao + _logs)
  • Logging compreensivo para stdout e para a tabela worker_logs

Fluxo de processamento
──────────────────────
  1. Carrega escalões (parametros.json) e mapa de unidades
     (tabela mibel.unidades: CODIGO → regime + categoria_zona)
  2. Descobre ZIPs em /data/bids/ no intervalo de datas solicitado
  3. Para cada ZIP (paralelo nível-1):
       Para cada CSV interno (paralelo nível-2):
         Lê DataFrame, constrói mapa de unidades para o ficheiro
         Para cada (Hora, Pais) (paralelo nível-3):
           a. Clearing ORIGINAL com clearing() de clearing.py
           b. aplica_escalao(): escala de volume + escalões de preço por bid
           c. Clearing COM SUBSTITUIÇÃO
           d. Grava resultado + log de substituições
  4. Insere em lote no ClickHouse
  5. Emite [STATUS] DONE ou [STATUS] FAILED

Uso:
    python substituicao_worker.py \\
        --job_id  <UUID> \\
        --data_inicio YYYY-MM-DD \\
        --data_fim    YYYY-MM-DD \\
        [--workers N]
"""

import argparse
import os
import re
import sys
import threading
import traceback
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from io import StringIO
from typing import Optional

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
sys.path.insert(0, '/app')                              # clearing.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # utils.py

from clearing import clearing  # algoritmo real (pointer + degrau handling)
from utils import (
    get_ch, ch_insert_batch,
    carrega_escaloes,
    carrega_mapa_unidades_ch,
    zip_files_no_intervalo, extrai_data, extrai_periodo_zip, normaliza_hora,
    ensure_output_dir,
)

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING THREAD-SAFE
# ══════════════════════════════════════════════════════════════════════════════

_print_lock = threading.Lock()


def log(nivel: str, mensagem: str, job_id: str = '', ch=None) -> None:
    """
    Escreve linha de log no stdout (thread-safe) e, opcionalmente,
    insere na tabela worker_logs do ClickHouse.

    Níveis usados: INFO, OK, AVISO, ERRO, CSV, ZIP, STATUS
    A última linha com [STATUS] DONE ou [STATUS] FAILED é detectada pelo PHP
    para actualizar o estado do job.
    """
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
            pass  # falha silenciosa — o log em stdout é suficiente


# ══════════════════════════════════════════════════════════════════════════════
#  MAPEAMENTO DE COLUNAS DOS FICHEIROS DE BIDS
# ══════════════════════════════════════════════════════════════════════════════

MAPA_COLUNAS: dict[str, str] = {
    # Energia
    'Energía Compra/Venta': 'Energia',
    'Energia Compra/Venta': 'Energia',
    'Potencia Compra/Venta': 'Energia',
    'Potencia':              'Energia',
    'Energia':               'Energia',
    # Preço
    'Precio Compra/Venta':   'Precio',
    'Precio':                'Precio',
    # Hora
    'Hora':                  'Hora',
    'Periodo':               'Hora',
    # País
    'Pais':                  'Pais',
    'País':                  'Pais',
    # Tipo oferta e unidade
    'Tipo Oferta':           'Tipo Oferta',
    'Unidad':                'Unidad',
    # Tecnologia / Tipo de unidade (usado para classificação)
    'Tipo Unidad':           'TIPO_UNIDAD',
    'TipoUnidad':            'TIPO_UNIDAD',
    'Tecnología':            'TIPO_UNIDAD',
    'Tecnologia':            'TIPO_UNIDAD',
}

COLUNAS_OBRIGATORIAS = ('Hora', 'Pais', 'Tipo Oferta', 'Unidad', 'Energia', 'Precio')


# ══════════════════════════════════════════════════════════════════════════════
#  CONSTRUÇÃO DO MAPA DE UNIDADES (CODIGO → (regime, categoria_zona))
# ══════════════════════════════════════════════════════════════════════════════

def build_mapa_unidades(
    df: pd.DataFrame,
    mapa_unidades_ch: dict,  # {CODIGO_UPPER: (regime, categoria_zona)} — da tabela mibel.unidades
    escaloes: dict,          # ESCALOES completo (para validação)
) -> dict:
    """
    Constrói {CODIGO_upper → (regime, categoria_zona)} filtrando do mapa
    pré-classificado da tabela mibel.unidades pelas unidades presentes neste ficheiro.

    A classificação (regime + categoria com sufixo _ES/_PT/_EXT) já foi calculada
    por classificacao_pre.py a partir de LISTA_UNIDADES.csv, que contém a zona
    de cada unidade. Não é necessário inferir o sufixo a partir do campo Pais
    do ficheiro de bids.

    Unidades não presentes na tabela ou cujo regime/categoria não existe nos
    escalões configurados são excluídas do mapa e ignoradas.
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
    """
    Pré-calcula {(classe, categoria): {hora_int: volume_orig}} para
    categorias que tenham "perfil_hora" definido.
    Necessário para normalizar o factor horário sem alterar o volume total diário.
    """
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
    """Factor efectivo para a hora actual, garantindo escala total diária."""
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
#  APLICAÇÃO DE ESCALA + ESCALÕES  (idêntico ao script C1 original)
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
    Aplica para TODAS as classes (PRE, PRO, CONSUMO, COMERCIALIZADOR, …):

      • "escala"      — multiplica Energia de cada bid pelo factor.
      • "perfil_hora" — factor horário não uniforme (requer volumes_diarios).
      • "escaloes"    — (PRE) substitui Precio≈0 pelos preços dos escalões,
                        distribuindo volume acumulado pelos limiares definidos.
      • "delta_preco" — adiciona offset a todos os Precios da categoria.

    Devolve (df_modificado, lista_de_logs_de_substituição).
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

            # ── 1. Escala de volume ──────────────────────────────────────────
            if 'escala' in cfg:
                if 'perfil_hora' in cfg and volumes_diarios is not None:
                    factor = calcula_factor_horario(Hora, cfg, volumes_diarios, classe, categoria)
                else:
                    factor = cfg['escala']

                if factor != 1.0:
                    df.loc[mask_cat, 'Energia'] = (
                        df.loc[mask_cat, 'Energia'] * factor
                    )
                    # Recalcular máscara pois Energia mudou (mas Unidad não)
                    unidades_upper = df['Unidad'].astype(str).str.strip().str.upper()

            # ── 2. Escalões de preço (bids com Precio ≈ 0) ──────────────────
            if 'escaloes' in cfg:
                escalonamento = cfg['escaloes']

                mask_zero = mask_cat & df['Precio'].between(-0.001, 0.001)
                df_zero   = df[mask_zero]
                if df_zero.empty:
                    continue

                vol_total = df_zero['Energia'].sum()
                vol_acum  = 0.0
                esc_idx   = 0

                # Limiares de volume acumulado por escalão
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
                        continue  # escalão a 0 → não modifica

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

            # ── 3. Delta de preço ────────────────────────────────────────────
            if 'delta_preco' in cfg:
                delta = cfg['delta_preco']
                if delta != 0.0:
                    df.loc[mask_cat, 'Precio'] = (
                        df.loc[mask_cat, 'Precio'] + delta
                    )

    return df, logs


# ══════════════════════════════════════════════════════════════════════════════
#  NÍVEL 3 — PROCESSAMENTO POR (Hora, Pais)  [função pura, thread-safe]
# ══════════════════════════════════════════════════════════════════════════════

def _processa_hora_pais(
    df: pd.DataFrame,
    internal_file: str,
    Hora: str,
    pais: str,
    mapa_unidades: dict,
    escaloes: dict,
    volumes_diarios: dict,
) -> tuple:
    """
    Calcula clearing original + clearing com substituição para um único
    par (Hora, Pais). Função pura e thread-safe.

    Devolve (row_dict | None, lista_de_logs).
    """
    compras = df[
        (df['Hora'] == Hora) & (df['Tipo Oferta'] == 'C') & (df['Pais'] == pais)
    ].copy()
    vendas = df[
        (df['Hora'] == Hora) & (df['Tipo Oferta'] == 'V') & (df['Pais'] == pais)
    ].copy()

    if compras.empty or vendas.empty:
        return None, []

    # ── Clearing ORIGINAL ────────────────────────────────────────────────────
    compras_o = compras.sort_values('Precio', ascending=False).reset_index(drop=True)
    vendas_o  = vendas.sort_values( 'Precio', ascending=True ).reset_index(drop=True)
    compras_o['Volume_Acumulado'] = compras_o['Energia'].cumsum()
    vendas_o ['Volume_Acumulado'] = vendas_o ['Energia'].cumsum()

    preco_orig, volume_orig = clearing(compras_df=compras_o, vendas_df=vendas_o)

    # ── Aplicar escala + escalões ────────────────────────────────────────────
    compras_mod, _ = aplica_escalao(
        compras, mapa_unidades, escaloes,
        Hora=Hora, volumes_diarios=volumes_diarios,
    )
    vendas_mod, logs_sub = aplica_escalao(
        vendas, mapa_unidades, escaloes,
        Hora=Hora, pais=pais, internal_file=internal_file,
        volumes_diarios=volumes_diarios,
    )

    # ── Clearing COM SUBSTITUIÇÃO ────────────────────────────────────────────
    compras_s = compras_mod.sort_values('Precio', ascending=False).reset_index(drop=True)
    vendas_s  = vendas_mod.sort_values( 'Precio', ascending=True ).reset_index(drop=True)
    compras_s['Volume_Acumulado'] = compras_s['Energia'].cumsum()
    vendas_s ['Volume_Acumulado'] = vendas_s ['Energia'].cumsum()

    preco_sub, volume_sub = clearing(compras_df=compras_s, vendas_df=vendas_s)

    delta = (
        (preco_sub - preco_orig)
        if preco_sub is not None and preco_orig is not None
        else None
    )

    row = {
        'Hora':                  Hora,
        'pais':                  pais,
        'internal_file':         internal_file,
        'preco_clearing_orig':   preco_orig,
        'volume_clearing_orig':  volume_orig,
        'preco_clearing_sub':    preco_sub,
        'volume_clearing_sub':   volume_sub,
        'delta_preco':           delta,
        'n_bids_substituidos':   len(logs_sub),
    }
    return row, logs_sub


# ══════════════════════════════════════════════════════════════════════════════
#  NÍVEL 2 — PROCESSAMENTO DE UM FICHEIRO CSV INTERNO AO ZIP
# ══════════════════════════════════════════════════════════════════════════════

def _processa_internal_file(
    zip_path: str,
    internal_file: str,
    mapa_unidades_ch: dict,
    escaloes: dict,
    workers_hora_pais: int,
    job_id: str,
    ch,
) -> tuple[list, list]:
    """
    Nível 2 — lê um CSV de dentro do ZIP, constrói o mapa de unidades
    específico para esse ficheiro, e paraleliza o clearing por (Hora, Pais).

    Devolve (rows, logs) onde:
      rows — lista de dicts prontos para inserção em clearing_substituicao
      logs — lista de dicts para clearing_substituicao_logs
    """
    zip_nome = os.path.basename(zip_path)
    log('CSV', f'{zip_nome} → {internal_file}', job_id, ch)

    # ── Leitura do CSV ───────────────────────────────────────────────────────
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open(internal_file) as f:
                content = f.read().decode('latin-1')

        df = pd.read_csv(StringIO(content), sep=';', dtype=str, skiprows=2)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns=MAPA_COLUNAS)
        df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')

    except Exception as e:
        log('ERRO', f'{internal_file}: falha na leitura — {e}', job_id, ch)
        return [], []

    # Verificar colunas obrigatórias
    faltam = [c for c in COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltam:
        log('AVISO', f'{internal_file}: colunas em falta {faltam} — ignorado', job_id, ch)
        return [], []

    # Converter Energia e Precio (formato ibérico: ponto=milhar, vírgula=decimal)
    for col in ('Energia', 'Precio'):
        df[col] = (
            df[col]
            .astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0.0)
        )

    n_rows  = len(df)
    n_units = df['Unidad'].nunique()
    paises  = sorted(df['Pais'].unique().tolist())
    horas   = sorted(df['Hora'].unique().tolist())
    log('INFO',
        f'{internal_file}: {n_rows} bids | {n_units} unidades | '
        f'países={paises} | horas {horas[0]}–{horas[-1]}',
        job_id, ch)

    # ── Construção do mapa de unidades ──────────────────────────────────────
    mapa_unidades = build_mapa_unidades(df, mapa_unidades_ch, escaloes)

    contagem_regime: dict[str, int] = {}
    for reg, _ in mapa_unidades.values():
        contagem_regime[reg] = contagem_regime.get(reg, 0) + 1

    resumo_reg = '  '.join(f'{r}={n}' for r, n in sorted(contagem_regime.items()))
    log('INFO',
        f'{internal_file}: {len(mapa_unidades)} unidades classificadas  |  {resumo_reg}',
        job_id, ch)

    n_sem = n_units - len(mapa_unidades)
    if n_sem:
        log('AVISO', f'{internal_file}: {n_sem} unidades sem classificação (serão ignoradas)', job_id, ch)

    # ── Volumes diários (para perfil_hora) ──────────────────────────────────
    volumes_diarios = calcula_volumes_diarios(df, mapa_unidades, escaloes)

    # ── Combinações (Hora, Pais) ─────────────────────────────────────────────
    combinacoes = [
        (h, p)
        for h in sorted(df['Hora'].unique())
        for p in sorted(df[df['Hora'] == h]['Pais'].unique())
    ]
    log('INFO', f'{internal_file}: {len(combinacoes)} combinações (Hora × País)', job_id, ch)

    rows: list = []
    logs: list = []

    # ── Paralelismo nível-3 ──────────────────────────────────────────────────
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
                    # Log por período com delta %
                    po = row['preco_clearing_orig']
                    ps = row['preco_clearing_sub']
                    delta = row['delta_preco']
                    if po and ps:
                        pct = ((ps / po) - 1) * 100 if po else 0
                        log('OK',
                            f'{internal_file}|H{h}|{p} '
                            f'orig={po:.4f} sub={ps:.4f} '
                            f'Δ={delta:+.4f} ({pct:+.2f}%) '
                            f'bids_sub={row["n_bids_substituidos"]}',
                            job_id)
                    else:
                        log('OK',
                            f'{internal_file}|H{h}|{p} orig={po} sub={ps}',
                            job_id)
                logs.extend(log_este)
            except Exception as e:
                log('ERRO', f'{internal_file}|H{h}|{p}: {e}', job_id, ch)

    # Resumo do ficheiro
    if rows:
        precos_o = [r['preco_clearing_orig'] for r in rows if r['preco_clearing_orig'] is not None]
        precos_s = [r['preco_clearing_sub']  for r in rows if r['preco_clearing_sub']  is not None]
        deltas   = [r['delta_preco']         for r in rows if r['delta_preco']         is not None]
        avg_o    = sum(precos_o) / len(precos_o) if precos_o else None
        avg_s    = sum(precos_s) / len(precos_s) if precos_s else None
        avg_d    = sum(deltas)   / len(deltas)   if deltas   else None
        log('INFO',
            f'{internal_file}: concluído — '
            f'{len(rows)} períodos | {sum(r["n_bids_substituidos"] for r in rows)} bids sub. | '
            f'avg_orig={avg_o:.4f} avg_sub={avg_s:.4f} avg_Δ={avg_d:+.4f}',
            job_id, ch)
    else:
        log('AVISO', f'{internal_file}: sem resultados de clearing', job_id, ch)

    return rows, logs


# ══════════════════════════════════════════════════════════════════════════════
#  NÍVEL 1 — PROCESSAMENTO DE UM ZIP COMPLETO
# ══════════════════════════════════════════════════════════════════════════════

def _processa_zip(
    zip_path: str,
    mapa_unidades_ch: dict,
    escaloes: dict,
    workers_interno: int,
    workers_hora_pais: int,
    job_id: str,
    ch,
    data_inicio: str = '',
    data_fim: str = '',
) -> tuple[list, list]:
    """
    Nível 1 — abre um ZIP, lista os seus ficheiros CSV internos e paraleliza
    o processamento com um sub-pool de threads.

    Quando data_inicio/data_fim são fornecidos, filtra os ficheiros internos
    ao intervalo solicitado (útil para ZIPs mensais com CSVs diários).

    Devolve (rows, logs) acumulados de todos os ficheiros internos.
    """
    zip_nome = os.path.basename(zip_path)
    log('ZIP', f'A abrir {zip_nome}', job_id, ch)

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            all_internal = z.namelist()
    except Exception as e:
        log('ERRO', f'{zip_nome}: não foi possível abrir o ZIP — {e}', job_id, ch)
        return [], []

    # Filtrar ficheiros internos pelo intervalo de datas (para ZIPs mensais)
    if data_inicio and data_fim:
        ini = date.fromisoformat(data_inicio)
        fim = date.fromisoformat(data_fim)
        internal_files = []
        for f in all_internal:
            d_str = extrai_data(f)
            if d_str == '1970-01-01':
                # Nome sem data reconhecível → incluir por precaução
                internal_files.append(f)
                continue
            try:
                d = date.fromisoformat(d_str)
                if ini <= d <= fim:
                    internal_files.append(f)
            except ValueError:
                internal_files.append(f)

        excluidos = len(all_internal) - len(internal_files)
        if excluidos:
            log('INFO',
                f'{zip_nome}: {excluidos} ficheiro(s) interno(s) fora do intervalo ignorados',
                job_id, ch)
    else:
        internal_files = all_internal

    log('INFO', f'{zip_nome}: {len(internal_files)} ficheiro(s) interno(s) a processar: {internal_files}', job_id, ch)

    rows_zip: list = []
    logs_zip: list = []

    with ThreadPoolExecutor(max_workers=workers_interno) as ex:
        futures = {
            ex.submit(
                _processa_internal_file,
                zip_path, ifile,
                mapa_unidades_ch, escaloes,
                workers_hora_pais, job_id, None,  # ch=None nas threads filho
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
                log('ERRO', f'{zip_nome}/{ifile}: {e}', job_id, ch)

    log('ZIP',
        f'{zip_nome}: concluído — '
        f'{len(rows_zip)} períodos | {sum(r["n_bids_substituidos"] for r in rows_zip)} bids sub.',
        job_id, ch)

    return rows_zip, logs_zip


# ══════════════════════════════════════════════════════════════════════════════
#  ORQUESTRADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_worker(
    job_id: str,
    data_inicio: str,
    data_fim: str,
    n_workers: int = 4,
) -> bool:
    """
    Ponto de entrada principal do worker.

    Arquitectura de threads:
      workers_zip       = n_workers        (ZIPs em paralelo)
      workers_interno   = max(1, n_workers // 2)  (ficheiros por ZIP)
      workers_hora_pais = max(2, n_workers)        (pares hora/país)
    """
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
        log('INFO', 'A carregar configuração (escalões + mapa de unidades do ClickHouse)…', job_id, ch)
        escaloes         = carrega_escaloes()
        mapa_unidades_ch = carrega_mapa_unidades_ch(ch)  # {CODIGO: (regime, categoria)} da tabela mibel.unidades

        n_pre    = len(escaloes.get('PRE', {}))
        n_outras = sum(len(v) for k, v in escaloes.items() if k != 'PRE')
        log('INFO',
            f'Configuração: '
            f'{len(mapa_unidades_ch)} unidades classificadas | '
            f'{n_pre} categorias PRE | '
            f'{n_outras} categorias outras classes',
            job_id, ch)

        # ── 2. Descobrir ZIPs no intervalo ───────────────────────────────────
        zip_files = zip_files_no_intervalo(data_inicio, data_fim)
        if not zip_files:
            log('AVISO', f'Nenhum ficheiro ZIP encontrado em /data/bids/ '
                         f'no intervalo {data_inicio} → {data_fim}', job_id, ch)
            log('STATUS', 'DONE', job_id, ch)
            return True

        log('INFO', f'Encontrados {len(zip_files)} ficheiro(s) ZIP:', job_id, ch)
        for zp in zip_files:
            log('INFO', f'  • {os.path.basename(zp)}', job_id, ch)

        # ── 3. Processar todos os ZIPs ────────────────────────────────────────
        workers_zip       = n_workers
        workers_interno   = max(1, n_workers // 2)
        workers_hora_pais = max(2, n_workers)

        log('INFO',
            f'Paralelismo: ZIP={workers_zip} | interno={workers_interno} | hora/país={workers_hora_pais}',
            job_id, ch)

        all_rows: list = []
        all_logs: list = []
        erros: list    = []

        with ThreadPoolExecutor(max_workers=workers_zip) as ex:
            futures = {
                ex.submit(
                    _processa_zip,
                    zp, mapa_unidades_ch, escaloes,
                    workers_interno, workers_hora_pais,
                    job_id, None,  # ch=None nas threads filho
                    data_inicio, data_fim,
                ): zp
                for zp in zip_files
            }
            concluidos = 0
            for fut in as_completed(futures):
                zp = futures[fut]
                concluidos += 1
                try:
                    rows, logs = fut.result()
                    all_rows.extend(rows)
                    all_logs.extend(logs)
                    log('INFO',
                        f'[{concluidos}/{len(zip_files)}] {os.path.basename(zp)} processado — '
                        f'{len(rows)} períodos acumulados até agora: {len(all_rows)}',
                        job_id, ch)
                except Exception as e:
                    nome = os.path.basename(zp)
                    erros.append(nome)
                    log('ERRO', f'{nome}: {e}', job_id, ch)

        # ── 4. Inserir resultados no ClickHouse ──────────────────────────────
        log('INFO', '─' * 60, job_id, ch)
        log('INFO', f'Total: {len(all_rows)} períodos | {len(all_logs)} substituições de preço',
            job_id, ch)

        if all_rows:
            log('INFO', f'A inserir {len(all_rows)} linhas em clearing_substituicao…', job_id, ch)

            rows_ch = []
            for r in all_rows:
                hora_raw, hora_num, _ = normaliza_hora(r['Hora'])
                data_str = extrai_data(r['internal_file'])
                try:
                    data_date = date.fromisoformat(data_str)
                except ValueError:
                    data_date = date(1970, 1, 1)

                rows_ch.append({
                    'job_id':                job_id,
                    'data_ficheiro':         data_str,
                    'data_date':             data_date,
                    'hora_raw':              hora_raw,
                    'hora_num':              hora_num,
                    'pais':                  r['pais'],
                    'preco_clearing_orig':   r['preco_clearing_orig'],
                    'volume_clearing_orig':  r['volume_clearing_orig'],
                    'preco_clearing_sub':    r['preco_clearing_sub'],
                    'volume_clearing_sub':   r['volume_clearing_sub'],
                    'delta_preco':           r['delta_preco'],
                    'n_bids_substituidos':   r['n_bids_substituidos'] or 0,
                })

            inserted = ch_insert_batch(ch, 'mibel.clearing_substituicao', rows_ch)
            log('INFO', f'Inseridos {inserted} registos em clearing_substituicao', job_id, ch)
        else:
            log('AVISO', 'Sem resultados de clearing para inserir', job_id, ch)

        if all_logs:
            log('INFO', f'A inserir {len(all_logs)} linhas em clearing_substituicao_logs…', job_id, ch)

            logs_ch = []
            for l in all_logs:
                hora_raw, hora_num, _ = normaliza_hora(l.get('Hora', '0'))
                data_str = extrai_data(l.get('internal_file', ''))
                try:
                    data_date = date.fromisoformat(data_str)
                except ValueError:
                    data_date = date(1970, 1, 1)

                logs_ch.append({
                    'job_id':        job_id,
                    'data_ficheiro': data_str,
                    'data_date':     data_date,
                    'hora_raw':      hora_raw,
                    'hora_num':      hora_num,
                    'pais':          l.get('pais', ''),
                    'unidade':       str(l.get('Unidad', '')),
                    'categoria':     l.get('categoria', ''),
                    'escalao_preco': float(l.get('escalao_preco', 0) or 0),
                    'preco_original': float(l.get('preco_original', 0) or 0),
                    'energia_mw':    float(l.get('Energia_MW', 0) or 0),
                })

            inserted = ch_insert_batch(ch, 'mibel.clearing_substituicao_logs', logs_ch)
            log('INFO', f'Inseridos {inserted} registos em clearing_substituicao_logs', job_id, ch)

        # ── 5. Resumo final ──────────────────────────────────────────────────
        log('INFO', '═' * 60, job_id, ch)
        total_bids_sub = sum(r.get('n_bids_substituidos', 0) for r in all_rows)
        precos_o = [r['preco_clearing_orig'] for r in all_rows if r['preco_clearing_orig'] is not None]
        precos_s = [r['preco_clearing_sub']  for r in all_rows if r['preco_clearing_sub']  is not None]
        deltas   = [r['delta_preco']         for r in all_rows if r['delta_preco']          is not None]

        if precos_o:
            log('INFO',
                f'Preço médio original : {sum(precos_o)/len(precos_o):.4f} €/MWh', job_id, ch)
        if precos_s:
            log('INFO',
                f'Preço médio simulado : {sum(precos_s)/len(precos_s):.4f} €/MWh', job_id, ch)
        if deltas:
            log('INFO',
                f'Delta médio          : {sum(deltas)/len(deltas):+.4f} €/MWh  '
                f'(min={min(deltas):+.4f}  max={max(deltas):+.4f})', job_id, ch)

        log('INFO', f'Períodos processados : {len(all_rows)}', job_id, ch)
        log('INFO', f'Bids substituídos    : {total_bids_sub}', job_id, ch)
        log('INFO', f'ZIPs com erro        : {len(erros)}', job_id, ch)

        if erros:
            for e in erros:
                log('AVISO', f'  Ficheiro com erro: {e}', job_id, ch)

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
        description='MIBEL Substituição Worker — clearing original + PRE substituição'
    )
    parser.add_argument('--job_id',      required=True, help='UUID do job (SQLite)')
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
