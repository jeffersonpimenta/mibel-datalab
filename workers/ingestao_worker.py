#!/usr/bin/env python3
"""
MIBEL Platform — Ingestão Worker
==================================
Lê um ficheiro ZIP mensal de bids OMIE (curva_pbc_uof_YYYYMM.zip) e insere
os dados na tabela mibel.bids_raw do ClickHouse.

Fluxo:
  1. Abre o ZIP e lista os ficheiros CSV internos
  2. Para cada ficheiro CSV interno:
     a. Verifica se a data já existe em bids_raw (evita duplicados)
     b. Lê o CSV (sep=";", encoding="latin-1", skiprows=2)
     c. Aplica MAPA_COLUNAS para normalizar nomes de colunas
     d. Normaliza o campo Hora e extrai data do nome do ficheiro
     e. Insere em lote em mibel.bids_raw
  3. Regista [STATUS] DONE ou [STATUS] FAILED

Uso:
    python ingestao_worker.py \\
        --job_id  <UUID> \\
        --zip_path /data/bids/curva_pbc_uof_YYYYMM.zip \\
        [--workers N]
"""

import argparse
import os
import sys
import threading
import traceback
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from io import StringIO

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import get_ch, ch_insert_batch, normaliza_hora, extrai_data, ensure_output_dir

# ══════════════════════════════════════════════════════════════════════════════
#  MAPEAMENTO DE COLUNAS — combina todas as variantes conhecidas dos ficheiros OMIE
# ══════════════════════════════════════════════════════════════════════════════

MAPA_COLUNAS: dict[str, str] = {
    # Energia — variantes com / e com \
    'Energía Compra/Venta':   'Energia',
    'Energía Compra\\Venta':  'Energia',
    'Energia Compra/Venta':   'Energia',
    'Energia Compra\\Venta':  'Energia',
    'Potencia Compra/Venta':  'Energia',
    'Potencia Compra\\Venta': 'Energia',
    'Potencia':               'Energia',
    'Energia':                'Energia',
    # Preço
    'Precio Compra/Venta':    'Precio',
    'Precio Compra\\Venta':   'Precio',
    'Precio':                 'Precio',
    # Hora / Período — "Hora" (formato numérico) ou "Periodo" (formato HxQy)
    'Hora':                   'Hora',
    'Periodo':                'Hora',
    # País
    'Pais':                   'Pais',
    'País':                   'Pais',
    # Tipo de oferta e código da unidade
    'Tipo Oferta':            'Tipo Oferta',
    'Unidad':                 'Unidad',
}

COLUNAS_OBRIGATORIAS = ('Hora', 'Pais', 'Tipo Oferta', 'Unidad', 'Energia', 'Precio')

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
#  VERIFICAÇÃO DE DADOS JÁ INGERIDOS
# ══════════════════════════════════════════════════════════════════════════════

def datas_ja_ingeridas(ch, zip_nome: str) -> set:
    """
    Devolve o conjunto de datas (strings 'YYYY-MM-DD') já presentes em
    mibel.bids_raw para o ZIP indicado, para evitar duplicados.
    """
    rows = ch.execute(
        "SELECT DISTINCT toString(data_ficheiro) "
        "FROM mibel.bids_raw "
        "WHERE zip_nome = %(zip)s",
        {'zip': zip_nome}
    )
    return {r[0] for r in rows}


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESSAMENTO DE UM FICHEIRO CSV INTERNO AO ZIP
# ══════════════════════════════════════════════════════════════════════════════

def processa_csv_interno(
    zip_path:       str,
    internal_file:  str,
    zip_nome:       str,
    datas_existentes: set,
    job_id:         str,
) -> tuple[int, str]:
    """
    Lê um ficheiro CSV de dentro do ZIP, faz parsing e insere em mibel.bids_raw.

    Cada thread cria a sua própria ligação ao ClickHouse para inserir em paralelo.

    Devolve (n_inserido, status):
      n_inserido ≥ 0  — número de linhas inseridas
      n_inserido = -1 — data já existia (ignorado)
      status: 'ok' | 'skip' | 'error'
    """
    data_str = extrai_data(internal_file)

    # Ignorar se a data já está no ClickHouse
    if data_str in datas_existentes:
        return -1, 'skip'

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
        with _print_lock:
            print(f'[ERRO] {internal_file}: falha na leitura — {e}', flush=True)
        return 0, 'error'

    # Verificar colunas obrigatórias
    faltam = [c for c in COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltam:
        with _print_lock:
            print(f'[AVISO] {internal_file}: colunas em falta {faltam} — ignorado', flush=True)
        return 0, 'error'

    # Converter Energia e Precio (formato ibérico: ponto=milhar, vírgula=decimal)
    for col in ('Energia', 'Precio'):
        df[col] = (
            df[col].astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0.0)
        )

    # Converter data_str para objecto date
    try:
        data_date = date.fromisoformat(data_str) if data_str != '1970-01-01' else None
    except ValueError:
        data_date = None

    if data_date is None:
        with _print_lock:
            print(f'[AVISO] {internal_file}: data não reconhecida ("{data_str}") — ignorado', flush=True)
        return 0, 'error'

    # ── Construção das linhas para inserção ──────────────────────────────────
    rows: list[dict] = []
    for _, row in df.iterrows():
        hora_val = str(row.get('Hora', '')).strip()
        hora_raw, hora_num, periodo_fmt = normaliza_hora(hora_val)

        tipo = str(row.get('Tipo Oferta', '')).strip().upper()
        if tipo not in ('C', 'V'):
            continue

        rows.append({
            'data_ficheiro':   data_date,
            'ficheiro_nome':   internal_file,
            'zip_nome':        zip_nome,
            'hora_raw':        hora_raw,
            'hora_num':        hora_num,
            'periodo_formato': periodo_fmt,
            'pais':            str(row.get('Pais', '')).strip(),
            'tipo_oferta':     tipo,
            'unidade':         str(row.get('Unidad', '')).strip(),
            'energia':         float(row.get('Energia', 0.0) or 0.0),
            'precio':          float(row.get('Precio', 0.0) or 0.0),
        })

    if not rows:
        with _print_lock:
            print(f'[AVISO] {internal_file}: nenhuma linha C/V válida após parsing', flush=True)
        return 0, 'error'

    # ── Inserção no ClickHouse (ligação própria da thread) ───────────────────
    ch_thread = get_ch()
    try:
        inserted = ch_insert_batch(ch_thread, 'mibel.bids_raw', rows)
    finally:
        try:
            ch_thread.disconnect()
        except Exception:
            pass

    return inserted, 'ok'


# ══════════════════════════════════════════════════════════════════════════════
#  ORQUESTRADOR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def run_worker(job_id: str, zip_path: str, n_workers: int = 4) -> bool:
    """
    Ponto de entrada principal do worker de ingestão.
    Lê o ZIP, processa em paralelo e insere em mibel.bids_raw.
    """
    ch = None

    try:
        ensure_output_dir()
        ch = get_ch()

        zip_nome = os.path.basename(zip_path)

        log('INFO', '═' * 60, job_id, ch)
        log('INFO', f'Job ID    : {job_id}', job_id, ch)
        log('INFO', f'ZIP       : {zip_path}', job_id, ch)
        log('INFO', f'Workers   : {n_workers}', job_id, ch)
        log('INFO', '═' * 60, job_id, ch)

        # Verificar existência do ficheiro
        if not os.path.isfile(zip_path):
            log('ERRO', f'Ficheiro não encontrado: {zip_path}', job_id, ch)
            log('STATUS', 'FAILED', job_id, ch)
            return False

        # Listar ficheiros internos do ZIP
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                internal_files = [
                    f for f in z.namelist()
                    if not f.endswith('/')  # excluir directórios
                ]
        except Exception as e:
            log('ERRO', f'Não foi possível abrir o ZIP: {e}', job_id, ch)
            log('STATUS', 'FAILED', job_id, ch)
            return False

        log('INFO', f'{zip_nome}: {len(internal_files)} ficheiro(s) interno(s) encontrado(s)', job_id, ch)

        if not internal_files:
            log('AVISO', f'{zip_nome}: ZIP vazio — nada a processar', job_id, ch)
            log('STATUS', 'DONE', job_id, ch)
            return True

        # Verificar datas já ingeridas para este ZIP
        datas_existentes = datas_ja_ingeridas(ch, zip_nome)
        if datas_existentes:
            log('INFO',
                f'{zip_nome}: {len(datas_existentes)} data(s) já ingerida(s) — serão ignoradas',
                job_id, ch)

        # ── Processamento paralelo ────────────────────────────────────────────
        total_inserido = 0
        total_ignorado = 0
        total_erro     = 0

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futures = {
                ex.submit(
                    processa_csv_interno,
                    zip_path, ifile, zip_nome, datas_existentes, job_id,
                ): ifile
                for ifile in internal_files
            }

            concluidos = 0
            for fut in as_completed(futures):
                ifile      = futures[fut]
                concluidos += 1
                try:
                    n, status = fut.result()
                    if status == 'ok' and n > 0:
                        total_inserido += n
                        log('OK',
                            f'[{concluidos}/{len(internal_files)}] {ifile}: '
                            f'{n} bids inseridos  (total: {total_inserido})',
                            job_id, ch)
                    elif status == 'skip':
                        total_ignorado += 1
                        log('INFO',
                            f'[{concluidos}/{len(internal_files)}] {ifile}: já existia — ignorado',
                            job_id, ch)
                    else:
                        total_erro += 1
                        log('AVISO',
                            f'[{concluidos}/{len(internal_files)}] {ifile}: sem dados inseridos',
                            job_id, ch)
                except Exception as e:
                    total_erro += 1
                    log('ERRO', f'{ifile}: {e}', job_id, ch)

        # ── Resumo final ──────────────────────────────────────────────────────
        log('INFO', '═' * 60, job_id, ch)
        log('INFO', f'Bids inseridos      : {total_inserido}', job_id, ch)
        log('INFO', f'Ficheiros ignorados : {total_ignorado} (dados já existentes)', job_id, ch)
        log('INFO', f'Ficheiros com erro  : {total_erro}', job_id, ch)
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
        description='MIBEL Ingestão Worker — lê ZIP de bids OMIE e insere em mibel.bids_raw'
    )
    parser.add_argument('--job_id',   required=True, help='UUID do job')
    parser.add_argument('--zip_path', required=True, help='Caminho absoluto para o ficheiro ZIP')
    parser.add_argument('--workers',  type=int, default=4,
                        help='Threads paralelas para processamento dos CSVs (default: 4)')
    args = parser.parse_args()

    ok = run_worker(
        job_id    = args.job_id,
        zip_path  = args.zip_path,
        n_workers = args.workers,
    )
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
