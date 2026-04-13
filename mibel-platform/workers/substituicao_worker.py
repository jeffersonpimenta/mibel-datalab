#!/usr/bin/env python3
"""
MIBEL Platform - Substitution Worker

Processes OMIE bid files and calculates clearing prices with PRE substitution.
Results are stored in ClickHouse tables.

Usage:
    python substituicao_worker.py --job_id JOB_ID --data_inicio YYYY-MM-DD --data_fim YYYY-MM-DD [--workers N]
"""

import argparse
import os
import sys
import zipfile
import io
import traceback
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
import numpy as np

# Add workers directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import (
    get_ch, ch_insert_batch, log,
    carrega_escaloes, carrega_mapa_unidades,
    zip_files_no_intervalo, extrai_data, normaliza_hora,
    get_categoria_zona, sufixo_de_pais, ensure_output_dir,
    calcula_clearing, aplica_substituicao_pre
)

# ============================================================================
# Constants
# ============================================================================

COLUMNS_BID = [
    'Hora', 'Fecha', 'Pais', 'Unidad', 'Tipo Oferta', 'Energia',
    'Precio', 'Ofertada', 'Casada', 'Tecnologia'
]

# ============================================================================
# Bid File Processing
# ============================================================================

def le_ficheiro_zip(zip_path: str) -> Optional[pd.DataFrame]:
    """
    Read bid data from OMIE ZIP file.

    Expected structure: ZIP contains one or more .1 files with semicolon-separated data.
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find the bid file (usually .1 extension)
            bid_files = [f for f in zf.namelist() if f.endswith('.1') or f.endswith('.txt')]

            if not bid_files:
                return None

            dfs = []
            for fname in bid_files:
                with zf.open(fname) as f:
                    content = f.read().decode('latin-1')

                # Parse semicolon-separated data
                lines = content.strip().split('\n')
                data = []

                for line in lines:
                    parts = line.split(';')
                    if len(parts) >= 10:
                        try:
                            row = {
                                'Hora': parts[0].strip(),
                                'Fecha': parts[1].strip(),
                                'Pais': parts[2].strip(),
                                'Unidad': parts[3].strip(),
                                'Tipo Oferta': parts[4].strip(),
                                'Energia': float(parts[5].replace(',', '.')) if parts[5].strip() else 0,
                                'Precio': float(parts[6].replace(',', '.')) if parts[6].strip() else 0,
                                'Ofertada': float(parts[7].replace(',', '.')) if parts[7].strip() else 0,
                                'Casada': float(parts[8].replace(',', '.')) if parts[8].strip() else 0,
                                'Tecnologia': parts[9].strip() if len(parts) > 9 else ''
                            }
                            data.append(row)
                        except (ValueError, IndexError):
                            continue

                if data:
                    dfs.append(pd.DataFrame(data))

            if dfs:
                return pd.concat(dfs, ignore_index=True)

    except Exception as e:
        print(f'Error reading ZIP {zip_path}: {e}', flush=True)

    return None


def processa_hora(
    df_hora: pd.DataFrame,
    hora_raw: str,
    hora_num: int,
    periodo_fmt: str,
    pais_filter: str,
    mapa: dict,
    escaloes: dict,
    job_id: str
) -> tuple:
    """
    Process bids for a single hour and country.

    Returns:
        (resultado_dict, logs_list)
    """
    # Separate buy and sell offers
    df_compra = df_hora[df_hora['Tipo Oferta'] == 'C'].copy()
    df_venda = df_hora[df_hora['Tipo Oferta'] == 'V'].copy()

    # Calculate original clearing
    ofertas_compra_orig = list(zip(df_compra['Precio'], df_compra['Energia']))
    ofertas_venda_orig = list(zip(df_venda['Precio'], df_venda['Energia']))

    preco_orig, volume_orig = calcula_clearing(ofertas_compra_orig, ofertas_venda_orig)

    # Apply PRE substitution to sell offers
    bids_venda_mod = []
    bids_venda_outros = []
    logs_substituicao = []
    n_bids_substituidos = 0

    for _, row in df_venda.iterrows():
        unidade = row['Unidad']
        tecnologia = row.get('Tecnologia', '')
        pais = row['Pais']

        regime, cat_zona = get_categoria_zona(unidade, tecnologia, pais, mapa, escaloes)

        if regime == 'PRE' and cat_zona in escaloes.get('PRE', {}):
            # This is a PRE unit - apply substitution
            config_cat = escaloes['PRE'][cat_zona]

            bid_orig = {
                'unidade': unidade,
                'energia': row['Energia'],
                'precio': row['Precio'],
                'categoria': cat_zona,
                'tipo_oferta': 'V'
            }

            # Apply substitution
            bids_mod, logs = aplica_substituicao_pre([bid_orig], cat_zona, config_cat)
            bids_venda_mod.extend(bids_mod)

            for log_entry in logs:
                log_entry.update({
                    'unidade': unidade,
                    'preco_original': row['Precio'],
                    'energia_mw': row['Energia']
                })
                logs_substituicao.append(log_entry)

            n_bids_substituidos += 1
        else:
            # Non-PRE or unclassified - keep original
            bids_venda_outros.append({
                'unidade': unidade,
                'energia': row['Energia'],
                'precio': row['Precio'],
                'categoria': cat_zona,
                'tipo_oferta': 'V'
            })

    # Calculate new clearing with substituted bids
    ofertas_venda_sub = [(b['precio'], b['energia']) for b in bids_venda_mod + bids_venda_outros]
    preco_sub, volume_sub = calcula_clearing(ofertas_compra_orig, ofertas_venda_sub)

    # Calculate delta
    delta_preco = None
    if preco_orig is not None and preco_sub is not None:
        delta_preco = preco_sub - preco_orig

    resultado = {
        'hora_raw': hora_raw,
        'hora_num': hora_num,
        'pais': pais_filter,
        'preco_clearing_orig': preco_orig,
        'volume_clearing_orig': volume_orig,
        'preco_clearing_sub': preco_sub,
        'volume_clearing_sub': volume_sub,
        'delta_preco': delta_preco,
        'n_bids_substituidos': n_bids_substituidos
    }

    return resultado, logs_substituicao


def processa_ficheiro(
    zip_path: str,
    mapa: dict,
    escaloes: dict,
    job_id: str,
    ch=None
) -> tuple:
    """
    Process a single ZIP file.

    Returns:
        (resultados_list, logs_list, nome_ficheiro)
    """
    nome_zip = os.path.basename(zip_path)
    data_ficheiro = extrai_data(nome_zip)

    df = le_ficheiro_zip(zip_path)
    if df is None or df.empty:
        log('AVISO', f'{nome_zip} | Ficheiro vazio ou inválido', job_id, ch)
        return [], [], nome_zip

    resultados = []
    logs_total = []

    # Process each country
    for pais in ['ES', 'PT']:
        df_pais = df[df['Pais'] == pais]
        if df_pais.empty:
            continue

        # Process each hour
        for hora_val in df_pais['Hora'].unique():
            df_hora = df_pais[df_pais['Hora'] == hora_val]
            hora_raw, hora_num, periodo_fmt = normaliza_hora(hora_val)

            try:
                resultado, logs = processa_hora(
                    df_hora, hora_raw, hora_num, periodo_fmt,
                    pais, mapa, escaloes, job_id
                )

                resultado['job_id'] = job_id
                resultado['data_ficheiro'] = data_ficheiro
                resultado['data_date'] = data_ficheiro

                resultados.append(resultado)

                for log_entry in logs:
                    log_entry.update({
                        'job_id': job_id,
                        'data_ficheiro': data_ficheiro,
                        'data_date': data_ficheiro,
                        'hora_raw': hora_raw,
                        'hora_num': hora_num,
                        'pais': pais
                    })
                    logs_total.append(log_entry)

            except Exception as e:
                log('ERRO', f'{nome_zip} | Hora {hora_val} Pais {pais}: {e}', job_id, ch)

    n_periodos = len(resultados)
    if resultados:
        p_orig_avg = np.mean([r['preco_clearing_orig'] for r in resultados if r['preco_clearing_orig'] is not None])
        p_sub_avg = np.mean([r['preco_clearing_sub'] for r in resultados if r['preco_clearing_sub'] is not None])
        log('OK', f'{nome_zip} | {n_periodos} períodos | orig={p_orig_avg:.2f} sub={p_sub_avg:.2f}', job_id, ch)

    return resultados, logs_total, nome_zip

# ============================================================================
# Main Worker Logic
# ============================================================================

def run_worker(job_id: str, data_inicio: str, data_fim: str, n_workers: int = 4):
    """Main worker entry point."""
    ch = None

    try:
        ch = get_ch()
        log('INFO', f'Iniciando job {job_id}', job_id, ch)
        log('INFO', f'Intervalo: {data_inicio} a {data_fim} | Workers: {n_workers}', job_id, ch)

        # Load configuration
        log('INFO', 'A carregar configuração...', job_id, ch)
        escaloes = carrega_escaloes()
        mapa = carrega_mapa_unidades()

        n_categorias_pre = len(escaloes.get('PRE', {}))
        n_excecoes = len(mapa.get('excecoes', {}))
        log('INFO', f'Configuração: {n_categorias_pre} categorias PRE, {n_excecoes} excepções', job_id, ch)

        # Find ZIP files
        zip_files = zip_files_no_intervalo(data_inicio, data_fim)
        if not zip_files:
            log('AVISO', 'Nenhum ficheiro ZIP encontrado no intervalo', job_id, ch)
            log('STATUS', 'DONE', job_id, ch)
            return True

        log('INFO', f'Encontrados {len(zip_files)} ficheiros ZIP', job_id, ch)

        # Process files
        all_resultados = []
        all_logs = []
        errors = []

        if n_workers <= 1:
            # Sequential processing
            for zip_path in zip_files:
                try:
                    resultados, logs, nome = processa_ficheiro(
                        zip_path, mapa, escaloes, job_id, ch
                    )
                    all_resultados.extend(resultados)
                    all_logs.extend(logs)
                except Exception as e:
                    errors.append(f'{os.path.basename(zip_path)}: {e}')
                    log('ERRO', f'Erro processando {os.path.basename(zip_path)}: {e}', job_id, ch)
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                futures = {
                    executor.submit(processa_ficheiro, zp, mapa, escaloes, job_id, None): zp
                    for zp in zip_files
                }

                for future in as_completed(futures):
                    zip_path = futures[future]
                    try:
                        resultados, logs, nome = future.result()
                        all_resultados.extend(resultados)
                        all_logs.extend(logs)

                        # Log progress (after file completes)
                        if resultados:
                            p_orig = np.mean([r['preco_clearing_orig'] for r in resultados if r['preco_clearing_orig']])
                            p_sub = np.mean([r['preco_clearing_sub'] for r in resultados if r['preco_clearing_sub']])
                            log('OK', f'{nome} | {len(resultados)} períodos | orig={p_orig:.2f} sub={p_sub:.2f}', job_id, ch)

                    except Exception as e:
                        nome = os.path.basename(zip_path)
                        errors.append(f'{nome}: {e}')
                        log('ERRO', f'Erro processando {nome}: {e}', job_id, ch)

        # Insert results into ClickHouse
        if all_resultados:
            log('INFO', f'A inserir {len(all_resultados)} resultados no ClickHouse...', job_id, ch)

            rows_clearing = []
            for r in all_resultados:
                rows_clearing.append({
                    'job_id': r['job_id'],
                    'data_ficheiro': r['data_ficheiro'],
                    'data_date': r['data_date'],
                    'hora_raw': r['hora_raw'],
                    'hora_num': r['hora_num'],
                    'pais': r['pais'],
                    'preco_clearing_orig': r['preco_clearing_orig'],
                    'volume_clearing_orig': r['volume_clearing_orig'],
                    'preco_clearing_sub': r['preco_clearing_sub'],
                    'volume_clearing_sub': r['volume_clearing_sub'],
                    'delta_preco': r['delta_preco'],
                    'n_bids_substituidos': r['n_bids_substituidos']
                })

            inserted = ch_insert_batch(ch, 'mibel.clearing_substituicao', rows_clearing)
            log('INFO', f'Inseridos {inserted} resultados em clearing_substituicao', job_id, ch)

        if all_logs:
            log('INFO', f'A inserir {len(all_logs)} logs de substituição...', job_id, ch)

            rows_logs = []
            for l in all_logs:
                rows_logs.append({
                    'job_id': l['job_id'],
                    'data_ficheiro': l['data_ficheiro'],
                    'data_date': l['data_date'],
                    'hora_raw': l['hora_raw'],
                    'hora_num': l['hora_num'],
                    'pais': l['pais'],
                    'unidade': l.get('unidade', ''),
                    'categoria': l.get('categoria', ''),
                    'escalao_preco': l.get('escalao_preco', 0),
                    'preco_original': l.get('preco_original', 0),
                    'energia_mw': l.get('energia_mw', 0)
                })

            inserted = ch_insert_batch(ch, 'mibel.clearing_substituicao_logs', rows_logs)
            log('INFO', f'Inseridos {inserted} logs em clearing_substituicao_logs', job_id, ch)

        # Summary
        total_bids_sub = sum(r['n_bids_substituidos'] for r in all_resultados)
        log('INFO', f'Total: {len(all_resultados)} períodos | {total_bids_sub} bids substituídos', job_id, ch)

        if errors:
            log('AVISO', f'{len(errors)} ficheiros com erros', job_id, ch)

        log('STATUS', 'DONE', job_id, ch)
        return True

    except Exception as e:
        error_msg = f'Erro fatal: {e}\n{traceback.format_exc()}'
        log('ERRO', error_msg, job_id, ch)
        log('STATUS', 'FAILED', job_id, ch)
        return False

    finally:
        if ch:
            try:
                ch.disconnect()
            except:
                pass

# ============================================================================
# CLI Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='MIBEL Substitution Worker')
    parser.add_argument('--job_id', required=True, help='Job UUID')
    parser.add_argument('--data_inicio', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--data_fim', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')

    args = parser.parse_args()

    # Validate dates
    try:
        date.fromisoformat(args.data_inicio)
        date.fromisoformat(args.data_fim)
    except ValueError as e:
        print(f'[ERRO] Invalid date format: {e}', flush=True)
        sys.exit(1)

    success = run_worker(
        job_id=args.job_id,
        data_inicio=args.data_inicio,
        data_fim=args.data_fim,
        n_workers=args.workers
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
