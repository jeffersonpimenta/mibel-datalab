#!/usr/bin/env python3
"""
MIBEL Platform - Optimization Worker

Processes OMIE bid files and calculates optimal PRE bidding strategies.
Results are stored as CSV files in /data/outputs/.

Usage:
    python otimizacao_worker.py --job_id JOB_ID --data_inicio YYYY-MM-DD --data_fim YYYY-MM-DD [--workers N]
"""

import argparse
import os
import sys
import zipfile
import traceback
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict

import pandas as pd
import numpy as np

# Add workers directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import (
    get_ch, ch_insert_batch, log,
    carrega_escaloes, carrega_mapa_unidades,
    zip_files_no_intervalo, extrai_data, normaliza_hora,
    get_categoria_zona, sufixo_de_pais, ensure_output_dir,
    calcula_clearing, OUTPUTS_DIR
)

# ============================================================================
# Constants
# ============================================================================

# Price step for optimization search (€/MWh)
PRICE_STEP = 1.0

# Price range for optimization
PRICE_MIN = 0.0
PRICE_MAX = 180.0

# ============================================================================
# Bid File Processing (shared with substituicao_worker)
# ============================================================================

def le_ficheiro_zip(zip_path: str) -> Optional[pd.DataFrame]:
    """Read bid data from OMIE ZIP file."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            bid_files = [f for f in zf.namelist() if f.endswith('.1') or f.endswith('.txt')]

            if not bid_files:
                return None

            dfs = []
            for fname in bid_files:
                with zf.open(fname) as f:
                    content = f.read().decode('latin-1')

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

# ============================================================================
# Optimization Logic
# ============================================================================

def calcula_lucro_pre(
    preco_clearing: float,
    volume_pre: float,
    preco_bid_pre: float
) -> float:
    """
    Calculate PRE profit.

    PRE profit = (clearing_price - bid_price) * accepted_volume
    When bid_price <= clearing_price, all volume is accepted.
    """
    if preco_clearing is None or preco_clearing < preco_bid_pre:
        return 0.0
    return (preco_clearing - preco_bid_pre) * volume_pre


def encontra_preco_otimo(
    ofertas_compra: List[tuple],
    ofertas_venda_nao_pre: List[tuple],
    volume_pre: float,
    preco_min: float = PRICE_MIN,
    preco_max: float = PRICE_MAX,
    step: float = PRICE_STEP
) -> Dict:
    """
    Find optimal PRE bid price that maximizes profit.

    Args:
        ofertas_compra: Buy offers [(price, volume), ...]
        ofertas_venda_nao_pre: Non-PRE sell offers
        volume_pre: Total PRE volume to bid
        preco_min: Minimum price to test
        preco_max: Maximum price to test
        step: Price step for search

    Returns:
        Dict with optimization results
    """
    if volume_pre <= 0:
        return {
            'preco_otimo': None,
            'lucro_otimo': 0,
            'preco_clearing_otimo': None,
            'volume_clearing_otimo': None
        }

    melhor_preco = preco_min
    melhor_lucro = 0
    melhor_clearing = None
    melhor_volume = None

    # Test each price level
    preco = preco_min
    while preco <= preco_max:
        # Add PRE bid at this price
        ofertas_venda_test = ofertas_venda_nao_pre + [(preco, volume_pre)]

        # Calculate clearing
        preco_clearing, volume_clearing = calcula_clearing(ofertas_compra, ofertas_venda_test)

        if preco_clearing is not None:
            # Calculate PRE profit
            lucro = calcula_lucro_pre(preco_clearing, volume_pre, preco)

            if lucro > melhor_lucro:
                melhor_lucro = lucro
                melhor_preco = preco
                melhor_clearing = preco_clearing
                melhor_volume = volume_clearing

        preco += step

    return {
        'preco_otimo': melhor_preco,
        'lucro_otimo': melhor_lucro,
        'preco_clearing_otimo': melhor_clearing,
        'volume_clearing_otimo': melhor_volume
    }


def processa_hora_otimizacao(
    df_hora: pd.DataFrame,
    hora_raw: str,
    hora_num: int,
    pais_filter: str,
    mapa: dict,
    escaloes: dict
) -> Dict:
    """
    Process optimization for a single hour and country.

    Returns dict with base and optimized results.
    """
    # Separate buy and sell offers
    df_compra = df_hora[df_hora['Tipo Oferta'] == 'C'].copy()
    df_venda = df_hora[df_hora['Tipo Oferta'] == 'V'].copy()

    ofertas_compra = list(zip(df_compra['Precio'], df_compra['Energia']))

    # Separate PRE and non-PRE sell offers
    ofertas_venda_pre = []
    ofertas_venda_nao_pre = []
    energia_pre_total = 0

    for _, row in df_venda.iterrows():
        unidade = row['Unidad']
        tecnologia = row.get('Tecnologia', '')
        pais = row['Pais']

        regime, cat_zona = get_categoria_zona(unidade, tecnologia, pais, mapa, escaloes)

        if regime == 'PRE':
            ofertas_venda_pre.append((row['Precio'], row['Energia']))
            energia_pre_total += row['Energia']
        else:
            ofertas_venda_nao_pre.append((row['Precio'], row['Energia']))

    # Calculate base clearing (with original PRE bids)
    ofertas_venda_orig = ofertas_venda_pre + ofertas_venda_nao_pre
    preco_base, volume_base = calcula_clearing(ofertas_compra, ofertas_venda_orig)

    # Calculate base PRE profit
    lucro_pre_base = 0
    if preco_base is not None:
        for preco_bid, vol in ofertas_venda_pre:
            lucro_pre_base += calcula_lucro_pre(preco_base, vol, preco_bid)

    # Find optimal PRE strategy
    otimo = encontra_preco_otimo(
        ofertas_compra,
        ofertas_venda_nao_pre,
        energia_pre_total
    )

    return {
        'hora_raw': hora_raw,
        'hora_num': hora_num,
        'pais': pais_filter,
        'energia_pre_total': energia_pre_total,
        'n_bids_pre': len(ofertas_venda_pre),
        'preco_clearing_base': preco_base,
        'volume_clearing_base': volume_base,
        'lucro_pre_base': lucro_pre_base,
        'preco_bid_otimo': otimo['preco_otimo'],
        'preco_clearing_otimo': otimo['preco_clearing_otimo'],
        'volume_clearing_otimo': otimo['volume_clearing_otimo'],
        'lucro_pre_otimo': otimo['lucro_otimo'],
        'delta_lucro': otimo['lucro_otimo'] - lucro_pre_base if lucro_pre_base else otimo['lucro_otimo']
    }


def processa_ficheiro_otimizacao(
    zip_path: str,
    mapa: dict,
    escaloes: dict,
    job_id: str
) -> tuple:
    """
    Process a single ZIP file for optimization.

    Returns:
        (resultados_list, nome_ficheiro)
    """
    nome_zip = os.path.basename(zip_path)
    data_ficheiro = extrai_data(nome_zip)

    df = le_ficheiro_zip(zip_path)
    if df is None or df.empty:
        return [], nome_zip

    resultados = []

    # Process each country
    for pais in ['ES', 'PT']:
        df_pais = df[df['Pais'] == pais]
        if df_pais.empty:
            continue

        # Process each hour
        for hora_val in df_pais['Hora'].unique():
            df_hora = df_pais[df_pais['Hora'] == hora_val]
            hora_raw, hora_num, _ = normaliza_hora(hora_val)

            try:
                resultado = processa_hora_otimizacao(
                    df_hora, hora_raw, hora_num, pais, mapa, escaloes
                )

                resultado['job_id'] = job_id
                resultado['data_ficheiro'] = data_ficheiro
                resultado['data_date'] = data_ficheiro

                resultados.append(resultado)

            except Exception as e:
                print(f'[ERRO] {nome_zip} | Hora {hora_val} Pais {pais}: {e}', flush=True)

    return resultados, nome_zip

# ============================================================================
# Main Worker Logic
# ============================================================================

def run_worker(job_id: str, data_inicio: str, data_fim: str, n_workers: int = 4):
    """Main optimization worker entry point."""
    ch = None

    try:
        ch = get_ch()
        log('INFO', f'Iniciando job optimização {job_id}', job_id, ch)
        log('INFO', f'Intervalo: {data_inicio} a {data_fim} | Workers: {n_workers}', job_id, ch)

        # Load configuration
        log('INFO', 'A carregar configuração...', job_id, ch)
        escaloes = carrega_escaloes()
        mapa = carrega_mapa_unidades()

        # Find ZIP files
        zip_files = zip_files_no_intervalo(data_inicio, data_fim)
        if not zip_files:
            log('AVISO', 'Nenhum ficheiro ZIP encontrado no intervalo', job_id, ch)
            log('STATUS', 'DONE', job_id, ch)
            return True

        log('INFO', f'Encontrados {len(zip_files)} ficheiros ZIP', job_id, ch)

        # Process files
        all_resultados = []
        errors = []

        if n_workers <= 1:
            # Sequential processing
            for zip_path in zip_files:
                try:
                    resultados, nome = processa_ficheiro_otimizacao(
                        zip_path, mapa, escaloes, job_id
                    )
                    all_resultados.extend(resultados)

                    if resultados:
                        lucro_base = sum(r['lucro_pre_base'] for r in resultados)
                        lucro_opt = sum(r['lucro_pre_otimo'] for r in resultados)
                        log('OK', f'{nome} | {len(resultados)} períodos | lucro_base={lucro_base:.0f} lucro_opt={lucro_opt:.0f}', job_id, ch)

                except Exception as e:
                    errors.append(f'{os.path.basename(zip_path)}: {e}')
                    log('ERRO', f'Erro processando {os.path.basename(zip_path)}: {e}', job_id, ch)
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                futures = {
                    executor.submit(processa_ficheiro_otimizacao, zp, mapa, escaloes, job_id): zp
                    for zp in zip_files
                }

                for future in as_completed(futures):
                    zip_path = futures[future]
                    try:
                        resultados, nome = future.result()
                        all_resultados.extend(resultados)

                        if resultados:
                            lucro_base = sum(r['lucro_pre_base'] for r in resultados)
                            lucro_opt = sum(r['lucro_pre_otimo'] for r in resultados)
                            log('OK', f'{nome} | {len(resultados)} períodos | lucro_base={lucro_base:.0f} lucro_opt={lucro_opt:.0f}', job_id, ch)

                    except Exception as e:
                        nome = os.path.basename(zip_path)
                        errors.append(f'{nome}: {e}')
                        log('ERRO', f'Erro processando {nome}: {e}', job_id, ch)

        # Save results to CSV
        if all_resultados:
            ensure_output_dir()
            output_path = os.path.join(OUTPUTS_DIR, f'{job_id}_otimizacao.csv')

            df_result = pd.DataFrame(all_resultados)

            # Reorder columns
            columns_order = [
                'job_id', 'data_ficheiro', 'data_date', 'hora_raw', 'hora_num', 'pais',
                'energia_pre_total', 'n_bids_pre',
                'preco_clearing_base', 'volume_clearing_base', 'lucro_pre_base',
                'preco_bid_otimo', 'preco_clearing_otimo', 'volume_clearing_otimo', 'lucro_pre_otimo',
                'delta_lucro'
            ]
            df_result = df_result[[c for c in columns_order if c in df_result.columns]]

            df_result.to_csv(output_path, index=False)
            log('INFO', f'Resultados guardados em {output_path}', job_id, ch)

            # Summary statistics
            total_lucro_base = df_result['lucro_pre_base'].sum()
            total_lucro_opt = df_result['lucro_pre_otimo'].sum()
            total_delta = df_result['delta_lucro'].sum()

            log('INFO', f'Resumo: lucro_base={total_lucro_base:.0f}€ lucro_opt={total_lucro_opt:.0f}€ delta={total_delta:.0f}€', job_id, ch)

        # Summary
        log('INFO', f'Total: {len(all_resultados)} períodos processados', job_id, ch)

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
    parser = argparse.ArgumentParser(description='MIBEL Optimization Worker')
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
