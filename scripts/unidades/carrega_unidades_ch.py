#!/usr/bin/env python3
"""
carrega_unidades_ch.py
──────────────────────
Lê LISTA_UNIDADES.csv (via classificacao_pre.py), classifica cada unidade
e insere / actualiza a tabela mibel.unidades no ClickHouse.

A tabela usa ReplacingMergeTree(atualizado_em) para que re-execuções
actualizem as linhas existentes sem duplicados.

Uso:
    python carrega_unidades_ch.py [--csv LISTA_UNIDADES.csv]
                                  [--host clickhouse] [--port 9000]
                                  [--dry-run]
"""

import argparse
import os
import sys
from datetime import datetime

# classificacao_pre.py está na mesma directoria
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from classificacao_pre import carrega_e_classifica, resumo

from clickhouse_driver import Client


def get_ch(host: str, port: int) -> Client:
    return Client(
        host=host,
        port=port,
        database='mibel',
        settings={'use_numpy': False},
    )


def carrega_e_insere(
    path_csv: str,
    ch: Client,
    dry_run: bool = False,
) -> None:
    print(f"A classificar unidades de: {os.path.abspath(path_csv)}")
    df = carrega_e_classifica(path_csv)
    resumo(df)

    ts = datetime.now()

    rows = [
        {
            'codigo':        str(row.CODIGO).strip(),
            'descricao':     str(row.DESCRIPCION).strip(),
            'agente':        str(row.AGENTE_PROPIETARIO).strip(),
            'tipo_unidad':   str(row.TIPO_UNIDAD).strip(),
            'zona_frontera': str(row.ZONA_FRONTERA).strip(),
            'tecnologia':    str(row.TECNOLOGIA).strip(),
            'regime':        str(row.regime).strip(),
            'categoria':     str(row.categoria).strip(),
            'atualizado_em': ts,
        }
        for row in df.itertuples(index=False)
    ]

    print(f"Total de unidades a inserir/actualizar: {len(rows)}")

    if dry_run:
        print("[DRY-RUN] Nenhuma inserção efectuada.")
        for r in rows[:10]:
            print(f"  {r['codigo']:<12} {r['regime']:<15} {r['categoria']}")
        if len(rows) > 10:
            print(f"  ... (+{len(rows) - 10} mais)")
        return

    # INSERT em lote — ReplacingMergeTree elimina duplicados por (codigo)
    batch_size = 2000
    total = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        ch.execute(
            'INSERT INTO mibel.unidades '
            '(codigo, descricao, agente, tipo_unidad, zona_frontera, '
            ' tecnologia, regime, categoria, atualizado_em) VALUES',
            batch,
        )
        total += len(batch)
        print(f"  Inseridos {total}/{len(rows)}…")

    print(f"Concluído — {total} linhas inseridas/actualizadas em mibel.unidades.")

    # Forçar merge para que ReplacingMergeTree consolide duplicados
    try:
        ch.execute('OPTIMIZE TABLE mibel.unidades FINAL')
        print("OPTIMIZE TABLE executado.")
    except Exception as e:
        print(f"[AVISO] OPTIMIZE TABLE falhou (não crítico): {e}")


def main():
    parser = argparse.ArgumentParser(description='Carrega LISTA_UNIDADES.csv → mibel.unidades (ClickHouse)')
    parser.add_argument('--csv',      default='./LISTA_UNIDADES.csv', help='Caminho para LISTA_UNIDADES.csv')
    parser.add_argument('--host',     default=os.getenv('CLICKHOUSE_HOST', 'clickhouse'))
    parser.add_argument('--port',     type=int, default=int(os.getenv('CLICKHOUSE_PORT', '9000')))
    parser.add_argument('--dry-run',  action='store_true', help='Classifica mas não insere no CH')
    args = parser.parse_args()

    if not args.dry_run:
        ch = get_ch(args.host, args.port)
    else:
        ch = None

    carrega_e_insere(args.csv, ch, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
