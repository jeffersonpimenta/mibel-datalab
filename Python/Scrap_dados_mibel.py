#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import zipfile
import requests
import clickhouse_connect
from datetime import datetime, timedelta, date
import csv
import calendar

# ----------------- CONFIGURAÇÃO -----------------
OMIE_BASE = "https://www.omie.es/en/file-download"

# ZIPs históricos 2018 a 2022
zip_files = [f"curva_pbc_{year}.zip" for year in range(2018, 2023)]

# Ficheiros .1 diários 2023 a 2025
def generate_one_files(year):
    days_in_year = 366 if calendar.isleap(year) else 365
    return [
        f"curva_pbc_{(date(year, 1, 1) + timedelta(days=i)).strftime('%Y%m%d')}.1"
        for i in range(days_in_year)
    ]

one_files = []
for year in range(2023, 2026):
    one_files.extend(generate_one_files(year))

CURVA_PBC_FILES = zip_files + one_files

# ----------------- ClickHouse -----------------
client = clickhouse_connect.get_client(
    host='localhost',
    username='default',
    password='',
    database='default'
)

TABLE_NAME = "ofertas"

# ----------------- FUNÇÕES -----------------
def create_table_if_not_exists():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        periodo UInt8,
        data Date,
        pais String,
        tipo_oferta String,
        volume Float64,
        preco Float64,
        status String,
        tipologia String,
        arquivo String
    )
    ENGINE = MergeTree()
    ORDER BY (data, periodo)
    """
    client.command(ddl)
    print("[ClickHouse] Tabela verificada/criada com sucesso.")

def insert_records(records):
    if not records:
        print("[INFO] Nenhum registo para inserir.")
        return
    client.insert(
        table=TABLE_NAME,
        data=records,
        column_names=[
            "periodo", "data", "pais", "tipo_oferta", "volume",
            "preco", "status", "tipologia", "arquivo"
        ]
    )
    print(f"[ClickHouse] Inseridos {len(records)} registos do ficheiro {records[0][-1]}.")

def parse_file(raw, filename):
    text = raw.decode("latin-1", errors="ignore")
    lines = text.splitlines()
    reader = csv.reader(lines, delimiter=";")
    records = []

    for row in reader:
        if not row or len(row) < 8 or row[0].startswith("OMIE") or row[0].startswith("Periodo"):
            continue
        try:
            periodo = int(row[0])
            data = datetime.strptime(row[1], "%d/%m/%Y").date()
            pais = row[2].strip()
            tipo_oferta = row[4].strip() if row[4].strip() in ['C', 'V'] else 'C'
            volume = float(row[5].replace(".", "").replace(",", "."))
            preco = float(row[6].replace(".", "").replace(",", "."))
            status = row[7].strip() if len(row) > 7 else ''
            tipologia = row[8].strip() if len(row) > 8 else ''
            if not tipologia and len(row) > 9:
                tipologia = row[9].strip()

            records.append((periodo, data, pais, tipo_oferta, volume, preco, status, tipologia, filename))
        except Exception:
            continue

    print(f"[PARSE] {len(records)} registos extraídos de {filename}")
    return records

def download_and_process_file(filename):
    url = f"{OMIE_BASE}?filename={filename}&parents=curva_pbc"
    headers = {"User-Agent": "Mozilla/5.0"}
    print(f"[DOWNLOAD] Iniciando download: {filename}")
    r = requests.get(url, headers=headers)
    if r.status_code != 200 or not r.content:
        print(f"[INFO] {filename} não encontrado ou erro no download.")
        return
    print(f"[DOWNLOAD] {filename} concluído")

    if filename.endswith(".zip"):
        print(f"[ZIP] Processando ZIP em memória: {filename}")
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            for name in zf.namelist():
                if not name.endswith(".1"):
                    print(f"[SKIP] {name} (não é .1)")
                    continue
                try:
                    raw = zf.read(name)
                    records = parse_file(raw, name)
                    insert_records(records)
                except Exception as e:
                    print(f"[ERRO] {name} dentro do ZIP {filename}: {e}")
    else:
        records = parse_file(r.content, filename)
        insert_records(records)

# ----------------- MAIN -----------------
def main():
    print("=== Iniciando processamento das curvas do OMIE 2018-2025 ===")
    create_table_if_not_exists()
    for f in CURVA_PBC_FILES:
        download_and_process_file(f)
    print("=== Processamento concluído ===")

if __name__ == "__main__":
    main()
