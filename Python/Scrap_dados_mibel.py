#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import zipfile
import requests
import mysql.connector
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

# ----------------- MySQL -----------------
MYSQL_HOST = "localhost"
MYSQL_DB = "mibel"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"

# ----------------- FUNÇÕES -----------------
def get_mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

def insert_records(records):
    if not records:
        print("[INFO] Nenhum registo para inserir.")
        return
    conn = get_mysql_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS curvas_mercado (
            id INT AUTO_INCREMENT PRIMARY KEY,
            periodo INT NOT NULL,
            data DATE NOT NULL,
            pais VARCHAR(2) NOT NULL,
            tipo_oferta VARCHAR(1) NOT NULL,
            volume DOUBLE NOT NULL,
            preco DOUBLE NOT NULL,
            status VARCHAR(1),
            tipologia VARCHAR(20),
            arquivo VARCHAR(255)
        )
    """)
    sql = """
        INSERT INTO curvas_mercado
        (periodo, data, pais, tipo_oferta, volume, preco, status, tipologia, arquivo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(sql, records)
    conn.commit()
    cur.close()
    conn.close()
    print(f"[MySQL] Inseridos {len(records)} registos do ficheiro {records[0][-1]}.")

def parse_file(raw, filename):
    text = raw.decode("latin-1", errors="ignore")
    lines = text.splitlines()
    reader = csv.reader(lines, delimiter=";")
    records = []

    for row in reader:
        # ignora headers, linhas vazias e linhas muito curtas
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

            # se ainda sobrar coluna extra por conta de ";" no final
            if not tipologia and len(row) > 9:
                tipologia = row[9].strip()

            records.append((periodo, data, pais, tipo_oferta, volume, preco, status, tipologia, filename))
        except Exception:
            # ignora linha ruim sem poluir a tela
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
        # arquivo .1
        records = parse_file(r.content, filename)
        insert_records(records)

# ----------------- MAIN -----------------
def main():
    print("=== Iniciando processamento das curvas do OMIE 2018-2025 ===")
    for f in CURVA_PBC_FILES:
        download_and_process_file(f)
    print("=== Processamento concluído ===")

if __name__ == "__main__":
    main()
