"""
classificacao_pre.py
────────────────────
Lê LISTA_UNIDADES.csv (OMEL) e produz dois artefactos:

  1. unidades_classificadas.csv
     Consumido por carrega_unidades() em clearing_substituicao_multithread.py.
     Colunas: CODIGO ; TIPO_UNIDAD ; TECNOLOGIA ; regime ; categoria

     A coluna "categoria" inclui um sufixo de zona geográfica:
       _ES  → ZONA ESPAÑOLA
       _PT  → ZONA PORTUGUESA
       _EXT → zonas de fronteira (FRONTERA FRANCIA, FRONTERA MARRUECOS, etc.)

     Excepção: categorias já inerentemente zonais (COMERC_EXT, CONTRATO_INT)
     não recebem sufixo adicional.

     Exemplos:
       SOLAR_FOT_ES, SOLAR_FOT_PT, CICLO_COMBINADO_ES, COMERC_PT, COMERC_EXT

  2. parametros_modelo.py
     Dicionário PARAMETROS editável com:
       • PRE  → escala de volume + escalões de preço/pct_bids por categoria_zona
       • PRO  → escala de volume por tecnologia_zona convencional
       • CONSUMO / COMERCIALIZADOR / GENERICA / PORFOLIO → escala por tecnologia_zona

Classificação baseada na inspecção real do ficheiro OMEL Março 2026.
"""

import os
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
#  EXCEPÇÕES POR UNIDADE  (CODIGO → categoria_zona override)
#  Unidades listadas aqui ignoram a classificação automática por TECNOLOGIA
#  e são atribuídas directamente à categoria_zona indicada.
# ─────────────────────────────────────────────────────────────────────────────
EXCECOES_UNIDADE: dict[str, str] = {
    # SOLAR_FOT_PT
    "ACCGV02": "SOLAR_FOT_PT",
    "AGPRCTI": "SOLAR_FOT_PT",
    "AGPRSUL": "SOLAR_FOT_PT",
    "EGLEV2":  "SOLAR_FOT_PT",
    "IGNIV02": "SOLAR_FOT_PT",
    "JAFPV02": "SOLAR_FOT_PT",
    # EOLICA_PT
    "AAXRSUL": "EOLICA_PT",
    "EDPGPV2": "EOLICA_PT",
    "GRENV02": "EOLICA_PT",
    "INATV02": "EOLICA_PT",
    "LEZIV02": "EOLICA_PT",
    "MUONV02": "EOLICA_PT",
    "TSRDV02": "EOLICA_PT",
}

# ─────────────────────────────────────────────────────────────────────────────
#  MAPA DE CLASSIFICAÇÃO  (TECNOLOGIA exacta → regime, categoria)
#  Cobre todas as 55 combinações TIPO_UNIDAD × TECNOLOGIA observadas no CSV.
# ─────────────────────────────────────────────────────────────────────────────
MAPA_TECNOLOGIA: dict[str, tuple[str, str]] = {

    # ── PRE — Regime Especial (geração renovável e cogeração) ─────────────────
    "RE Mercado Solar Fotovoltáica":  ("PRE", "SOLAR_FOT"),
    "RE Mercado Solar Térmica":       ("PRE", "SOLAR_TER"),
    "RE Mercado Eólica":              ("PRE", "EOLICA"),
    "RE Mercado Eólica Marina":       ("PRE", "EOLICA_MARINA"),
    "RE Mercado Hidráulica":          ("PRE", "HIDRICA"),
    "RE Mercado Térmica Renovable":   ("PRE", "TERMICA_RENOV"),
    "RE Mercado Térmica no Renovab.": ("PRE", "TERMICA_NREN"),
    "RE Mercado Geotérmica":          ("PRE", "GEOTERMICA"),
    "Híbrida Renovable":              ("PRE", "HIBRIDA_RENOV"),
    "Híbrida Renov.-Almacenamiento":  ("PRE", "HIBRIDA_RENOV"),
    "Híbrida Renov.-Térmica":         ("PRE", "HIBRIDA_RENOV"),
    # Tarifa CUR — ainda em regime especial, preço regulado
    "RE Tarifa CUR (uof)":            ("PRE", "RE_TARIFA_CUR"),
    "RE Tar. CUR Eólica":             ("PRE", "RE_TARIFA_CUR"),
    "RE Tar. CUR Solar Fotovoltáica": ("PRE", "RE_TARIFA_CUR"),
    "RE Tar. CUR Solar Térmica":      ("PRE", "RE_TARIFA_CUR"),
    "RE Tar. CUR Hidráulica":         ("PRE", "RE_TARIFA_CUR"),
    "RE Tar. CUR Térmica Renovable":  ("PRE", "RE_TARIFA_CUR"),
    "RE Tar. CUR Térmica no Renov.":  ("PRE", "RE_TARIFA_CUR"),
    # Agente que oferta em nome de unidades RE
    "Agente vendedor Reg. Especial":  ("PRE", "RE_OUTRO"),
    # Armazenamento acoplado a RE — oferta de venda (descarga)
    "Almacenamiento Venta":           ("PRE", "ARMAZENAMENTO_VENDA"),

    # ── PRO — Geração Convencional (Regime Ordinário) ─────────────────────────
    "Ciclo Combinado":                ("PRO", "CICLO_COMBINADO"),
    "Nuclear":                        ("PRO", "NUCLEAR"),
    "Hidráulica Generación":          ("PRO", "HIDRICA_PRO"),
    "Hidráulica de Bombeo Puro":      ("PRO", "BOMBEO_PURO_PRO"),
    "Hulla Antracita":                ("PRO", "CARVAO"),
    "Carbón de Importación":          ("PRO", "CARVAO"),
    "Gas":                            ("PRO", "GAS"),

    # ── CONSUMO — Bombagem (modo consumo) ─────────────────────────────────────
    "Consumo Bombeo Mixto":           ("CONSUMO", "BOMBEO_CONSUMO"),
    "Consumo Bombeo Puro":            ("CONSUMO", "BOMBEO_CONSUMO"),
    "Consumo de bombeo":              ("CONSUMO", "BOMBEO_CONSUMO"),

    # ── CONSUMO — Armazenamento (modo carga) ──────────────────────────────────
    "Almacenamiento Compra":          ("CONSUMO", "ARMAZENAMENTO_COMPRA"),

    # ── CONSUMO — Consumidores directos e auxiliares ──────────────────────────
    "Compras Consumo Directo":        ("CONSUMO", "CONS_DIRECTO"),
    "Consumidor directo":             ("CONSUMO", "CONS_DIRECTO"),
    "Compras Cons. Directo Balance":  ("CONSUMO", "CONS_DIRECTO"),
    "Rep. de consumidores directos":  ("CONSUMO", "CONS_DIRECTO"),
    "Compras Consumos Auxiliares":    ("CONSUMO", "CONS_AUXILIARES"),
    "Rep. Consumos Auxiliares":       ("CONSUMO", "CONS_AUXILIARES"),
    "Consumo de productores":         ("CONSUMO", "CONS_PRODUTOR"),

    # ── COMERCIALIZADOR ───────────────────────────────────────────────────────
    "Comercializador":                ("COMERCIALIZADOR", "COMERC"),
    "Comercializador no residente":   ("COMERCIALIZADOR", "COMERC_NR"),
    "Compras Comercialización":       ("COMERCIALIZADOR", "COMERC"),
    "Compra Comercializador Balance": ("COMERCIALIZADOR", "COMERC"),
    "Import. de agentes externos":    ("COMERCIALIZADOR", "COMERC_EXT"),
    "Import. de comercializadoras":   ("COMERCIALIZADOR", "COMERC_EXT"),
    "Rep. de comercializadores":      ("COMERCIALIZADOR", "COMERC"),
    "Comercializador ultimo recurso": ("COMERCIALIZADOR", "COMERC_ULT_REC"),

    # ── GENÉRICA ──────────────────────────────────────────────────────────────
    "Unidad Generica":                ("GENERICA", "GENERICA"),
    "VENTA GENERICA":                 ("GENERICA", "GENERICA_VENDA"),

    # ── PORTFOLIO ─────────────────────────────────────────────────────────────
    "Porfolio Produccion Compra":     ("PORFOLIO", "PORTF_PROD"),
    "Porfolio Produccion Venta":      ("PORFOLIO", "PORTF_PROD"),
    "Porfolio Comerc. Compra":        ("PORFOLIO", "PORTF_COMERC"),
    "Porfolio Comerc. Venta":         ("PORFOLIO", "PORTF_COMERC"),
}


# ─────────────────────────────────────────────────────────────────────────────
#  MAPEAMENTO ZONA_FRONTERA → SUFIXO DE ZONA
# ─────────────────────────────────────────────────────────────────────────────

def _zona_sufixo(zona_frontera: str) -> str:
    """
    Converte o valor de ZONA_FRONTERA no sufixo de zona a anexar à categoria.

      ZONA PORTUGUESA          → PT
      ZONA ESPAÑOLA            → ES
      FRONTERA FRANCIA /
      FRONTERA MARRUECOS /
      FRONTERA ANDORRA / ...   → EXT
    """
    z = str(zona_frontera).strip().upper()
    if "PORTUG" in z:
        return "PT"
    if "ESPA" in z:
        return "ES"
    return "EXT"


def _categoria_zona(categoria: str, zona: str) -> str:
    """
    Constrói a categoria com sufixo de zona.

    Categorias que são já inerentemente zonais — COMERC_EXT (fronteiras
    externas) e CONTRATO_INT (contratos internacionais) — não recebem
    sufixo adicional para evitar nomes redundantes como COMERC_EXT_EXT.
    """
    if categoria in ("COMERC_EXT", "CONTRATO_INT"):
        return categoria
    return f"{categoria}_{zona}"

def carrega_e_classifica(path_csv: str) -> pd.DataFrame:
    """
    Lê LISTA_UNIDADES.csv e devolve DataFrame classificado com colunas:
      CODIGO | TIPO_UNIDAD | TECNOLOGIA | regime | categoria

    A coluna "categoria" inclui o sufixo de zona (_ES, _PT, _EXT), excepto
    para categorias já inerentemente zonais (COMERC_EXT, CONTRATO_INT).
    """
    df_raw = pd.read_csv(path_csv, sep=";", encoding="latin-1", header=None, dtype=str)

    hdr_row = next(
        i for i, row in df_raw.iterrows()
        if row.astype(str).str.upper().str.contains("CODIGO").any()
        and row.astype(str).str.upper().str.contains("TIPO").any()
    )

    df = df_raw.iloc[hdr_row + 1:, :7].copy()
    df.columns = [
        "CODIGO", "DESCRIPCION", "AGENTE_PROPIETARIO",
        "PORCENTAJE_PROPIEDAD", "TIPO_UNIDAD", "ZONA_FRONTERA", "TECNOLOGIA"
    ]

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    df = df[df["CODIGO"].str.upper() != "NAN"].reset_index(drop=True)
    df = df[df["CODIGO"] != ""].reset_index(drop=True)

    # Classificação por TECNOLOGIA; fallback por TIPO_UNIDAD para linhas sem tecnologia
    FALLBACK_TIPO: dict[str, tuple[str, str]] = {
        "CONTRATO INTERNACIONAL": ("CONTRATO_INT", "CONTRATO_INT"),
        "DISTRIBUIDOR":           ("COMERCIALIZADOR", "COMERC_ULT_REC"),
    }

    def _classifica(row):
        tecn = row["TECNOLOGIA"]
        if tecn in MAPA_TECNOLOGIA:
            return pd.Series(MAPA_TECNOLOGIA[tecn])
        tipo = row["TIPO_UNIDAD"]
        if tipo in FALLBACK_TIPO:
            return pd.Series(FALLBACK_TIPO[tipo])
        return pd.Series(("OUTRO", "NAO_CLASSIFICADO"))

    df[["regime", "_cat_base"]] = df.apply(_classifica, axis=1)

    # Aplicar sufixo de zona à categoria
    df["zona"]      = df["ZONA_FRONTERA"].apply(_zona_sufixo)
    df["categoria"] = df.apply(
        lambda r: _categoria_zona(r["_cat_base"], r["zona"]), axis=1
    )
    df = df.drop(columns=["_cat_base", "zona"])

    # Aplicar excepções por unidade (CODIGO override)
    _MAPA_CAT_REGIME: dict[str, str] = {
        cat_zona: regime
        for regime, cats in {
            "PRE":             [
                "SOLAR_FOT_ES", "SOLAR_FOT_PT", "SOLAR_TER_ES", "SOLAR_TER_PT",
                "EOLICA_ES", "EOLICA_PT", "EOLICA_MARINA_ES", "EOLICA_MARINA_PT",
                "HIDRICA_ES", "HIDRICA_PT", "TERMICA_RENOV_ES", "TERMICA_RENOV_PT",
                "TERMICA_NREN_ES", "TERMICA_NREN_PT", "GEOTERMICA_ES", "GEOTERMICA_PT",
                "HIBRIDA_RENOV_ES", "HIBRIDA_RENOV_PT",
                "RE_TARIFA_CUR_ES", "RE_TARIFA_CUR_PT",
                "RE_OUTRO_ES", "RE_OUTRO_PT",
                "ARMAZENAMENTO_VENDA_ES", "ARMAZENAMENTO_VENDA_PT",
            ],
            "PRO":             ["CICLO_COMBINADO_ES", "CICLO_COMBINADO_PT",
                                "NUCLEAR_ES", "HIDRICA_PRO_ES", "BOMBEO_PURO_PRO_ES",
                                "CARVAO_ES", "GAS_ES"],
            "CONSUMO":         ["BOMBEO_CONSUMO_ES", "BOMBEO_CONSUMO_PT",
                                "ARMAZENAMENTO_COMPRA_ES", "ARMAZENAMENTO_COMPRA_PT",
                                "CONS_DIRECTO_ES", "CONS_DIRECTO_PT", "CONS_DIRECTO_EXT",
                                "CONS_AUXILIARES_ES", "CONS_AUXILIARES_PT",
                                "CONS_PRODUTOR_ES", "CONS_PRODUTOR_PT"],
            "COMERCIALIZADOR": ["COMERC_ES", "COMERC_PT", "COMERC_EXT",
                                "COMERC_NR_ES", "COMERC_NR_PT",
                                "COMERC_ULT_REC_ES", "COMERC_ULT_REC_PT"],
            "GENERICA":        ["GENERICA_ES", "GENERICA_PT",
                                "GENERICA_VENDA_ES", "GENERICA_VENDA_PT"],
            "PORFOLIO":        ["PORTF_PROD_ES", "PORTF_PROD_PT",
                                "PORTF_COMERC_ES", "PORTF_COMERC_PT"],
        }.items()
        for cat_zona in cats
    }

    for codigo, cat_zona_override in EXCECOES_UNIDADE.items():
        mask = df["CODIGO"] == codigo
        if mask.any():
            regime_override = _MAPA_CAT_REGIME.get(cat_zona_override, "PRE")
            df.loc[mask, "categoria"] = cat_zona_override
            df.loc[mask, "regime"]    = regime_override

    return df


def resumo(df: pd.DataFrame) -> None:
    total = len(df)
    print(f"\n{'='*62}")
    print(f"  CLASSIFICAÇÃO DE UNIDADES OMEL — {total} unidades")
    print(f"{'='*62}")
    for regime, grp in df.groupby("regime", sort=False):
        n = len(grp)
        print(f"  {regime:<20}: {n:>5}  ({n/total*100:.1f}%)")
        for cat, cgrp in grp.groupby("categoria", sort=False):
            print(f"      {cat:<28}: {len(cgrp):>5}")
    print(f"{'='*62}\n")


# ─────────────────────────────────────────────────────────────────────────────
#  GERAÇÃO DO CSV
# ─────────────────────────────────────────────────────────────────────────────

def gera_csv(df: pd.DataFrame, path_saida: str) -> None:
    """
    Grava unidades_classificadas.csv consumido por carrega_unidades_pre().
    Colunas mínimas necessárias pelo pipeline + informação de suporte.
    """
    colunas = [
        "CODIGO", "TIPO_UNIDAD", "TECNOLOGIA", "regime", "categoria",
        "DESCRIPCION", "AGENTE_PROPIETARIO", "ZONA_FRONTERA"
    ]
    df[colunas].to_csv(
        path_saida, sep=";", index=False, encoding="utf-8-sig", decimal=","
    )
    print(f"CSV guardado → {os.path.abspath(path_saida)}  ({len(df)} linhas)")


# ─────────────────────────────────────────────────────────────────────────────
#  GERAÇÃO DO FICHEIRO DE PARÂMETROS EDITÁVEL
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
#  GERAÇÃO DO FICHEIRO DE PARÂMETROS EDITÁVEL
# ─────────────────────────────────────────────────────────────────────────────

# Parâmetros padrão por categoria (sem sufixo de zona).
# Inclui escala e escalões para PRE, e apenas escala para os restantes regimes.
ESCALOES: dict = {
    "PRE": {
        "SOLAR_FOT_ES": {
            "escala": 2.2297,
            "escaloes": [
                {"preco": 0.0,  "pct_bids": 0.30},
                {"preco": 20.0, "pct_bids": 0.30},
                {"preco": 35.0, "pct_bids": 0.40},
            ],
        },
        "SOLAR_FOT_PT": {
            "escala": 3.6879,
            "escaloes": [
                {"preco": 0.0,  "pct_bids": 0.30},
                {"preco": 20.0, "pct_bids": 0.30},
                {"preco": 35.0, "pct_bids": 0.40},
            ],
        },
        "SOLAR_TER_ES": {
            "escala": 2.0869,
            "escaloes": [
                {"preco": 40.0, "pct_bids": 1.00},
            ],
        },
        "EOLICA_ES": {
            "escala": 1.8857,
            "escaloes": [
                {"preco": 50.0, "pct_bids": 0.50},
                {"preco": 70.0, "pct_bids": 0.50},
            ],
        },
        "EOLICA_PT": {
            "escala": 2.1379,
            "escaloes": [
                {"preco": 50.0, "pct_bids": 0.50},
                {"preco": 70.0, "pct_bids": 0.50},
            ],
        },
        "TERMICA_RENOV_ES": {
            "escala": 1.4,
            "escaloes": [
                {"preco": 10.0, "pct_bids": 1.00},
            ],
        },
        "TERMICA_RENOV_PT": {
            "escala": 1.5348,
            "escaloes": [
                {"preco": 10.0, "pct_bids": 1.00},
            ],
        },
    },
    "PRO": {
        "CICLO_COMBINADO_PT": {"escala": 0.7143},
        "NUCLEAR_ES":         {"escala": 0.4470},
    },
    "CONSUMO": {
        "BOMBEO_CONSUMO_ES":   {"escala": 1.6926},
        "BOMBEO_CONSUMO_PT":   {"escala": 1.3871},
        "CONS_AUXILIARES_ES":  {"escala": 1.0},
        "CONS_DIRECTO_ES":     {"escala": 1.6926},
        "CONS_DIRECTO_EXT":    {"escala": 1.6926},
    },
    "COMERCIALIZADOR": {
        "COMERC_ES": {"escala": 1.3871},
        "COMERC_PT": {"escala": 1.6926},
    },
    "GENERICA": {
        "GENERICA_ES":       {"escala": 1.3871},
        "GENERICA_PT":       {"escala": 1.6926},
        "GENERICA_VENDA_ES": {"escala": 1.3871},
        "GENERICA_VENDA_PT": {"escala": 1.6926},
    },
    "PORFOLIO": {
        "PORTF_COMERC_ES": {"escala": 1.0},
        "PORTF_PROD_ES":   {"escala": 1.0},
        "PORTF_PROD_PT":   {"escala": 1.0},
    },
}

# Escalões base PRE por categoria_zona exacta (com sufixo de zona).
# Usado internamente para lookup durante geração do ficheiro de parâmetros.
_ESCALOES_PRE_BASE: dict[str, list[dict]] = {
    cat_zona: cfg["escaloes"]
    for cat_zona, cfg in ESCALOES["PRE"].items()
}

# Escalões fallback para categorias PRE sem entrada em ESCALOES
_ESCALOES_PRE_FALLBACK: dict[str, list[dict]] = {
    "SOLAR_TER":           [{"preco": 40.0, "pct_bids": 1.00}],
    "EOLICA_MARINA":       [{"preco": 80.0, "pct_bids": 1.00}],
    "HIDRICA":             [{"preco": 15.0, "pct_bids": 1.00}],
    "TERMICA_NREN":        [{"preco": 60.0, "pct_bids": 1.00}],
    "GEOTERMICA":          [{"preco": 30.0, "pct_bids": 1.00}],
    "HIBRIDA_RENOV":       [{"preco": 45.0, "pct_bids": 1.00}],
    "RE_TARIFA_CUR":       [{"preco":  0.0, "pct_bids": 1.00}],
    "RE_OUTRO":            [{"preco":  0.0, "pct_bids": 1.00}],
    "ARMAZENAMENTO_VENDA": [{"preco":  0.0, "pct_bids": 1.00}],
}

# Ordem canónica de exibição das categorias PRE base (sem sufixo)
_PRE_ORDER = [
    "SOLAR_FOT", "SOLAR_TER", "EOLICA", "EOLICA_MARINA", "HIDRICA",
    "TERMICA_RENOV", "TERMICA_NREN", "GEOTERMICA", "HIBRIDA_RENOV",
    "RE_TARIFA_CUR", "RE_OUTRO", "ARMAZENAMENTO_VENDA",
]


def _sort_key_pre(cat_zona: str) -> tuple:
    """
    Ordena as categorias PRE com sufixo de zona:
      1. pela posição da categoria base em _PRE_ORDER
      2. dentro da mesma base: ES < PT < EXT (alphabético)
    """
    for base in _PRE_ORDER:
        if cat_zona == base or cat_zona.startswith(base + "_"):
            sufixo = cat_zona[len(base):].lstrip("_") or ""
            return (_PRE_ORDER.index(base), sufixo)
    return (999, cat_zona)


def _get_escaloes_pre(cat_zona: str) -> list[dict]:
    """
    Devolve os escalões para cat_zona (ex: "SOLAR_FOT_PT").
    Procura primeiro em ESCALOES["PRE"] (lookup exacto por cat_zona),
    depois por categoria base no fallback.
    """
    if cat_zona in ESCALOES["PRE"]:
        return ESCALOES["PRE"][cat_zona]["escaloes"]
    cat_base = next(
        (b for b in _PRE_ORDER if cat_zona == b or cat_zona.startswith(b + "_")),
        cat_zona,
    )
    return _ESCALOES_PRE_FALLBACK.get(cat_base, [{"preco": 0.0, "pct_bids": 1.00}])


def _get_escala(regime: str, cat_zona: str) -> float:
    """
    Devolve a escala padrão para regime/cat_zona a partir do dict ESCALOES.
    Retorna 1.0 se não existir entrada.
    """
    return ESCALOES.get(regime, {}).get(cat_zona, {}).get("escala", 1.0)


def gera_parametros_py(df: pd.DataFrame, path_saida: str) -> None:
    """
    Grava parametros_modelo.py — dicionário PARAMETROS sem comentários internos,
    pronto para edição manual e importação no pipeline.

    Valores de escala e escalões são preenchidos a partir do dict ESCALOES.
    Categorias não contempladas em ESCALOES recebem escala=1.0 e escalões fallback.
    """
    def _fmt_esc(esc: dict) -> str:
        return f'{{"preco": {esc["preco"]:<6.1f}, "pct_bids": {esc["pct_bids"]:.2f}}}'

    L = []

    L += [
        '"""',
        'parametros_modelo.py',
        'Dicionário PARAMETROS gerado automaticamente por classificacao_pre.py.',
        'Edite os valores de escala e escaloes conforme necessário.',
        '"""',
        '',
        '',
        'PARAMETROS: dict = {',
    ]

    # ── PRE ──────────────────────────────────────────────────────────────────
    cats_pre_found = sorted(
        df[df["regime"] == "PRE"]["categoria"].dropna().unique(),
        key=_sort_key_pre,
    )

    L.append('    "PRE": {')
    for cat_zona in cats_pre_found:
        escala   = _get_escala("PRE", cat_zona)
        escaloes = _get_escaloes_pre(cat_zona)
        L.append(f'        "{cat_zona}": {{')
        L.append(f'            "escala": {escala},')
        L.append(f'            "escaloes": [')
        for i, esc in enumerate(escaloes):
            virgula = "," if i < len(escaloes) - 1 else ""
            L.append(f'                {_fmt_esc(esc)}{virgula}')
        L.append(f'            ],')
        L.append(f'        }},')
    L.append('    },')

    # ── Restantes regimes (só escala) ─────────────────────────────────────────
    for regime in ("PRO", "CONSUMO", "COMERCIALIZADOR", "GENERICA", "PORFOLIO"):
        df_r = df[df["regime"] == regime]
        if df_r.empty:
            continue
        L.append(f'    "{regime}": {{')
        for cat_zona in sorted(df_r["categoria"].dropna().unique()):
            escala = _get_escala(regime, cat_zona)
            L.append(f'        "{cat_zona}": {{"escala": {escala}}},')
        L.append(f'    }},')

    L += [
        '}',
        '',
        '',
        'def get_escala(regime: str, categoria_zona: str) -> float:',
        '    return PARAMETROS.get(regime, {}).get(categoria_zona, {}).get("escala", 1.0)',
        '',
        '',
        'def get_escaloes_pre(categoria_zona: str) -> list[dict]:',
        '    return PARAMETROS["PRE"].get(categoria_zona, {}).get(',
        '        "escaloes", [{"preco": 0.0, "pct_bids": 1.00}]',
        '    )',
        '',
        '',
        'def escaloes_pre_flat() -> dict[str, list[dict]]:',
        '    return {',
        '        cat: cfg["escaloes"]',
        '        for cat, cfg in PARAMETROS["PRE"].items()',
        '    }',
        '',
    ]

    with open(path_saida, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L))

    print(f"Parâmetros guardados → {os.path.abspath(path_saida)}")



# ─────────────────────────────────────────────────────────────────────────────
#  EXECUÇÃO DIRECTA
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    PATH_CSV     = "./LISTA_UNIDADES.csv"
    PATH_OUT_CSV = "unidades_classificadas.csv"
    PATH_OUT_PY  = "parametros_modelo.py"

    df = carrega_e_classifica(PATH_CSV)
    resumo(df)
    gera_csv(df, PATH_OUT_CSV)
    gera_parametros_py(df, PATH_OUT_PY)