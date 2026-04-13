"""
parametros_modelo.py
Dicionário PARAMETROS gerado automaticamente por classificacao_pre.py.
Edite os valores de escala e escaloes conforme necessário.
"""


PARAMETROS: dict = {
    "PRE": {
        "SOLAR_FOT_ES": {
            "escala": 2.2297,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 0.30},
                {"preco": 20.0  , "pct_bids": 0.30},
                {"preco": 35.0  , "pct_bids": 0.40}
            ],
        },
        "SOLAR_FOT_PT": {
            "escala": 3.6879,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 0.30},
                {"preco": 20.0  , "pct_bids": 0.30},
                {"preco": 35.0  , "pct_bids": 0.40}
            ],
        },
        "SOLAR_TER_ES": {
            "escala": 2.0869,
            "escaloes": [
                {"preco": 40.0  , "pct_bids": 1.00}
            ],
        },
        "EOLICA_ES": {
            "escala": 1.8857,
            "escaloes": [
                {"preco": 50.0  , "pct_bids": 0.50},
                {"preco": 70.0  , "pct_bids": 0.50}
            ],
        },
        "EOLICA_MARINA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "EOLICA_PT": {
            "escala": 2.1379,
            "escaloes": [
                {"preco": 50.0  , "pct_bids": 0.50},
                {"preco": 70.0  , "pct_bids": 0.50}
            ],
        },
        "HIDRICA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 15.0  , "pct_bids": 1.00}
            ],
        },
        "TERMICA_RENOV_ES": {
            "escala": 1.4,
            "escaloes": [
                {"preco": 10.0  , "pct_bids": 1.00}
            ],
        },
        "TERMICA_RENOV_PT": {
            "escala": 1.5348,
            "escaloes": [
                {"preco": 10.0  , "pct_bids": 1.00}
            ],
        },
        "TERMICA_NREN_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 60.0  , "pct_bids": 1.00}
            ],
        },
        "GEOTERMICA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 30.0  , "pct_bids": 1.00}
            ],
        },
        "HIBRIDA_RENOV_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 45.0  , "pct_bids": 1.00}
            ],
        },
        "RE_TARIFA_CUR_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "RE_TARIFA_CUR_PT": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "RE_OUTRO_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "ARMAZENAMENTO_VENDA_ES": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
        "ARMAZENAMENTO_VENDA_PT": {
            "escala": 1.0,
            "escaloes": [
                {"preco": 0.0   , "pct_bids": 1.00}
            ],
        },
    },
    "PRO": {
        "BOMBEO_PURO_PRO_ES": {"escala": 1.0},
        "CARVAO_ES": {"escala": 1.0},
        "CICLO_COMBINADO_ES": {"escala": 1.0},
        "CICLO_COMBINADO_PT": {"escala": 0.7143},
        "GAS_ES": {"escala": 1.0},
        "HIDRICA_PRO_ES": {"escala": 1.0},
        "HIDRICA_PRO_PT": {"escala": 1.0},
        "NUCLEAR_ES": {"escala": 0.447},
    },
    "CONSUMO": {
        "ARMAZENAMENTO_COMPRA_ES": {"escala": 1.0},
        "ARMAZENAMENTO_COMPRA_PT": {"escala": 1.0},
        "BOMBEO_CONSUMO_ES": {"escala": 1.6926},
        "BOMBEO_CONSUMO_PT": {"escala": 1.3871},
        "CONS_AUXILIARES_ES": {"escala": 1.0},
        "CONS_DIRECTO_ES": {"escala": 1.6926},
        "CONS_DIRECTO_EXT": {"escala": 1.6926},
        "CONS_PRODUTOR_ES": {"escala": 1.0},
    },
    "COMERCIALIZADOR": {
        "COMERC_ES": {"escala": 1.3871},
        "COMERC_EXT": {"escala": 1.0},
        "COMERC_NR_EXT": {"escala": 1.0},
        "COMERC_PT": {"escala": 1.6926},
        "COMERC_ULT_REC_ES": {"escala": 1.0},
        "COMERC_ULT_REC_PT": {"escala": 1.0},
    },
    "GENERICA": {
        "GENERICA_ES": {"escala": 1.0},
        "GENERICA_PT": {"escala": 1.0},
        "GENERICA_VENDA_ES": {"escala": 1.0},
        "GENERICA_VENDA_PT": {"escala": 1.0},
    },
    "PORFOLIO": {
        "PORTF_COMERC_ES": {"escala": 1.0},
        "PORTF_PROD_ES": {"escala": 1.0},
        "PORTF_PROD_PT": {"escala": 1.0},
    },
}


def get_escala(regime: str, categoria_zona: str) -> float:
    return PARAMETROS.get(regime, {}).get(categoria_zona, {}).get("escala", 1.0)


def get_escaloes_pre(categoria_zona: str) -> list[dict]:
    return PARAMETROS["PRE"].get(categoria_zona, {}).get(
        "escaloes", [{"preco": 0.0, "pct_bids": 1.00}]
    )


def escaloes_pre_flat() -> dict[str, list[dict]]:
    return {
        cat: cfg["escaloes"]
        for cat, cfg in PARAMETROS["PRE"].items()
    }
