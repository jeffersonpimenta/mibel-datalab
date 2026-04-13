"""
Analise Estatistica e Visualizacao - Historico Clearing 
=================================================================
Le o ficheiro CSV de clearing, gera um sumario estatistico completo
e produz multiplos graficos analiticos.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import warnings
import os

warnings.filterwarnings("ignore")

# --- Configuracao de estilo global -------------------------------------------
# plt.rcParams.update({
    # "figure.facecolor": "#0f1117",
    # "axes.facecolor":   "#1a1d27",
    # "axes.edgecolor":   "#3a3d4d",
    # "axes.labelcolor":  "#c8cad8",
    # "xtick.color":      "#8a8ca0",
    # "ytick.color":      "#8a8ca0",
    # "text.color":       "#e0e2f0",
    # "grid.color":       "#2a2d3d",
    # "grid.linewidth":   0.6,
    # "grid.alpha":       0.7,
    # "legend.facecolor": "#1a1d27",
    # "legend.edgecolor": "#3a3d4d",
    # "font.family":      "DejaVu Sans",
    # "font.size":        10,
    # "axes.titlesize":   13,
    # "axes.titleweight": "bold",
# })

COR_ORIG  = "#4fc3f7"
COR_SUB   = "#f06292"
COR_DELTA = "#81c784"
COR_VOL   = "#ffb74d"
COR_BIDS  = "#ce93d8"

PAISES_CORES = {"MI": "#4fc3f7", "ES": "#f06292", "PT": "#a5d6a7"}

OUTPUT_DIR = "graficos_clearing-C1"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# --- 1. LEITURA E PRE-PROCESSAMENTO ------------------------------------------

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
        dtype={"Hora": int, "pais": str},
    )
    df.columns = df.columns.str.strip()
    df["data"] = df["data_ficheiro"].str.extract(r"(\d{8})")[0]
    df["data"] = pd.to_datetime(df["data"], format="%Y%m%d")
    df["ano"]        = df["data"].dt.year
    df["mes"]        = df["data"].dt.to_period("M")
    df["semana"]     = df["data"].dt.to_period("W")
    df["dia_semana"] = df["data"].dt.day_name()
    num_cols = ["preco_clearing_orig", "volume_clearing_orig",
                "preco_clearing_sub",  "volume_clearing_sub",
                "delta_preco",         "n_bids_substituidos"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# --- 2. SUMARIO ESTATISTICO --------------------------------------------------

def print_summary(df: pd.DataFrame):
    sep = "=" * 70
    print(f"\n{sep}")
    print("  SUMARIO ESTATISTICO - HISTORICO CLEARING")
    print(sep)
    print(f"\n  Periodo analisado : {df['data'].min().date()}  ->  {df['data'].max().date()}")
    print(f"  Total de registos : {len(df):,}")
    print(f"  Dias unicos       : {df['data'].nunique():,}")

    print("\n-- Distribuicao por Pais " + "-" * 46)
    pais_dist = df.groupby("pais").size().rename("registos")
    pais_pct  = (pais_dist / len(df) * 100).round(2).rename("%")
    print(pd.concat([pais_dist, pais_pct], axis=1).to_string())

    cols_labels = {
        "preco_clearing_orig":  "Preco Clearing Original  (EUR/MWh)",
        "preco_clearing_sub":   "Preco Clearing Substituido (EUR/MWh)",
        "delta_preco":          "Delta Preco              (EUR/MWh)",
        "volume_clearing_orig": "Volume Original          (MWh)",
        "volume_clearing_sub":  "Volume Substituido       (MWh)",
        "n_bids_substituidos":  "Nr Bids Substituidos",
    }
    print("\n-- Estatisticas Globais " + "-" * 47)
    stats = df[list(cols_labels)].describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]).T
    stats.index = [cols_labels[c] for c in stats.index]
    with pd.option_context("display.float_format", "{:,.2f}".format,
                           "display.max_colwidth", 45):
        print(stats.to_string())

    neg = df[df["delta_preco"] < 0]
    print(f"\n-- Delta Negativo (preco sub < original) " + "-" * 29)
    print(f"   Registos com delta < 0 : {len(neg):,}  ({len(neg)/len(df)*100:.1f} %)")
    if len(neg):
        print(f"   Delta minimo           : {neg['delta_preco'].min():.2f} EUR/MWh")
        print(f"   Paises afetados        : {neg['pais'].value_counts().to_dict()}")
        print(f"   Horas mais frequentes  : {neg['Hora'].value_counts().head(5).to_dict()}")

    print("\n-- Médias Anuais " + "-" * 53)
    anual = df.groupby("ano").agg(
        preco_orig_medio=("preco_clearing_orig", "mean"),
        preco_sub_medio =("preco_clearing_sub",  "mean"),
        delta_medio     =("delta_preco",          "mean"),
        vol_orig_medio  =("volume_clearing_orig", "mean"),
        vol_sub_medio   =("volume_clearing_sub",  "mean"),
        bids_medio      =("n_bids_substituidos",  "mean"),
        registos        =("data", "count"),
    ).round(2)
    print(anual.to_string())

    dias_split = df[df["pais"].isin(["ES", "PT"])]["data"].nunique()
    dias_total = df["data"].nunique()
    print(f"\n-- Separacao de Mercado (ES/PT) " + "-" * 39)
    print(f"   Dias com separacao MI->ES+PT : {dias_split:,}  ({dias_split/dias_total*100:.1f} % dos dias)")
    print(f"\n{sep}\n")


# --- 3. GRAFICOS -------------------------------------------------------------

def save(fig, name):
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"  Gravado: {path}")
    plt.close(fig)


def _fmt_mensal_xaxis(ax, interval=1):
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=interval))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=8)


# --- Dashboard: 6 graficos individuais ---------------------------------------

def plot_dashboard_preco_mensal(df):
    mensal = df.groupby("mes").agg(
        orig=("preco_clearing_orig", "mean"),
        sub =("preco_clearing_sub",  "mean"),
    )
    mensal.index = mensal.index.to_timestamp()
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(mensal.index, mensal["orig"], color=COR_ORIG, lw=2, label="Referência",    marker="o", ms=3)
    ax.plot(mensal.index, mensal["sub"],  color=COR_SUB,  lw=2, label="Cenário x", marker="s", ms=3)
    ax.fill_between(mensal.index, mensal["orig"], mensal["sub"], alpha=0.1, color=COR_SUB)
    #ax.set_title("Preco Medio Mensal (EUR/MWh)")
    ax.set_ylabel("EUR/MWh")
    ax.legend()
    ax.grid()
    _fmt_mensal_xaxis(ax)
    fig.tight_layout()
    save(fig, "00a_dashboard_preco_mensal.png")


def plot_dashboard_dist_pais(df):
    fig, ax = plt.subplots(figsize=(6, 6))
    pais_cnt = df["pais"].value_counts()
    wedge_colors = [PAISES_CORES.get(p, "#aaaaaa") for p in pais_cnt.index]
    ax.pie(pais_cnt, labels=pais_cnt.index, colors=wedge_colors,
           autopct="%1.1f%%", startangle=90,
           textprops={"color": "#e0e2f0", "fontsize": 11})
    #ax.set_title("Registos por Pais")
    fig.tight_layout()
    save(fig, "00b_dashboard_dist_pais.png")


def plot_dashboard_delta_mensal(df):
    mensal = df.groupby("mes").agg(delta=("delta_preco", "mean"))
    mensal.index = mensal.index.to_timestamp()
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(mensal.index, mensal["delta"],
           color=[COR_DELTA if v >= 0 else COR_SUB for v in mensal["delta"]],
           width=20, alpha=0.85)
    ax.axhline(0, color="white", lw=0.8, linestyle="--", alpha=0.5)
    #ax.set_title("Delta Medio Mensal (EUR/MWh)")
    ax.set_ylabel("EUR/MWh")
    ax.grid(axis="y")
    _fmt_mensal_xaxis(ax)
    fig.tight_layout()
    save(fig, "00c_dashboard_delta_mensal.png")


def plot_dashboard_bids_mensal(df):
    mensal = df.groupby("mes").agg(bids=("n_bids_substituidos", "mean"))
    mensal.index = mensal.index.to_timestamp()
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(mensal.index, mensal["bids"], color=COR_BIDS, lw=2, marker="o", ms=3)
    ax.fill_between(mensal.index, mensal["bids"], alpha=0.15, color=COR_BIDS)
    #ax.set_title("Nr Medio Mensal de Bids do Cenário x")
    ax.set_ylabel("Nr bids")
    ax.grid()
    _fmt_mensal_xaxis(ax)
    fig.tight_layout()
    save(fig, "00d_dashboard_bids_mensal.png")


def plot_dashboard_volume_mensal(df):
    mensal = df.groupby("mes").agg(
        vol_orig=("volume_clearing_orig", "mean"),
        vol_sub =("volume_clearing_sub",  "mean"),
    )
    mensal.index = mensal.index.to_timestamp()
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(mensal.index, mensal["vol_orig"], color=COR_ORIG, lw=2, label="Volume Referência",    marker="o", ms=3)
    ax.plot(mensal.index, mensal["vol_sub"],  color=COR_VOL,  lw=2, label="Volume do Cenário x", marker="s", ms=3)
    ax.fill_between(mensal.index, mensal["vol_orig"], mensal["vol_sub"], alpha=0.1, color=COR_VOL)
    #ax.set_title("Volume Medio Mensal Despachado (MWh/h)")
    ax.set_ylabel("MWh")
    ax.legend()
    ax.grid()
    _fmt_mensal_xaxis(ax)
    fig.tight_layout()
    save(fig, "00e_dashboard_volume_mensal.png")


def plot_dashboard_hist_delta(df):
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.hist(df["delta_preco"].dropna(), bins=80, color=COR_DELTA, alpha=0.8, edgecolor="#0f1117")
    ax.axvline(df["delta_preco"].mean(), color="white", lw=1.2, linestyle="--",
               label=f"Média={df['delta_preco'].mean():.1f}")
    #ax.set_title("Distribuicao Delta Preco (EUR/MWh)")
    ax.set_xlabel("Delta Preço")
    ax.set_ylabel("Frequência")
    ax.legend()
    ax.grid(axis="y")
    fig.tight_layout()
    save(fig, "00f_dashboard_hist_delta.png")


# --- Graficos gerais ---------------------------------------------------------

def plot_anual(df):
    # Calculamos a média de cada cenário
    media_2024 = df["preco_clearing_orig"].mean()
    media_2030 = df["preco_clearing_sub"].mean()
    
    fig, ax = plt.subplots(figsize=(8, 6))
    
    # Definimos os rótulos e os valores para o eixo X
    categorias = ["2024\n(Referência)", "2030\n"]
    valores = [media_2024, media_2030]
    cores = [COR_ORIG, COR_SUB]
    
    # Criamos as barras
    bars = ax.bar(categorias, valores, color=cores, alpha=0.85, width=0.6)
    
    # Adicionamos os valores no topo das barras
    ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=10, weight='bold')
    
    # Ajustes de títulos e estilo
    #ax.set_title("Comparação de Preço Médio de Clearing: 2024 vs 2030")
    ax.set_ylabel("EUR/MWh")
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    
    # Opcional: remover moldura superior e direita para um visual mais limpo
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    fig.tight_layout()
    save(fig, "01_preco_anual.png")
    
def plot_mensal(df):
    mensal = df.groupby("mes").agg(
        orig =("preco_clearing_orig", "mean"),
        sub  =("preco_clearing_sub",  "mean"),
        delta=("delta_preco",         "mean"),
    )
    mensal.index = mensal.index.to_timestamp()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True,
                                   gridspec_kw={"height_ratios": [3, 1.4]})
    ax1.plot(mensal.index, mensal["orig"], color=COR_ORIG, lw=2, label="Referência",    marker="o", ms=4)
    ax1.plot(mensal.index, mensal["sub"],  color=COR_SUB,  lw=2, label="Cenário x", marker="s", ms=4)
    ax1.fill_between(mensal.index, mensal["orig"], mensal["sub"], alpha=0.1, color=COR_SUB)
    ax1.set_title("Evolucao Mensal do Preço de Clearing  (EUR/MWh)")
    ax1.set_ylabel("EUR/MWh")
    ax1.legend()
    ax1.grid()
    ax2.bar(mensal.index, mensal["delta"], color=COR_DELTA, alpha=0.8, width=20)
    ax2.axhline(0, color="white", lw=0.8, linestyle="--", alpha=0.5)
    ax2.set_ylabel("Delta Preço (EUR/MWh)")
    ax2.set_title("Delta Medio Mensal")
    ax2.grid()
    _fmt_mensal_xaxis(ax2)
    fig.tight_layout()
    save(fig, "02_preco_mensal.png")


def plot_semanal(df):
    semanal = df.groupby("semana").agg(
        orig =("preco_clearing_orig", "mean"),
        sub  =("preco_clearing_sub",  "mean"),
    )
    semanal.index = semanal.index.to_timestamp()
    fig, ax = plt.subplots(figsize=(16, 5))
    ax.plot(semanal.index, semanal["orig"], color=COR_ORIG, lw=1.5, label="Referência",    alpha=0.9)
    ax.plot(semanal.index, semanal["sub"],  color=COR_SUB,  lw=1.5, label="Cenário x", alpha=0.9)
    ax.fill_between(semanal.index, semanal["orig"], semanal["sub"], alpha=0.08, color=COR_SUB)
    #ax.set_title("Evolucao Semanal do Preco de Clearing  (EUR/MWh)")
    ax.set_ylabel("EUR/MWh")
    ax.legend()
    ax.grid()
    _fmt_mensal_xaxis(ax)
    fig.tight_layout()
    save(fig, "03_preco_semanal.png")


# --- Perfil horario: um grafico por pais -------------------------------------

def _plot_perfil_horario_pais(df, pais, filename):
    sub = df[df["pais"] == pais].groupby("Hora").agg(
        orig=("preco_clearing_orig", "mean"),
        sub =("preco_clearing_sub",  "mean"),
    )
    if sub.empty:
        print(f"  Sem dados para pais={pais}, grafico ignorado.")
        return
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(sub.index, sub["orig"], color=COR_ORIG, lw=2, label="Referência",    marker="o", ms=5)
    ax.plot(sub.index, sub["sub"],  color=COR_SUB,  lw=2, label="Cenário x", marker="s", ms=5)
    ax.fill_between(sub.index, sub["orig"], sub["sub"], alpha=0.12, color=COR_SUB)
    #ax.set_title(f"Perfil Horario Medio do Preco de Clearing - {pais}  (EUR/MWh)")
    ax.set_xlabel("Hora")
    ax.set_ylabel("EUR/MWh")
    ax.set_xticks(range(1, 25))
    ax.grid()
    ax.legend()
    fig.tight_layout()
    save(fig, filename)


def plot_perfil_horario_MI(df):
    _plot_perfil_horario_pais(df, "MI", "04a_perfil_horario_MI.png")

def plot_perfil_horario_ES(df):
    _plot_perfil_horario_pais(df, "ES", "04b_perfil_horario_ES.png")

def plot_perfil_horario_PT(df):
    _plot_perfil_horario_pais(df, "PT", "04c_perfil_horario_PT.png")


def plot_volume_mensal(df):
    mensal = df.groupby("mes").agg(
        vol_orig=("volume_clearing_orig", "mean"),
        vol_sub =("volume_clearing_sub",  "mean"),
    )
    mensal.index = mensal.index.to_timestamp()
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(mensal.index, mensal["vol_orig"], color=COR_ORIG, lw=2, label="Volume Referência",    marker="o", ms=4)
    ax.plot(mensal.index, mensal["vol_sub"],  color=COR_VOL,  lw=2, label="Volume do Cenário x", marker="s", ms=4)
    ax.fill_between(mensal.index, mensal["vol_orig"], mensal["vol_sub"], alpha=0.1, color=COR_VOL)
    #ax.set_title("Evolucao Mensal do Volume Despachado  (MWh)")
    ax.set_ylabel("MWh (média/hora)")
    ax.legend()
    ax.grid()
    _fmt_mensal_xaxis(ax)
    fig.tight_layout()
    save(fig, "05_volume_mensal.png")


def plot_bids(df):
    mensal_bids = df.groupby("mes")["n_bids_substituidos"].mean()
    mensal_bids.index = mensal_bids.index.to_timestamp()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    ax1.plot(mensal_bids.index, mensal_bids.values, color=COR_BIDS, lw=2, marker="o", ms=4)
    ax1.fill_between(mensal_bids.index, mensal_bids.values, alpha=0.15, color=COR_BIDS)
    ax1.set_title("Evolucao Mensal - Nr Medio de Bids do Cenário x")
    ax1.set_ylabel("Nr bids")
    ax1.grid()
    _fmt_mensal_xaxis(ax1, interval=2)
    ax2.hist(df["n_bids_substituidos"].dropna(), bins=50, color=COR_BIDS, alpha=0.8, edgecolor="#0f1117")
    ax2.set_title("Distribuicao - Nr de Bids do Cenário x")
    ax2.set_xlabel("Nr bids")
    ax2.set_ylabel("Frequência")
    ax2.grid(axis="y")
    fig.tight_layout()
    save(fig, "06_bids_substituidos.png")


def plot_heatmap_hora_mes(df):
    sub_mi = df[df["pais"] == "MI"].copy()
    sub_mi["mes_str"] = sub_mi["data"].dt.strftime("%Y-%m")
    pivot = sub_mi.pivot_table(index="Hora", columns="mes_str",
                               values="preco_clearing_sub", aggfunc="mean")
    fig, ax = plt.subplots(figsize=(18, 7))
    im = ax.imshow(pivot.values, aspect="auto", cmap="plasma",
                   origin="lower", interpolation="nearest")
    cbar = fig.colorbar(im, ax=ax, pad=0.01)
    cbar.set_label("EUR/MWh", color="#c8cad8")
    cbar.ax.yaxis.set_tick_params(color="#c8cad8")
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="#c8cad8")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=7)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=90, fontsize=7)
    ax.set_ylabel("Hora")
    ax.set_xlabel("Mês")
    #ax.set_title("Heatmap: Preco Cenário x Medio por Hora x Mes  (MI, EUR/MWh)")
    fig.tight_layout()
    save(fig, "07_heatmap_hora_mes.png")


# --- Boxplot delta: um grafico por pais --------------------------------------

def _plot_boxplot_delta_pais(df, pais, filename):
    sub = df[df["pais"] == pais].copy()
    if sub.empty:
        print(f"  Sem dados para pais={pais}, grafico ignorado.")
        return
    grupos = [grp["delta_preco"].dropna().values
              for _, grp in sub.groupby(sub["data"].dt.to_period("M"))]
    labels  = [str(k) for k, _ in sub.groupby(sub["data"].dt.to_period("M"))]
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.boxplot(grupos, patch_artist=True, showfliers=False,
               medianprops=dict(color="white", lw=1.5),
               boxprops=dict(facecolor=PAISES_CORES[pais], alpha=0.6),
               whiskerprops=dict(color="#8a8ca0"),
               capprops=dict(color="#8a8ca0"))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.axhline(0, color="white", lw=0.8, linestyle="--", alpha=0.5)
    #ax.set_title(f"Distribuicao Mensal do Delta de Preco - {pais}  (EUR/MWh)")
    ax.set_ylabel("Delta Preço (EUR/MWh)")
    ax.grid(axis="y")
    fig.tight_layout()
    save(fig, filename)


def plot_boxplot_delta_MI(df):
    _plot_boxplot_delta_pais(df, "MI", "08a_boxplot_delta_MI.png")

def plot_boxplot_delta_ES(df):
    _plot_boxplot_delta_pais(df, "ES", "08b_boxplot_delta_ES.png")

def plot_boxplot_delta_PT(df):
    _plot_boxplot_delta_pais(df, "PT", "08c_boxplot_delta_PT.png")


def plot_scatter_orig_vs_sub(df):
    sample = df.sample(min(5000, len(df)), random_state=42)
    fig, ax = plt.subplots(figsize=(7, 7))
    sc = ax.scatter(sample["preco_clearing_orig"], sample["preco_clearing_sub"],
                    c=sample["delta_preco"], cmap="coolwarm", alpha=0.4, s=12,
                    vmin=sample["delta_preco"].quantile(0.05),
                    vmax=sample["delta_preco"].quantile(0.95))
    cbar = fig.colorbar(sc, ax=ax)
    cbar.set_label("Delta Preço (EUR/MWh)", color="#c8cad8")
    cbar.ax.yaxis.set_tick_params(color="#c8cad8")
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color="#c8cad8")
    lim = max(sample["preco_clearing_orig"].max(), sample["preco_clearing_sub"].max()) * 1.05
    ax.plot([0, lim], [0, lim], "--", color="white", lw=1, alpha=0.5, label="y = x")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.set_xlabel("Preço Referência (EUR/MWh)")
    ax.set_ylabel("Preço do Cenário x (EUR/MWh)")
    #ax.set_title("Preco Referência vs Cenário x\n(amostra de 5 000 registos, cor = delta)")
    ax.legend(fontsize=9)
    ax.grid()
    fig.tight_layout()
    save(fig, "09_scatter_orig_vs_sub.png")


def plot_separacao_mercado(df):
    split = df[df["pais"].isin(["ES", "PT"])].copy()
    mensal_split = split.groupby(["mes", "pais"]).size().unstack(fill_value=0)
    mensal_split.index = mensal_split.index.to_timestamp()
    fig, ax = plt.subplots(figsize=(14, 5))
    if "ES" in mensal_split.columns:
        ax.bar(mensal_split.index, mensal_split["ES"], width=20,
               color=PAISES_CORES["ES"], label="ES", alpha=0.85)
    if "PT" in mensal_split.columns:
        bottom = mensal_split.get("ES", 0)
        ax.bar(mensal_split.index, mensal_split["PT"], width=20,
               bottom=bottom, color=PAISES_CORES["PT"], label="PT", alpha=0.85)
    #ax.set_title("Nr de Horas Mensais com Separacao de Mercado (MI -> ES + PT)")
    ax.set_ylabel("Nr de horas")
    ax.legend()
    ax.grid(axis="y")
    _fmt_mensal_xaxis(ax)
    fig.tight_layout()
    save(fig, "10_separacao_mercado.png")


def plot_correlacao_delta_bids(df):
    sample = df.dropna(subset=["delta_preco", "n_bids_substituidos"]).sample(
        min(8000, len(df)), random_state=7)
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.hexbin(sample["n_bids_substituidos"], sample["delta_preco"],
              gridsize=50, cmap="YlOrRd", mincnt=1)
    ax.set_xlabel("Nr de Bids do Cenário x")
    ax.set_ylabel("Delta Preço (EUR/MWh)")
    corr = sample["delta_preco"].corr(sample["n_bids_substituidos"])
    #ax.set_title(f"Delta Preco vs Nr Bids  (Pearson r = {corr:.3f})")
    ax.grid()
    fig.tight_layout()
    save(fig, "11_corr_delta_bids.png")


# --- MAIN --------------------------------------------------------------------

def main():
    ficheiro = "historico_clearing_comparado-c1.csv"
    if not os.path.exists(ficheiro):
        ficheiro = os.path.join(os.path.dirname(__file__), ficheiro)
    if not os.path.exists(ficheiro):
        candidato = "/mnt/user-data/uploads/historico_clearing_comparado_cenario1.csv"
        if os.path.exists(candidato):
            ficheiro = candidato

    print(f"\nA carregar ficheiro: {ficheiro}")
    df = load_data(ficheiro)
    print(f"{len(df):,} registos carregados.")

    print_summary(df)

    print("A gerar graficos...\n")

    # Dashboard: 6 graficos individuais
    #plot_dashboard_preco_mensal(df)    # 00a
    #plot_dashboard_dist_pais(df)       # 00b
    #plot_dashboard_delta_mensal(df)    # 00c
    #plot_dashboard_bids_mensal(df)     # 00d
    #plot_dashboard_volume_mensal(df)   # 00e
    plot_dashboard_hist_delta(df)      # 00f

    # Graficos gerais
    plot_anual(df)                     # 01
    plot_mensal(df)                    # 02
    #plot_semanal(df)                   # 03

    # Perfil horario: um por pais
    plot_perfil_horario_MI(df)         # 04a
    plot_perfil_horario_ES(df)         # 04b
    plot_perfil_horario_PT(df)         # 04c

    plot_volume_mensal(df)             # 05
    #plot_bids(df)                      # 06
    plot_heatmap_hora_mes(df)          # 07

    # Boxplot delta: um por pais
    plot_boxplot_delta_MI(df)          # 08a
    plot_boxplot_delta_ES(df)          # 08b
    plot_boxplot_delta_PT(df)          # 08c

    plot_scatter_orig_vs_sub(df)       # 09
    #plot_separacao_mercado(df)         # 10
    #plot_correlacao_delta_bids(df)     # 11

    print(f"\nTodos os graficos gravados em: ./{OUTPUT_DIR}/")
    print("Ficheiros gerados:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        print(f"  {f}")


if __name__ == "__main__":
    main()