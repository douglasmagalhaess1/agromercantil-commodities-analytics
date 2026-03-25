"""Analise exploratoria — estatisticas descritivas e graficos."""

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DIRETORIO_PLOTS = Path("analysis/plots")
DIRETORIO_PLOTS.mkdir(parents=True, exist_ok=True)


def conectar():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE", "agromercantil"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def carregar_dados():
    """Tenta PostgreSQL, fallback para CSV processed."""
    try:
        conn = conectar()
        df = pd.read_sql("SELECT * FROM price_raw WHERE preco > 0", conn)
        conn.close()
        print(f"Carregados {len(df)} registros do PostgreSQL")
        return df
    except Exception as e:
        print(f"Postgres indisponivel ({e}), tentando CSV...")

    csvs = sorted(Path("data/processed").glob("precos_processed_*.csv"))
    if csvs:
        df = pd.read_csv(csvs[-1])
        df = df[df["preco"] > 0]
        print(f"Carregados {len(df)} registros de {csvs[-1].name}")
        return df

    raw = sorted(Path("data/raw").glob("*.json"))
    if raw:
        import json
        registros = []
        for arq in raw:
            with open(arq) as f:
                registros.extend(json.load(f))
        df = pd.DataFrame(registros)
        df["preco"] = pd.to_numeric(df["preco"], errors="coerce")
        df = df.dropna(subset=["preco"])
        df = df[df["preco"] > 0]
        print(f"Carregados {len(df)} registros de {len(raw)} arquivos JSON")
        return df

    print("Nenhuma fonte de dados encontrada")
    return pd.DataFrame()


def estatisticas_descritivas(df):
    print("\n=== Estatisticas descritivas por produto ===\n")
    for produto, grupo in df.groupby("produto"):
        precos = grupo["preco"]
        print(f"--- {produto.upper()} ---")
        print(f"  Registros:      {len(precos)}")
        print(f"  Media:          R$ {precos.mean():.2f}")
        print(f"  Mediana:        R$ {precos.median():.2f}")
        print(f"  Desvio padrao:  R$ {precos.std():.2f}")
        print(f"  Minimo:         R$ {precos.min():.2f}")
        print(f"  Maximo:         R$ {precos.max():.2f}")
        print()


def detectar_outliers_iqr(df):
    """Detecta outliers pelo metodo IQR (interquartil) por produto."""
    print("\n=== Outliers detectados (metodo IQR) ===\n")
    outliers_total = pd.DataFrame()

    for produto, grupo in df.groupby("produto"):
        q1 = grupo["preco"].quantile(0.25)
        q3 = grupo["preco"].quantile(0.75)
        iqr = q3 - q1
        limite_inferior = q1 - 1.5 * iqr
        limite_superior = q3 + 1.5 * iqr

        outliers = grupo[(grupo["preco"] < limite_inferior) | (grupo["preco"] > limite_superior)]
        print(f"{produto}: {len(outliers)} outliers "
              f"(limites: R$ {limite_inferior:.2f} — R$ {limite_superior:.2f})")

        outliers_total = pd.concat([outliers_total, outliers])

    return outliers_total


def plot_boxplot_precos(df):
    produtos = df["produto"].unique()
    dados = [df[df["produto"] == p]["preco"].values for p in produtos]

    fig, ax = plt.subplots(figsize=(10, 6))
    bp = ax.boxplot(dados, tick_labels=[p.capitalize() for p in produtos], patch_artist=True)

    cores = ["#2ecc71", "#e74c3c", "#3498db", "#f39c12", "#9b59b6"]
    for patch, cor in zip(bp["boxes"], cores[:len(produtos)]):
        patch.set_facecolor(cor)
        patch.set_alpha(0.7)

    ax.set_title("Distribuicao de precos por commodity")
    ax.set_ylabel("Preco (R$)")
    ax.grid(axis="y", alpha=0.3)

    caminho = DIRETORIO_PLOTS / "boxplot_precos.png"
    fig.savefig(caminho, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Salvo: {caminho}")


def plot_histograma_soja(df):
    soja = df[df["produto"] == "soja"]["preco"]
    if soja.empty:
        print("Sem dados de soja para histograma")
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(soja, bins=30, color="#2ecc71", edgecolor="white", alpha=0.8)
    ax.axvline(soja.mean(), color="#e74c3c", linestyle="--", label=f"Media: R$ {soja.mean():.2f}")
    ax.axvline(soja.median(), color="#3498db", linestyle="--", label=f"Mediana: R$ {soja.median():.2f}")

    ax.set_title("Distribuicao de precos — Soja")
    ax.set_xlabel("Preco (R$/saca)")
    ax.set_ylabel("Frequencia")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    caminho = DIRETORIO_PLOTS / "histograma_soja.png"
    fig.savefig(caminho, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Salvo: {caminho}")


def plot_scatter_preco_volume(df):
    # price_processed tem coluna volume; price_raw nao tem.
    # Se volume nao existir, usa qtd de registros por regiao como proxy.
    if "volume" in df.columns:
        dados = df.dropna(subset=["preco", "volume"])
        x, y = dados["volume"], dados["preco"]
        xlabel = "Volume"
    else:
        agrupado = df.groupby(["produto", "regiao"]).agg(
            preco_medio=("preco", "mean"),
            qtd_registros=("preco", "count"),
        ).reset_index()
        x, y = agrupado["qtd_registros"], agrupado["preco_medio"]
        xlabel = "Qtd registros (proxy de volume)"

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(x, y, alpha=0.6, edgecolors="white", s=50)
    ax.set_title("Preco vs Volume")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Preco (R$)")
    ax.grid(alpha=0.3)

    caminho = DIRETORIO_PLOTS / "scatter_preco_volume.png"
    fig.savefig(caminho, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Salvo: {caminho}")


def executar():
    df = carregar_dados()
    if df.empty:
        return

    estatisticas_descritivas(df)
    detectar_outliers_iqr(df)

    plot_boxplot_precos(df)
    plot_histograma_soja(df)
    plot_scatter_preco_volume(df)

    print("\nEDA concluida — graficos salvos em analysis/plots/")


if __name__ == "__main__":
    executar()