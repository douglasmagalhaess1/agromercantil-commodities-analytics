"""ETL — transformação dos dados brutos da CONAB para a camada processed."""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DIRETORIO_RAW = Path("data/raw")
DIRETORIO_PROCESSED = Path("data/processed")
DIRETORIO_CURATED = Path("data/curated")


def carregar_arquivos_raw(diretorio=DIRETORIO_RAW):
    arquivos = sorted(diretorio.glob("*.json"))
    if not arquivos:
        log.warning("Nenhum arquivo encontrado em %s", diretorio)
        return []

    registros = []
    for arq in arquivos:
        with open(arq, "r", encoding="utf-8") as f:
            dados = json.load(f)
            registros.extend(dados)
        log.info("Carregado %s (%d registros)", arq.name, len(dados))

    log.info("Total bruto: %d registros de %d arquivos", len(registros), len(arquivos))
    return registros


def normalizar_preco(valor):
    if isinstance(valor, (int, float)):
        return round(float(valor), 2)
    if not isinstance(valor, str):
        return None
    texto = valor.replace("R$", "").replace("/sc", "").replace("/kg", "").strip()
    texto = re.sub(r"[^\d.,]", "", texto)
    if not texto:
        return None
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")
    try:
        return round(float(texto), 2)
    except ValueError:
        return None


def normalizar_data(texto):
    if not texto or not isinstance(texto, str):
        return None
    formatos = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%Y"]
    for fmt in formatos:
        try:
            return datetime.strptime(texto.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def padronizar_regiao(nome):
    if not nome or not isinstance(nome, str):
        return None
    nome = nome.strip().upper()
    # Remove acentos comuns que aparecem em encoding quebrado
    substituicoes = {"Ã£": "Ã", "Ã©": "É", "Ã§": "Ç"}
    for errado, certo in substituicoes.items():
        nome = nome.replace(errado, certo)
    return nome


def padronizar_produto(nome):
    if not nome or not isinstance(nome, str):
        return None
    return nome.strip().lower()


def transformar(registros):
    """Ponto de entrada — aplica todas as transformações nos registros brutos."""
    if not registros:
        log.warning("Nenhum registro para transformar")
        return pd.DataFrame()

    df = pd.DataFrame(registros)
    qtd_original = len(df)

    df["preco"] = df["preco"].apply(normalizar_preco)
    df["data_referencia"] = df.get("data_referencia", pd.Series()).apply(normalizar_data)
    df["regiao"] = df["regiao"].apply(padronizar_regiao)
    df["produto"] = df["produto"].apply(padronizar_produto)

    # Remove linhas sem preço — dado essencial para análise
    df = df.dropna(subset=["preco"])
    df = df.drop_duplicates(subset=["produto", "regiao", "data_referencia", "preco"])

    qtd_final = len(df)
    log.info("Transformação: %d -> %d registros (%d removidos)",
             qtd_original, qtd_final, qtd_original - qtd_final)
    return df


def salvar_processed(df):
    DIRETORIO_PROCESSED.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    caminho_csv = DIRETORIO_PROCESSED / f"precos_processed_{timestamp}.csv"
    df.to_csv(caminho_csv, index=False, encoding="utf-8")

    caminho_parquet = DIRETORIO_PROCESSED / f"precos_processed_{timestamp}.parquet"
    df.to_parquet(caminho_parquet, index=False)

    log.info("Salvo processed: %s e %s", caminho_csv.name, caminho_parquet.name)
    return caminho_csv, caminho_parquet


def gerar_curated(df):
    DIRETORIO_CURATED.mkdir(parents=True, exist_ok=True)

    resumo = df.groupby(["produto", "regiao"]).agg(
        preco_medio=("preco", "mean"),
        preco_min=("preco", "min"),
        preco_max=("preco", "max"),
        qtd_registros=("preco", "count"),
    ).reset_index()

    resumo["preco_medio"] = resumo["preco_medio"].round(2)
    caminho = DIRETORIO_CURATED / "resumo_precos.csv"
    resumo.to_csv(caminho, index=False, encoding="utf-8")
    log.info("Curated salvo: %s (%d linhas)", caminho, len(resumo))
    return caminho


def executar():
    """Ponto de entrada principal — roda o pipeline de transformação completo."""
    registros = carregar_arquivos_raw()
    if not registros:
        return

    df = transformar(registros)
    if df.empty:
        return

    salvar_processed(df)
    gerar_curated(df)
    log.info("Pipeline de transformação concluído")


if __name__ == "__main__":
    executar()
