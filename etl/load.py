"""Carga dos dados processed/curated no PostgreSQL via psycopg2."""

import csv
import logging
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DIRETORIO_PROCESSED = Path("data/processed")
DIRETORIO_CURATED = Path("data/curated")


def conectar():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE", "agromercantil"),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def carregar_processed(conn, arquivo_csv=None):
    """Carrega preços da camada processed na tabela price_raw."""
    if arquivo_csv is None:
        arquivos = sorted(DIRETORIO_PROCESSED.glob("precos_processed_*.csv"))
        if not arquivos:
            log.warning("Nenhum CSV processed encontrado")
            return 0
        arquivo_csv = arquivos[-1]

    inseridos = 0
    with open(arquivo_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f)
        with conn.cursor() as cur:
            for linha in leitor:
                preco = linha.get("preco")
                if not preco:
                    continue
                cur.execute("""
                    INSERT INTO price_raw (produto, regiao, data_referencia, preco, unidade, data_coleta)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (produto, regiao, data_referencia, preco) DO NOTHING
                """, (
                    linha.get("produto"),
                    linha.get("regiao"),
                    linha.get("data_referencia"),
                    float(preco),
                    linha.get("unidade", "saca"),
                    linha.get("data_coleta"),
                ))
                inseridos += cur.rowcount
    conn.commit()
    log.info("price_raw: %d registros inseridos de %s", inseridos, arquivo_csv.name)
    return inseridos


def _parsear_regiao(texto):
    """Extrai nome e UF de strings como 'SORRISO - MT'."""
    if not texto:
        return None, None
    if " - " in texto:
        partes = texto.rsplit(" - ", 1)
        return partes[0].strip(), partes[1].strip()[:2]
    return texto.strip(), None


def carregar_dimensoes(conn, arquivo_csv=None):
    """Popula tabelas de dimensão (commodity e regiao) a partir dos dados processados."""
    if arquivo_csv is None:
        arquivos = sorted(DIRETORIO_PROCESSED.glob("precos_processed_*.csv"))
        if not arquivos:
            log.warning("Nenhum CSV processed encontrado para dimensões")
            return
        arquivo_csv = arquivos[-1]

    produtos = set()
    regioes = set()

    with open(arquivo_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f)
        for linha in leitor:
            produto = linha.get("produto", "").strip()
            regiao = linha.get("regiao", "").strip()
            unidade = linha.get("unidade", "saca").strip()
            if produto:
                produtos.add((produto, unidade))
            if regiao:
                regioes.add(regiao)

    with conn.cursor() as cur:
        for nome, unidade in produtos:
            cur.execute("""
                INSERT INTO commodity (nome, unidade)
                VALUES (%s, %s)
                ON CONFLICT (nome) DO NOTHING
            """, (nome, unidade))

        for regiao_completa in regioes:
            nome, uf = _parsear_regiao(regiao_completa)
            cur.execute("""
                INSERT INTO regiao (nome, uf)
                VALUES (%s, %s)
                ON CONFLICT (nome, uf) DO NOTHING
            """, (nome, uf))

    conn.commit()
    log.info("Dimensões carregadas: %d produtos, %d regiões", len(produtos), len(regioes))


def carregar_price_processed(conn, arquivo_csv=None):
    """Carrega dados normalizados na tabela price_processed com FK lookups."""
    if arquivo_csv is None:
        arquivos = sorted(DIRETORIO_PROCESSED.glob("precos_processed_*.csv"))
        if not arquivos:
            log.warning("Nenhum CSV processed encontrado")
            return 0
        arquivo_csv = arquivos[-1]

    with conn.cursor() as cur:
        cur.execute("SELECT id, nome FROM commodity")
        commodity_map = {nome: cid for cid, nome in cur.fetchall()}

        cur.execute("SELECT id, nome, uf FROM regiao")
        regiao_map = {}
        for rid, nome, uf in cur.fetchall():
            regiao_map[(nome, uf)] = rid

    inseridos = 0
    with open(arquivo_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE price_processed")
            for linha in leitor:
                preco = linha.get("preco")
                if not preco:
                    continue

                produto = linha.get("produto", "").strip()
                regiao_texto = linha.get("regiao", "").strip()
                nome_regiao, uf = _parsear_regiao(regiao_texto)

                commodity_id = commodity_map.get(produto)
                region_id = regiao_map.get((nome_regiao, uf))

                if not commodity_id:
                    continue

                cur.execute("""
                    INSERT INTO price_processed
                        (commodity_id, region_id, data_referencia, preco, fonte)
                    VALUES (%s, %s, %s, %s, 'CONAB')
                """, (
                    commodity_id,
                    region_id,
                    linha.get("data_referencia"),
                    float(preco),
                ))
                inseridos += cur.rowcount

    conn.commit()
    log.info("price_processed: %d registros inseridos de %s", inseridos, arquivo_csv.name)
    return inseridos


def carregar_curated(conn, arquivo_csv=None):
    """Carrega resumo agregado na tabela price_curated."""
    if arquivo_csv is None:
        caminho = DIRETORIO_CURATED / "resumo_precos.csv"
        if not caminho.exists():
            log.warning("Arquivo curated não encontrado: %s", caminho)
            return 0
        arquivo_csv = caminho

    inseridos = 0
    with open(arquivo_csv, "r", encoding="utf-8") as f:
        leitor = csv.DictReader(f)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE price_curated")
            for linha in leitor:
                cur.execute("""
                    INSERT INTO price_curated (produto, regiao, preco_medio, preco_min, preco_max, qtd_registros)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    linha.get("produto"),
                    linha.get("regiao"),
                    float(linha["preco_medio"]),
                    float(linha["preco_min"]),
                    float(linha["preco_max"]),
                    int(linha["qtd_registros"]),
                ))
                inseridos += cur.rowcount
    conn.commit()
    log.info("price_curated: %d registros inseridos", inseridos)
    return inseridos


def executar():
    """Ponto de entrada principal — carrega processed e curated no PostgreSQL."""
    conn = conectar()
    try:
        carregar_processed(conn)
        carregar_dimensoes(conn)
        carregar_price_processed(conn)
        carregar_curated(conn)
        log.info("Carga no PostgreSQL concluída")
    finally:
        conn.close()


if __name__ == "__main__":
    executar()
