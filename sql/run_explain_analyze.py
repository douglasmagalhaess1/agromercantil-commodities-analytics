"""Executa EXPLAIN ANALYZE nas queries analíticas e salva o plano de execução.

Uso:
    python sql/run_explain_analyze.py

Pré-requisitos:
    - PostgreSQL rodando com as tabelas criadas (01_create_tables.sql)
    - Índices criados (02_indexes.sql)
    - Dados carregados via ETL (python -m etl.load)
    - Variáveis PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD no .env
"""

import os
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DIRETORIO_PRINTS = Path("docs/prints")
DIRETORIO_PRINTS.mkdir(parents=True, exist_ok=True)

QUERIES = {
    "q6a_preco_medio_lag": Path("sql/queries/q6a_preco_medio_lag.sql"),
    "q6b_top5_produtos": Path("sql/queries/q6b_top5_produtos.sql"),
    "q6c_anomalias": Path("sql/queries/q6c_anomalias.sql"),
}

INDICES = [
    "idx_price_raw_produto_data",
    "idx_price_raw_data_referencia",
    "idx_price_raw_produto_preco",
    "idx_price_raw_regiao",
]


def conectar():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE", "agromercantil"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def ler_sql(caminho):
    texto = caminho.read_text(encoding="utf-8")
    linhas = [l for l in texto.splitlines() if not l.strip().startswith("--")]
    return "\n".join(linhas).strip().rstrip(";")


def executar_explain(cur, sql):
    cur.execute(f"EXPLAIN ANALYZE {sql}")
    return "\n".join(row[0] for row in cur.fetchall())


def dropar_indices(cur):
    for idx in INDICES:
        cur.execute(f"DROP INDEX IF EXISTS {idx}")


def recriar_indices(cur):
    caminho = Path("sql/schema/02_indexes.sql")
    sql = caminho.read_text(encoding="utf-8")
    cur.execute(sql)


def executar():
    conn = conectar()
    conn.autocommit = True
    resultados = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    resultados.append(f"EXPLAIN ANALYZE — Planos de execução das queries analíticas")
    resultados.append(f"Gerado em: {timestamp}")
    resultados.append("=" * 80)

    with conn.cursor() as cur:
        # Contagem de registros para contexto
        cur.execute("SELECT COUNT(*) FROM price_raw")
        total = cur.fetchone()[0]
        resultados.append(f"\nRegistros em price_raw: {total}\n")

        for nome, caminho in QUERIES.items():
            sql = ler_sql(caminho)
            resultados.append(f"\n{'=' * 80}")
            resultados.append(f"QUERY: {nome}")
            resultados.append(f"Arquivo: {caminho}")
            resultados.append("=" * 80)

            # --- SEM índices ---
            resultados.append(f"\n--- SEM índices (após DROP) ---\n")
            dropar_indices(cur)
            plano_sem = executar_explain(cur, sql)
            resultados.append(plano_sem)

            # --- COM índices ---
            resultados.append(f"\n--- COM índices (após CREATE) ---\n")
            recriar_indices(cur)
            plano_com = executar_explain(cur, sql)
            resultados.append(plano_com)

    conn.close()

    saida = DIRETORIO_PRINTS / "explain_analyze_output.txt"
    saida.write_text("\n".join(resultados), encoding="utf-8")
    print(f"Planos de execução salvos em: {saida}")
    print(f"\nResumo:")
    for nome in QUERIES:
        print(f"  ✓ {nome}")


if __name__ == "__main__":
    executar()
