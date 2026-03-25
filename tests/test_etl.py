"""Testes do ETL — transformacao de dados."""

import pandas as pd
import pytest

from etl.load import _parsear_regiao
from etl.transform import (
    normalizar_data,
    normalizar_preco,
    padronizar_produto,
    padronizar_regiao,
    transformar,
)


# --- padronizar_produto ---

def test_padronizar_produto_maiusculo():
    assert padronizar_produto("SOJA") == "soja"


def test_padronizar_produto_misto():
    assert padronizar_produto("  Milho ") == "milho"


def test_padronizar_produto_nulo():
    assert padronizar_produto(None) is None


def test_padronizar_produto_vazio():
    assert padronizar_produto("") is None


# --- padronizar_regiao ---

def test_padronizar_regiao_uppercase():
    assert padronizar_regiao("mato grosso") == "MATO GROSSO"


def test_padronizar_regiao_strip():
    assert padronizar_regiao("  Goias  ") == "GOIAS"


def test_padronizar_regiao_nulo():
    assert padronizar_regiao(None) is None


# --- normalizar_preco ---

def test_normalizar_preco_float():
    assert normalizar_preco(125.456) == 125.46


def test_normalizar_preco_int():
    assert normalizar_preco(100) == 100.0


def test_normalizar_preco_string_br():
    assert normalizar_preco("1.234,56") == 1234.56


def test_normalizar_preco_string_com_real():
    assert normalizar_preco("R$ 87,50/sc") == 87.50


def test_normalizar_preco_nulo():
    assert normalizar_preco(None) is None


def test_normalizar_preco_texto():
    assert normalizar_preco("invalido") is None


# --- normalizar_data ---

def test_normalizar_data_barra():
    assert normalizar_data("15/03/2024") == "2024-03-15"


def test_normalizar_data_iso():
    assert normalizar_data("2024-03-15") == "2024-03-15"


def test_normalizar_data_traco():
    assert normalizar_data("15-03-2024") == "2024-03-15"


def test_normalizar_data_mes_ano():
    assert normalizar_data("03/2024") == "2024-03-01"


def test_normalizar_data_nula():
    assert normalizar_data(None) is None


def test_normalizar_data_formato_invalido():
    assert normalizar_data("data-invalida") is None


# --- transformar (pipeline completo) ---

def test_transformar_remove_preco_nulo():
    registros = [
        {"produto": "soja", "regiao": "MT", "data_referencia": "01/01/2024", "preco": 120.0},
        {"produto": "soja", "regiao": "GO", "data_referencia": "01/01/2024", "preco": None},
    ]
    df = transformar(registros)
    assert len(df) == 1
    assert df.iloc[0]["preco"] == 120.0


def test_transformar_remove_duplicatas():
    registros = [
        {"produto": "soja", "regiao": "MT", "data_referencia": "01/01/2024", "preco": 120.0},
        {"produto": "soja", "regiao": "MT", "data_referencia": "01/01/2024", "preco": 120.0},
    ]
    df = transformar(registros)
    assert len(df) == 1


def test_transformar_lista_vazia():
    df = transformar([])
    assert df.empty


def test_transformar_padroniza_produto():
    registros = [
        {"produto": "SOJA", "regiao": "MT", "data_referencia": "01/01/2024", "preco": 120.0},
        {"produto": "  Milho ", "regiao": "GO", "data_referencia": "01/01/2024", "preco": 80.0},
    ]
    df = transformar(registros)
    assert list(df["produto"]) == ["soja", "milho"]


def test_transformar_corrige_tipo_data():
    registros = [
        {"produto": "cafe", "regiao": "MG", "data_referencia": "15/06/2024", "preco": 950.0},
    ]
    df = transformar(registros)
    assert df.iloc[0]["data_referencia"] == "2024-06-15"


# --- _parsear_regiao (load.py) ---

def test_parsear_regiao_com_uf():
    nome, uf = _parsear_regiao("SORRISO - MT")
    assert nome == "SORRISO"
    assert uf == "MT"


def test_parsear_regiao_sem_separador():
    nome, uf = _parsear_regiao("GOIAS")
    assert nome == "GOIAS"
    assert uf is None


def test_parsear_regiao_nula():
    nome, uf = _parsear_regiao(None)
    assert nome is None
    assert uf is None


def test_parsear_regiao_multiplos_hifens():
    nome, uf = _parsear_regiao("SAO JOSE DO RIO PRETO - SP")
    assert nome == "SAO JOSE DO RIO PRETO"
    assert uf == "SP"