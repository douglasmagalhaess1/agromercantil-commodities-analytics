"""Testes do scraper CONAB."""

from unittest.mock import MagicMock, patch

import pytest

from scraper.conab_scraper import (
    _expandir_colunas_com_colspan,
    _limpar_valor_monetario,
    _extrair_unidade,
    descobrir_links_boletins,
    extrair_tabela_precos,
)


# --- _limpar_valor_monetario ---

def test_limpar_valor_real_com_unidade():
    assert _limpar_valor_monetario("R$ 123,45/sc") == 123.45


def test_limpar_valor_milhar_brasileiro():
    assert _limpar_valor_monetario("1.234,56") == 1234.56


def test_limpar_valor_simples():
    assert _limpar_valor_monetario("87,50") == 87.50


def test_limpar_valor_vazio():
    assert _limpar_valor_monetario("") is None


def test_limpar_valor_texto_puro():
    assert _limpar_valor_monetario("sem numero") is None


# --- _extrair_unidade ---

def test_extrair_unidade_saca():
    assert _extrair_unidade("R$ 100,00/sc") == "saca"


def test_extrair_unidade_kg():
    assert _extrair_unidade("50,00/kg") == "kg"


def test_extrair_unidade_tonelada():
    assert _extrair_unidade("2.500,00/t") == "tonelada"


def test_extrair_unidade_default():
    assert _extrair_unidade("100,00") == "saca"


# --- extrair_tabela_precos (parsing de HTML) ---

def test_parsing_tabela_com_precos():
    from bs4 import BeautifulSoup

    html = """
    <table>
        <caption>Preços da Soja</caption>
        <tr><th>Regiao</th><th>Data</th><th>Preco</th></tr>
        <tr><td>Mato Grosso</td><td>01/03/2024</td><td>R$ 125,30/sc</td></tr>
        <tr><td>Goias</td><td>01/03/2024</td><td>R$ 130,00/sc</td></tr>
    </table>
    """
    sopa = BeautifulSoup(html, "html.parser")
    registros = extrair_tabela_precos(sopa, "soja")
    assert len(registros) == 2
    assert registros[0]["regiao"] == "Mato Grosso"
    assert registros[0]["preco"] == 125.30
    assert registros[1]["preco"] == 130.00


def test_parsing_tabela_sem_precos():
    from bs4 import BeautifulSoup

    html = """
    <table>
        <caption>Calendario de eventos</caption>
        <tr><th>Evento</th><th>Data</th></tr>
        <tr><td>Feira</td><td>10/05/2024</td></tr>
    </table>
    """
    sopa = BeautifulSoup(html, "html.parser")
    registros = extrair_tabela_precos(sopa, "soja")
    assert registros == []


# --- _expandir_colunas_com_colspan ---

def test_expandir_colspan():
    from bs4 import BeautifulSoup

    html = """
    <table>
        <tr><th>Regiao</th><th colspan="3">Preco</th></tr>
        <tr><td>MT</td><td>100</td><td>110</td><td>120</td></tr>
    </table>
    """
    sopa = BeautifulSoup(html, "html.parser")
    tabela = sopa.find("table")
    colunas = _expandir_colunas_com_colspan(tabela)
    assert colunas == ["Regiao", "Preco", "Preco", "Preco"]


# --- descobrir_links_boletins (mock de requests) ---

@patch("scraper.conab_scraper.requisitar_com_retry")
def test_descobrir_links_encontra_boletins(mock_req):
    mock_resp = MagicMock()
    mock_resp.headers = {"Content-Type": "text/html; charset=utf-8"}
    mock_resp.apparent_encoding = "utf-8"
    mock_resp.text = """
    <html><body>
        <a href="/info-agro/soja-relatorio.html">Soja - Relatorio</a>
        <a href="/info-agro/milho-boletim.html">Milho boletim</a>
        <a href="/noticias/noticia-1">Noticia qualquer</a>
    </body></html>
    """
    mock_req.return_value = mock_resp

    sessao = MagicMock()
    links = descobrir_links_boletins(sessao, "http://fake-url.com")

    assert len(links) == 2
    assert any("soja" in l["produto"] for l in links)
    assert any("milho" in l["produto"] for l in links)


@patch("scraper.conab_scraper.requisitar_com_retry")
def test_descobrir_links_pagina_vazia(mock_req):
    mock_resp = MagicMock()
    mock_resp.headers = {"Content-Type": "text/html; charset=utf-8"}
    mock_resp.apparent_encoding = "utf-8"
    mock_resp.text = "<html><body><p>Pagina sem links</p></body></html>"
    mock_req.return_value = mock_resp

    sessao = MagicMock()
    links = descobrir_links_boletins(sessao, "http://fake-url.com")
    assert links == []