"""Scraper de preços de commodities agrícolas da CONAB."""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from scraper.utils import (
    expandir_colunas_com_colspan,
    extrair_unidade,
    limpar_valor_monetario,
    tabela_parece_preco,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

URL_BASE = "https://www.conab.gov.br/info-agro/analises-do-mercado-agropecuario-e-extrativismo-florestal"

PRODUTOS = {
    "soja": "soja",
    "milho": "milho",
    "cafe": "cafe",
}

DIRETORIO_RAW = Path("data/raw")

# As tabelas da CONAB usam colspan nos cabeçalhos e não possuem atributos id ou
# class estáveis — os seletores CSS que funcionam para o boletim de soja quebram
# no de milho porque a estrutura muda entre tipos de relatório. A única âncora
# confiável é procurar o <caption> ou o texto do <th> e reconstruir a tabela a
# partir das posições relativas das células, contando os colspan manualmente.


def criar_sessao():
    sessao = requests.Session()
    sessao.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return sessao


def requisitar_com_retry(sessao, url, tentativas=3, espera=2):
    for i in range(tentativas):
        try:
            resp = sessao.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            if resp.status_code >= 500 and i < tentativas - 1:
                log.warning("Erro %s, tentativa %d/%d — aguardando %ds",
                            resp.status_code, i + 1, tentativas, espera)
                time.sleep(espera)
                espera *= 2
                continue
            raise
        except requests.exceptions.RequestException as e:
            if i < tentativas - 1:
                log.warning("Falha de conexão: %s — retry %d/%d", e, i + 1, tentativas)
                time.sleep(espera)
                continue
            raise
    return None


def descobrir_links_boletins(sessao, url_pagina):
    resp = requisitar_com_retry(sessao, url_pagina)
    # Alguns relatórios legados vêm em ISO-8859-1 em vez de UTF-8
    if "iso-8859-1" in resp.headers.get("Content-Type", "").lower():
        resp.encoding = "iso-8859-1"
    else:
        resp.encoding = resp.apparent_encoding

    sopa = BeautifulSoup(resp.text, "lxml")
    links = []
    for a in sopa.select("a[href]"):
        href = a["href"]
        texto = a.get_text(strip=True).lower()
        if any(p in texto for p in PRODUTOS.values()) and href.endswith((".html", ".php", "/")):
            url_completa = href if href.startswith("http") else f"https://www.conab.gov.br{href}"
            links.append({"produto": texto, "url": url_completa})
    log.info("Encontrados %d links de boletins", len(links))
    return links


def extrair_tabela_precos(sopa, produto):
    """Ponto de entrada para parsing de uma página de boletim.

    Localiza tabelas pelo texto do caption/th porque a CONAB não usa ids ou
    classes estáveis. Reconstrói cabeçalhos expandindo colspan manualmente.
    """
    tabelas = sopa.find_all("table")
    registros = []

    for tabela in tabelas:
        caption = tabela.find("caption")
        texto_caption = caption.get_text(strip=True).lower() if caption else ""
        headers_th = [th.get_text(strip=True) for th in tabela.find_all("th")]
        texto_headers = " ".join(headers_th).lower()

        if not tabela_parece_preco(texto_caption, texto_headers, produto):
            continue

        colunas = expandir_colunas_com_colspan(tabela)
        linhas_dados = tabela.find_all("tr")

        for tr in linhas_dados:
            tds = tr.find_all("td")
            if not tds or len(tds) < 3:
                continue
            registro = _parsear_linha_preco(tds, colunas, produto)
            if registro:
                registros.append(registro)

    log.info("Produto '%s': %d registros extraídos", produto, len(registros))
    return registros


def _parsear_linha_preco(tds, colunas, produto):
    try:
        textos = [td.get_text(strip=True) for td in tds]
        # A CONAB mistura formatos: "R$ 123,45/sc", "123.45", "123,45"
        regiao = textos[0] if textos[0] else None
        preco_texto = textos[-1] if len(textos) > 1 else None
        data_texto = textos[1] if len(textos) > 2 else None

        if not preco_texto or not regiao:
            return None

        preco = limpar_valor_monetario(preco_texto)
        if preco is None:
            return None

        return {
            "produto": produto,
            "regiao": regiao,
            "data_referencia": data_texto,
            "preco": preco,
            "unidade": extrair_unidade(preco_texto),
            "data_coleta": datetime.now().isoformat(),
        }
    except (IndexError, ValueError):
        return None


def salvar_raw(registros, produto):
    DIRETORIO_RAW.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    caminho = DIRETORIO_RAW / f"conab_{produto}_{timestamp}.json"
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    log.info("Salvo %s (%d registros)", caminho, len(registros))
    return caminho


DELAY_ENTRE_REQUISICOES = 2  # segundos entre páginas (rate limiting)


def coletar_produto(sessao, url_boletim, produto):
    """Acessa uma página de boletim, extrai tabelas de preços e salva em raw."""
    resp = requisitar_com_retry(sessao, url_boletim)
    if not resp:
        log.warning("Sem resposta para %s — pulando", url_boletim)
        return []

    if "iso-8859-1" in resp.headers.get("Content-Type", "").lower():
        resp.encoding = "iso-8859-1"
    else:
        resp.encoding = resp.apparent_encoding

    sopa = BeautifulSoup(resp.text, "lxml")
    return extrair_tabela_precos(sopa, produto)


def executar():
    sessao = criar_sessao()
    log.info("Iniciando coleta — URL base: %s", URL_BASE)

    links = descobrir_links_boletins(sessao, URL_BASE)
    if not links:
        log.warning("Nenhum boletim encontrado na página principal")
        return

    for i, link in enumerate(links):
        produto = link["produto"]
        url = link["url"]
        log.info("[%d/%d] Coletando '%s' — %s", i + 1, len(links), produto, url)

        registros = coletar_produto(sessao, url, produto)
        if registros:
            salvar_raw(registros, produto)
        else:
            log.warning("Nenhum registro extraído de '%s'", url)

        # Rate limiting entre requisições
        if i < len(links) - 1:
            time.sleep(DELAY_ENTRE_REQUISICOES)

    log.info("Coleta finalizada — %d boletins processados", len(links))


if __name__ == "__main__":
    executar()


def coletar_produto(sessao, produto, url):
    resp = requisitar_com_retry(sessao, url)
    if "iso-8859-1" in resp.headers.get("Content-Type", "").lower():
        resp.encoding = "iso-8859-1"
    else:
        resp.encoding = resp.apparent_encoding

    sopa = BeautifulSoup(resp.text, "lxml")
    registros = extrair_tabela_precos(sopa, produto)
    if registros:
        salvar_raw(registros, produto)
    return registros


def executar():
    """Ponto de entrada principal — coleta preços de todos os produtos configurados."""
    sessao = criar_sessao()
    total = 0

    links = descobrir_links_boletins(sessao, URL_BASE)
    if not links:
        log.warning("Nenhum link de boletim encontrado. Verifique se a URL base mudou.")
        return

    for info in links:
        for chave, produto in PRODUTOS.items():
            if chave in info["produto"]:
                registros = coletar_produto(sessao, produto, info["url"])
                total += len(registros)

    log.info("Coleta finalizada — %d registros totais", total)


if __name__ == "__main__":
    executar()
