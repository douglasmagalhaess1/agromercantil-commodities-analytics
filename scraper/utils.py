"""Helpers de parsing para o scraper CONAB."""

import re


def expandir_colunas_com_colspan(tabela):
    # Cabeçalhos com colspan precisam ser expandidos para alinhar com as células
    # de dados. Ex: <th colspan="3">Preço</th> vira ["Preço", "Preço", "Preço"].
    colunas = []
    primeira_tr = tabela.find("tr")
    if not primeira_tr:
        return colunas
    for celula in primeira_tr.find_all(["th", "td"]):
        texto = celula.get_text(strip=True)
        span = int(celula.get("colspan", 1))
        colunas.extend([texto] * span)
    return colunas


def limpar_valor_monetario(texto):
    texto = texto.replace("R$", "").replace("/sc", "").replace("/kg", "").strip()
    texto = re.sub(r"[^\d.,]", "", texto)
    if not texto:
        return None
    # Converte vírgula decimal brasileira para ponto
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")
    try:
        return round(float(texto), 2)
    except ValueError:
        return None


def extrair_unidade(texto):
    if "/sc" in texto.lower():
        return "saca"
    if "/kg" in texto.lower():
        return "kg"
    if "/t" in texto.lower():
        return "tonelada"
    return "saca"


def tabela_parece_preco(texto_caption, texto_headers, produto):
    termos_preco = ["preço", "preco", "cotação", "cotacao", "r$"]
    tem_preco = any(t in texto_caption or t in texto_headers for t in termos_preco)
    tem_produto = produto in texto_caption or produto in texto_headers
    return tem_preco or tem_produto
