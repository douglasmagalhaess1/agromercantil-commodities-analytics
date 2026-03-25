"""Dashboard Streamlit — Preços de Commodities Agrícolas (CONAB)."""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# -- Paleta Agromercantil --
COR_PRIMARIA = "#2f6209"
COR_SECUNDARIA = "#388e3c"
COR_FUNDO = "#f9fbfd"
COR_CARD = "#ffffff"
COR_TEXTO = "#212529"
COR_TEXTO_MUDO = "#6c757d"
COR_ACENTO = "#CBE206"
COR_ALERTA = "#ff9800"
COR_ERRO = "#f44336"
COR_BORDA = "#e0e0e0"

PALETA_GRAFICOS = [COR_PRIMARIA, COR_SECUNDARIA, COR_ACENTO, "#ff9800", "#7b1fa2", "#00838f", "#c62828"]

st.set_page_config(
    page_title="Agromercantil — Commodities",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

DIRETORIO_CURATED = Path("data/curated")


# ── CSS Global ──────────────────────────────────────────────────────────────

def injetar_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    :root {
        --color-primary: #2f6209;
        --color-secondary: #388e3c;
        --color-accent: #CBE206;
    }

    /* H1, H2, H3, H4 Herdam Primária */
    h1, h2, h3, h4 {
        color: var(--color-primary);
    }
    
    /* Foco e Acessibilidade */
    div[data-testid="stTextInput"] input:focus,
    div[data-testid="stTextInput"] input:focus-visible,
    div[data-testid="stDateInputField"] input:focus,
    div[data-testid="stDateInputField"] input:focus-visible,
    button:focus,
    button:focus-visible,
    [data-baseweb="input"]:focus-within,
    [data-baseweb="select"]:focus-within {
        box-shadow: 0 0 0 2px var(--color-accent) !important;
        border-color: var(--color-accent) !important;
        outline: none !important;
    }

    /* Reset geral */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        color: #212529;
    }

    /* Fundo principal */
    .stApp {
        background-color: #f9fbfd;
    }

    /* Barra de filtros */
    div[data-testid="stTextInput"] input,
    div[data-testid="stDateInputField"] input {
        background: #ffffff !important;
        color: #212529 !important;
        border: 1px solid #d5dde6 !important;
        border-radius: 10px !important;
        min-height: 44px;
    }
    div[data-testid="stTextInput"] input::placeholder {
        color: #6c757d !important;
    }
    div[data-testid="stPopover"] > button {
        min-height: 44px;
        border-radius: 10px !important;
        border: 1px solid #d5dde6 !important;
        background: #ffffff !important;
        color: #212529 !important;
        font-weight: 600;
    }
    [data-baseweb="select"] {
        border: 1px solid #d5dde6 !important;
        border-radius: 10px !important;
        background: #ffffff !important;
        min-height: 40px;
    }
    [data-baseweb="select"] > div {
        border: none !important;
        box-shadow: none !important;
        background: transparent !important;
    }
    [data-baseweb="select"] input {
        border: none !important;
        box-shadow: none !important;
        outline: none !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e0e0e0;
    }
    section[data-testid="stSidebar"] .stMarkdown h1,
    section[data-testid="stSidebar"] .stMarkdown h2,
    section[data-testid="stSidebar"] .stMarkdown h3 {
        color: var(--color-primary);
        font-weight: 600;
        border-left: 3px solid var(--color-accent);
        padding-left: 0.5rem;
    }

    /* Header */
    .dashboard-header {
        background: linear-gradient(135deg, var(--color-primary) 0%, #3f7417 100%);
        padding: 2rem 2.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        animation: fadeInDown 0.6s ease-out;
    }
    .dashboard-header h1 {
        color: #ffffff;
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0 0 0.3rem 0;
    }
    .dashboard-header p {
        color: #ffffff;
        font-size: 1rem;
        font-weight: 500;
        margin: 0;
    }
    .dashboard-header .fonte-badge {
        display: inline-block;
        background: var(--color-accent);
        color: var(--color-primary);
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 700;
        margin-top: 0.6rem;
    }

    /* Cards de métricas */
    .metric-card {
        background: #ffffff;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.07), 0 2px 4px -1px rgba(0,0,0,0.04);
        border: 1px solid #e0e0e0;
        text-align: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        animation: fadeInUp 0.5s ease-out;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 16px -4px rgba(0,0,0,0.1), 0 4px 8px -2px rgba(0,0,0,0.06);
    }
    .metric-card .metric-label {
        font-size: 0.82rem;
        font-weight: 500;
        color: #6c757d;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.4rem;
    }
    .metric-card .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: var(--color-primary);
    }
    .metric-card .metric-value.green {
        color: var(--color-secondary);
    }
    .metric-card {
        border-top: 3px solid var(--color-accent);
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background: #ffffff;
        border-radius: 8px;
        padding: 0.3rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px;
        padding: 0.6rem 1.2rem;
        font-weight: 500;
        font-size: 0.9rem;
        color: #6c757d;
        transition: all 0.2s ease;
    }
    .stTabs [aria-selected="true"] {
        background-color: var(--color-primary) !important;
        color: #ffffff !important;
        font-weight: 600;
        border-bottom: 3px solid var(--color-accent) !important;
    }
    .stTabs [data-baseweb="tab-highlight"] {
        display: none;
    }

    /* Multiselect / Filter Chips */
    .stMultiSelect [data-baseweb="tag"] {
        background-color: var(--color-primary);
        color: #ffffff;
        border-radius: 16px;
        padding: 4px 8px;
        border: none;
    }
    .stMultiSelect [data-baseweb="tag"] span {
        color: #ffffff !important;
        font-weight: 500;
    }
    .stMultiSelect [data-baseweb="tag"] svg {
        fill: #ffffff !important;
    }

    /* Dataframe */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }

    /* Animações */
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(16px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    @keyframes fadeInDown {
        from { opacity: 0; transform: translateY(-16px); }
        to   { opacity: 1; transform: translateY(0); }
    }

    /* Esconde o header padrão do Streamlit */
    header[data-testid="stHeader"] {
        background: transparent;
    }

    /* Divider */
    hr {
        border-color: #e0e0e0;
        margin: 1.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)


# ── Layout de gráficos Plotly ───────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    font=dict(family="Inter, sans-serif", color=COR_TEXTO, size=13),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis=dict(
        gridcolor=COR_BORDA,
        linecolor=COR_BORDA,
        zerolinecolor=COR_BORDA,
        tickfont=dict(color=COR_TEXTO),
        title=dict(font=dict(color=COR_TEXTO)),
        tickformat="%d/%m/%Y",
        automargin=True,
    ),
    yaxis=dict(
        gridcolor=COR_BORDA,
        linecolor=COR_BORDA,
        zerolinecolor=COR_BORDA,
        tickfont=dict(color=COR_TEXTO),
        title=dict(font=dict(color=COR_TEXTO)),
        automargin=True,
    ),
    legend=dict(font=dict(size=12, color=COR_TEXTO)),
    margin=dict(l=60, r=30, t=70, b=60),
    height=460,
    hoverlabel=dict(bgcolor=COR_CARD, font_size=13, font_color=COR_TEXTO),
)


def aplicar_layout(fig, titulo=""):
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text=titulo, font=dict(size=16, color=COR_TEXTO)),
        hovermode="closest",
    )
    return fig


# ── Conexão e dados ─────────────────────────────────────────────────────────

def conectar_pg():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DATABASE", "agromercantil"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
    )


@st.cache_data(ttl=300)
def carregar_precos_pg():
    try:
        conn = conectar_pg()
        df = pd.read_sql("SELECT * FROM price_raw ORDER BY data_referencia", conn)
        conn.close()
        return df
    except Exception:
        return None


@st.cache_data(ttl=300)
def carregar_curated_csv():
    caminho = DIRETORIO_CURATED / "resumo_precos.csv"
    if caminho.exists():
        return pd.read_csv(caminho)
    return None


def carregar_dados():
    df = carregar_precos_pg()
    if df is not None and not df.empty:
        return df, "PostgreSQL"
    df = carregar_curated_csv()
    if df is not None:
        return df, "CSV local"
    return pd.DataFrame(), "nenhuma"


# ── Componentes ─────────────────────────────────────────────────────────────

def render_header(fonte):
    st.markdown(f"""
    <div class="dashboard-header">
        <h1>🌾 Painel Agromercantil — Commodities</h1>
        <p>Painel de Monitoramento de Commodities Brasileiras (Dados: CONAB)</p>
        <span class="fonte-badge">⚡ Fonte: {fonte}</span>
    </div>
    """, unsafe_allow_html=True)


def render_metrica(label, valor, verde=False):
    classe = "green" if verde else ""
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value {classe}">{valor}</div>
    </div>
    """


def mostrar_metricas(df):
    total = f"{len(df):,}".replace(",", ".")
    produtos = df["produto"].nunique() if "produto" in df.columns else 0
    regioes = df["regiao"].nunique() if "regiao" in df.columns else 0

    if "preco" in df.columns:
        preco_txt = f"R$ {df['preco'].mean():.2f}"
    elif "preco_medio" in df.columns:
        preco_txt = f"R$ {df['preco_medio'].mean():.2f}"
    else:
        preco_txt = "—"

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(render_metrica("Total de registros", total), unsafe_allow_html=True)
    with col2:
        st.markdown(render_metrica("Produtos", produtos), unsafe_allow_html=True)
    with col3:
        st.markdown(render_metrica("Regioes", regioes), unsafe_allow_html=True)
    with col4:
        st.markdown(render_metrica("Preco medio", preco_txt, verde=True), unsafe_allow_html=True)


def estilizar_tabela_zebrada(df):
    if df.empty:
        return df

    df_exibicao = df.reset_index(drop=True).copy()

    # Formata colunas monetárias como R$ 0.000,00
    colunas_monetarias = [c for c in df_exibicao.columns
                          if c in ("preco", "preco_medio", "preco_min", "preco_max")]
    for col in colunas_monetarias:
        df_exibicao[col] = pd.to_numeric(df_exibicao[col], errors="coerce").map(
            lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(v) else "—"
        )

    def _zebra(linha):
        cor = "#ffffff" if linha.name % 2 == 0 else "#f3f8ed"
        return [f"background-color: {cor}; color: {COR_TEXTO};"] * len(linha)

    return df_exibicao.style.apply(_zebra, axis=1)


def barra_de_filtros(df):
    st.markdown("### 🔍 Pesquisa e Filtros")
    
    # Layout responsivo
    col_busca, col_filtros, col_data = st.columns([2, 1, 1])

    with col_busca:
        termo_busca = st.text_input("Buscar Produto ou Região", placeholder="🔎 Pesquise por soja, milho, sul, etc...", label_visibility="collapsed")

    produtos = sorted(df["produto"].dropna().unique()) if "produto" in df.columns else []
    regioes = sorted(df["regiao"].dropna().unique()) if "regiao" in df.columns else []

    with col_filtros:
        # Consolidação de Filtros usando Popover para as opções
        with st.popover("⚙️ Filtros Avançados", use_container_width=True):
            st.markdown("**Selecione as opções desejadas:**")
            sel_produtos = st.multiselect("Produto", produtos, default=[])
            sel_regioes = st.multiselect("Região", regioes, default=[])
            
    with col_data:
        # Date Range Picker Unificado
        if "data_referencia" in df.columns:
            try:
                min_date = pd.to_datetime(df["data_referencia"]).min().date()
                max_date = pd.to_datetime(df["data_referencia"]).max().date()
                datas = st.date_input(
                    "Período",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    label_visibility="collapsed",
                    format="DD/MM/YYYY",
                )
            except Exception:
                datas = []
        else:
            datas = []

    # Estado de Filtros (Chips - já renderizados nativamente nos próximos multiselects ou custom logic)
    # Como os filtros estão dentro de popovers, precisamos exibi-los embaixo da barra de pesquisa 
    # como os Filter Chips de forma integrada.
    agrupado = sel_produtos + sel_regioes
    if agrupado:
        # Usamos um multiselect no container principal apenas visualmente (ou para remoção fácil) 
        # para simular perfeitamente o comportamento de "Chips" descartáveis que afeta a session state,
        # porém mais simples é apenas filtrar.
        sel_chips = st.multiselect("Filtros Ativos", agrupado, default=agrupado, label_visibility="collapsed")
        # Se usuário fechar a tag no chip, temos de atualizar sel_produtos
        sel_produtos = [p for p in sel_chips if p in produtos]
        sel_regioes = [r for r in sel_chips if r in regioes]

    st.markdown("---")

    # Aplicação dos Filtros
    if termo_busca:
        mascara_busca = pd.Series(False, index=df.index)
        if "produto" in df.columns:
            mascara_busca = mascara_busca | df["produto"].astype(str).str.contains(termo_busca, case=False, na=False)
        if "regiao" in df.columns:
            mascara_busca = mascara_busca | df["regiao"].astype(str).str.contains(termo_busca, case=False, na=False)
        df = df[mascara_busca]

    if sel_produtos:
        df = df[df["produto"].isin(sel_produtos)]

    if sel_regioes:
        df = df[df["regiao"].isin(sel_regioes)]

    if len(datas) == 2 and "data_referencia" in df.columns:
        df_temp = df.copy()
        df_temp["data_ref_temp"] = pd.to_datetime(df_temp["data_referencia"]).dt.date
        df = df[(df_temp["data_ref_temp"] >= datas[0]) & (df_temp["data_ref_temp"] <= datas[1])]

    # Rótulos automáticos: ativa por padrão quando o volume está baixo.
    qtd_pontos = len(df)
    limiar_rotulos = 60
    auto_rotulos = qtd_pontos <= limiar_rotulos
    faixa = "baixo" if auto_rotulos else "alto"
    col_toggle, col_hint = st.columns([1, 3])
    with col_toggle:
        mostrar_rotulos = st.toggle(
            "Mostrar rótulos",
            value=auto_rotulos,
            key=f"mostrar_rotulos_{faixa}",
            help=f"Ativação automática quando houver até {limiar_rotulos} pontos filtrados.",
        )
    with col_hint:
        st.caption(f"Pontos filtrados: {qtd_pontos}. Modo automático sugerido: {'Ligado' if auto_rotulos else 'Desligado'}.")

    return df, mostrar_rotulos


# ── Gráficos ────────────────────────────────────────────────────────────────

def grafico_evolucao_precos(df, mostrar_rotulos=False):
    if "data_referencia" not in df.columns or "preco" not in df.columns:
        st.info("Dados insuficientes para gráfico de evolução.")
        return

    df_g = df.dropna(subset=["data_referencia", "preco"]).copy()
    df_g["data_referencia"] = pd.to_datetime(df_g["data_referencia"], errors="coerce")
    df_g = df_g.dropna(subset=["data_referencia"])

    fig = px.line(
        df_g, x="data_referencia", y="preco", color="produto",
        color_discrete_sequence=PALETA_GRAFICOS,
        labels={"data_referencia": "Data", "preco": "Preço (R$)", "produto": "Produto"},
    )
    modo_linha = "lines+markers+text" if mostrar_rotulos else "lines+markers"
    fig.update_traces(
        line=dict(width=2.5),
        mode=modo_linha,
        marker=dict(size=7),
        text=df_g["preco"].map(lambda x: f"R$ {x:.2f}") if mostrar_rotulos else None,
        textposition="top center",
        textfont=dict(size=9, color=COR_TEXTO_MUDO),
        hovertemplate="Produto: %{fullData.name}<br>Data: %{x|%d/%m/%Y}<br>Preço: R$ %{y:.2f}<extra></extra>",
    )
    aplicar_layout(fig, "Evolução dos preços por produto")

    st.plotly_chart(fig, use_container_width=True, theme=None)


def grafico_media_movel(df, mostrar_rotulos=False):
    if "data_referencia" not in df.columns or "preco" not in df.columns:
        st.info("Dados insuficientes para média móvel.")
        return

    df_mm = df.dropna(subset=["data_referencia", "preco"]).copy()
    df_mm["data_referencia"] = pd.to_datetime(df_mm["data_referencia"], errors="coerce")
    df_mm = df_mm.dropna(subset=["data_referencia"])

    mensal = (df_mm.groupby([pd.Grouper(key="data_referencia", freq="MS"), "produto"])
              ["preco"].mean().reset_index())
    mensal = mensal.sort_values(["produto", "data_referencia"])
    mensal["media_movel_3m"] = (mensal.groupby("produto")["preco"]
                                .transform(lambda x: x.rolling(3, min_periods=1).mean()))

    fig = go.Figure()
    produtos = mensal["produto"].unique()
    for i, produto in enumerate(produtos):
        dados_p = mensal[mensal["produto"] == produto]
        cor = PALETA_GRAFICOS[i % len(PALETA_GRAFICOS)]
        fig.add_trace(go.Scatter(
            x=dados_p["data_referencia"], y=dados_p["preco"],
            mode="lines+markers", name=f"{produto} — mensal",
            line=dict(color=cor, width=1, dash="dot"), opacity=0.5,
            hovertemplate="Produto: %{fullData.name}<br>Data: %{x|%d/%m/%Y}<br>Preço mensal: R$ %{y:.2f}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            x=dados_p["data_referencia"], y=dados_p["media_movel_3m"],
            mode="lines+markers+text" if mostrar_rotulos else "lines+markers", name=f"{produto} — MM 3m",
            line=dict(color=cor, width=3),
            marker=dict(size=6),
            text=dados_p["media_movel_3m"].map(lambda x: f"R$ {x:.2f}") if mostrar_rotulos else None,
            textposition="top center",
            textfont=dict(size=9, color=COR_TEXTO_MUDO),
            hovertemplate="Produto: %{fullData.name}<br>Data: %{x|%d/%m/%Y}<br>Média móvel: R$ %{y:.2f}<extra></extra>",
        ))

    aplicar_layout(fig, "Preço mensal com média móvel de 3 meses")

    st.plotly_chart(fig, use_container_width=True, theme=None)


def grafico_preco_por_regiao(df, mostrar_rotulos=False):
    coluna_preco = "preco" if "preco" in df.columns else "preco_medio"
    if coluna_preco not in df.columns or "regiao" not in df.columns:
        return

    media_regiao = df.groupby("regiao")[coluna_preco].mean().sort_values(ascending=True).tail(15)
    fig = px.bar(
        x=media_regiao.values, y=media_regiao.index, orientation="h",
        color_discrete_sequence=[COR_PRIMARIA],
        labels={"x": "Preço médio (R$)", "y": "Região"},
    )
    fig.update_traces(
        marker_line_width=0,
        opacity=0.9,
        text=[f"R$ {v:.2f}" for v in media_regiao.values] if mostrar_rotulos else None,
        textposition="auto",
        textfont=dict(size=10),
        insidetextanchor="middle",
        hovertemplate="Região: %{y}<br>Preço médio: R$ %{x:.2f}<extra></extra>",
    )
    altura_dinamica = max(420, len(media_regiao) * 52)
    fig.update_layout(
        height=altura_dinamica,
        margin=dict(l=190, r=60, t=70, b=60),
        yaxis=dict(automargin=True, tickfont=dict(size=12), categoryorder="total ascending"),
    )
    aplicar_layout(fig, "Top 15 regiões — preço médio")

    st.plotly_chart(fig, use_container_width=True, theme=None)


def grafico_distribuicao(df, mostrar_rotulos=False):
    coluna_preco = "preco" if "preco" in df.columns else "preco_medio"
    if coluna_preco not in df.columns:
        return

    tem_produto = "produto" in df.columns and df["produto"].nunique() > 1
    n_registros = len(df)
    nbins = max(5, min(30, n_registros // 3))

    fig = px.histogram(
        df, x=coluna_preco, color="produto" if tem_produto else None,
        nbins=nbins, color_discrete_sequence=PALETA_GRAFICOS,
        labels={coluna_preco: "Faixa de preço (R$)", "count": "Recorrência"},
    )
    fig.update_traces(
        opacity=0.85,
        texttemplate="%{y}",
        textposition="outside",
        textfont=dict(size=11, color=COR_TEXTO),
        hovertemplate="Faixa de preço: R$ %{x:.2f}<br>Recorrência: %{y}<extra></extra>",
    )
    fig.update_layout(
        uniformtext_minsize=8, uniformtext_mode="hide",
        yaxis_title="Recorrência",
        xaxis_title="Faixa de preço (R$)",
        bargap=0.08,
    )
    aplicar_layout(fig, "Recorrência de preços por faixa")

    st.plotly_chart(fig, use_container_width=True, theme=None)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    injetar_css()

    df, fonte = carregar_dados()

    if df.empty:
        render_header("nenhuma")
        st.error("Nenhum dado disponível. Execute o scraper e o ETL primeiro.")
        st.code("python -m scraper.conab_scraper\npython -m etl.transform\npython -m etl.load")
        return

    render_header(fonte)
    mostrar_metricas(df)

    st.markdown("<br>", unsafe_allow_html=True)
    df_filtrado, mostrar_rotulos = barra_de_filtros(df)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Evolução", "📊 Média Móvel", "🗺️ Por Região",
        "📉 Distribuição", "📋 Dados Brutos",
    ])

    with tab1:
        grafico_evolucao_precos(df_filtrado, mostrar_rotulos)
    with tab2:
        grafico_media_movel(df_filtrado, mostrar_rotulos)
    with tab3:
        grafico_preco_por_regiao(df_filtrado, mostrar_rotulos)
    with tab4:
        grafico_distribuicao(df_filtrado, mostrar_rotulos)
    with tab5:
        st.dataframe(estilizar_tabela_zebrada(df_filtrado), use_container_width=True, height=500)


if __name__ == "__main__":
    main()
