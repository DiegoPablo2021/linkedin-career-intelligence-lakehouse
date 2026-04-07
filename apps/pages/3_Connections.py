import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    run_query,
    style_matplotlib,
)


st.set_page_config(page_title="Connections", layout="wide")
apply_app_theme()


@st.cache_data
def load_mart_connections_summary() -> pd.DataFrame:
    query = """
        select
            connection_year,
            connection_month,
            connection_year_month,
            total_connections,
            connections_with_email,
            unique_companies,
            unique_positions
        from main.mart_connections_summary
        order by connection_year, connection_month
    """
    return run_query(query)


@st.cache_data
def load_top_companies(limit: int = 10) -> pd.DataFrame:
    query = f"""
        select
            company_clean,
            count(*) as total_connections
        from main.int_connections_enriched
        where company_clean is not null
          and trim(company_clean) <> ''
        group by company_clean
        order by total_connections desc, company_clean asc
        limit {limit}
    """
    return run_query(query)


@st.cache_data
def load_top_positions(limit: int = 10) -> pd.DataFrame:
    query = f"""
        select
            position_clean,
            count(*) as total_connections
        from main.int_connections_enriched
        where position_clean is not null
          and trim(position_clean) <> ''
        group by position_clean
        order by total_connections desc, position_clean asc
        limit {limit}
    """
    return run_query(query)


@st.cache_data
def load_connections_overview() -> pd.DataFrame:
    query = """
        select
            count(*) as total_connections,
            sum(case when has_email then 1 else 0 end) as total_with_email,
            count(distinct company_clean) as total_unique_companies,
            count(distinct position_clean) as total_unique_positions
        from main.int_connections_enriched
    """
    return run_query(query)


@st.cache_data
def load_detailed_connections(limit: int = 200) -> pd.DataFrame:
    query = f"""
        select
            full_name,
            company,
            position,
            email_address,
            connected_on,
            connection_year_month,
            has_email
        from main.int_connections_enriched
        order by connected_on desc
        limit {limit}
    """
    return run_query(query)


def format_title_case(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().title()


st.title("Connections")
st.subheader("Análise da rede profissional")

df_summary = load_mart_connections_summary()
df_overview = load_connections_overview()
df_companies = load_top_companies()
df_positions = load_top_positions()
df_detail = load_detailed_connections()

if not df_companies.empty:
    df_companies["company_clean"] = df_companies["company_clean"].apply(format_title_case)

if not df_positions.empty:
    df_positions["position_clean"] = df_positions["position_clean"].apply(format_title_case)

if not df_detail.empty:
    df_detail["connected_on"] = pd.to_datetime(df_detail["connected_on"], errors="coerce")
    df_detail["company"] = df_detail["company"].fillna("").apply(format_title_case)
    df_detail["position"] = df_detail["position"].fillna("").apply(format_title_case)

if not df_overview.empty:
    overview_row = df_overview.iloc[0]

    total_connections = int(overview_row["total_connections"])
    total_with_email = int(overview_row["total_with_email"])
    total_unique_companies = int(overview_row["total_unique_companies"])
    total_unique_positions = int(overview_row["total_unique_positions"])
else:
    total_connections = 0
    total_with_email = 0
    total_unique_companies = 0
    total_unique_positions = 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de conexões", f"{total_connections}")
col2.metric("Conexões com e-mail", f"{total_with_email}")
col3.metric("Empresas únicas", f"{total_unique_companies}")
col4.metric("Cargos únicos", f"{total_unique_positions}")

st.divider()

st.markdown("## Evolução mensal da rede")

if not df_summary.empty:
    x_positions = list(range(len(df_summary)))
    x_labels = df_summary["connection_year_month"].astype(str).tolist()
    fig1, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(x_positions, df_summary["total_connections"], marker="o", color="#4EA1FF", linewidth=2.4)
    ax1.set_title("Conexões por mês")
    ax1.set_xlabel("Ano-Mês")
    ax1.set_ylabel("Total de conexões")
    apply_straight_xticks(ax1, x_labels, max_ticks=10)
    style_matplotlib(fig1, ax1)
    plt.tight_layout()
    st.pyplot(fig1)
else:
    st.warning("Não há dados disponíveis para o gráfico de evolução mensal.")

st.markdown("## Conexões com e-mail ao longo do tempo")

if not df_summary.empty:
    x_positions = list(range(len(df_summary)))
    x_labels = df_summary["connection_year_month"].astype(str).tolist()
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    ax2.bar(x_positions, df_summary["connections_with_email"], color="#26C6DA")
    ax2.set_title("Conexões com e-mail por mês")
    ax2.set_xlabel("Ano-Mês")
    ax2.set_ylabel("Quantidade")
    apply_straight_xticks(ax2, x_labels, max_ticks=10)
    style_matplotlib(fig2, ax2)
    plt.tight_layout()
    st.pyplot(fig2)
else:
    st.warning("Não há dados disponíveis para o gráfico de conexões com e-mail.")

st.markdown("## Distribuição da rede")

left, right = st.columns(2)

with left:
    st.markdown("### Top empresas")
    if not df_companies.empty:
        fig3, ax3 = plt.subplots(figsize=(10, 6))
        ax3.barh(df_companies["company_clean"], df_companies["total_connections"], color="#7C9CF5")
        ax3.set_title("Top 10 empresas")
        ax3.set_xlabel("Total de conexões")
        ax3.set_ylabel("Empresa")
        ax3.invert_yaxis()
        style_matplotlib(fig3, ax3)
        plt.tight_layout()
        st.pyplot(fig3)
    else:
        st.warning("Não há dados suficientes para top empresas.")

with right:
    st.markdown("### Top cargos")
    if not df_positions.empty:
        fig4, ax4 = plt.subplots(figsize=(10, 6))
        ax4.barh(df_positions["position_clean"], df_positions["total_connections"], color="#F2C14E")
        ax4.set_title("Top 10 cargos")
        ax4.set_xlabel("Total de conexões")
        ax4.set_ylabel("Cargo")
        ax4.invert_yaxis()
        style_matplotlib(fig4, ax4)
        plt.tight_layout()
        st.pyplot(fig4)
    else:
        st.warning("Não há dados suficientes para top cargos.")

st.divider()

st.markdown("## Tabelas de apoio")

tab1, tab2, tab3 = st.tabs(
    ["Resumo mensal", "Top empresas", "Detalhamento recente"]
)

with tab1:
    st.dataframe(df_summary, width="stretch")

with tab2:
    st.dataframe(df_companies, width="stretch")

with tab3:
    st.dataframe(df_detail, width="stretch")

st.markdown("## Leitura inicial dos dados")

if not df_summary.empty:
    best_idx = int(df_summary["total_connections"].idxmax())
    best_row = df_summary.iloc[best_idx]

    best_month = str(best_row["connection_year_month"])
    best_month_total = int(best_row["total_connections"])

    st.info(
        f"O período com maior volume de conexões foi **{best_month}**, com **{best_month_total}** conexões registradas."
    )

    if total_connections > 0:
        email_share = round((total_with_email / total_connections) * 100, 2)
        st.info(
            f"Do total da rede analisada, **{email_share}%** das conexões possuem e-mail disponível."
        )
else:
    st.info("Ainda não há dados suficientes para gerar insights automáticos.")
