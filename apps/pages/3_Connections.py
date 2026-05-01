import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_title_case,
    configure_page,
    format_title_case,
    load_query,
    render_dataframe,
    render_horizontal_bar_chart,
    render_metric_row,
    render_time_series_chart,
)


configure_page("Connections", "Análise da rede profissional")


def load_mart_connections_summary() -> pd.DataFrame:
    return load_query(
        """
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
    )


def load_top_companies(limit: int = 10) -> pd.DataFrame:
    return load_query(
        f"""
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
    )


def load_top_positions(limit: int = 10) -> pd.DataFrame:
    return load_query(
        f"""
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
    )


def load_connections_overview() -> pd.DataFrame:
    return load_query(
        """
        select
            count(*) as total_connections,
            sum(case when has_email then 1 else 0 end) as total_with_email,
            count(distinct company_clean) as total_unique_companies,
            count(distinct position_clean) as total_unique_positions
        from main.int_connections_enriched
        """
    )


def load_detailed_connections(limit: int = 200) -> pd.DataFrame:
    return load_query(
        f"""
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
    )

df_summary = load_mart_connections_summary()
df_overview = load_connections_overview()
df_companies = load_top_companies()
df_positions = load_top_positions()
df_detail = load_detailed_connections()

if not df_companies.empty:
    df_companies = apply_title_case(df_companies, ["company_clean"])

if not df_positions.empty:
    df_positions = apply_title_case(df_positions, ["position_clean"])

if not df_detail.empty:
    df_detail["connected_on"] = pd.to_datetime(df_detail["connected_on"], errors="coerce")
    df_detail = apply_title_case(df_detail, ["company", "position", "full_name"])

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

render_metric_row(
    [
        ("Total de conexões", total_connections),
        ("Conexões com e-mail", total_with_email),
        ("Empresas únicas", total_unique_companies),
        ("Cargos únicos", total_unique_positions),
    ]
)

st.divider()

st.markdown("## Evolução mensal da rede")

if not df_summary.empty:
    render_time_series_chart(
        df_summary,
        x_labels=df_summary["connection_year_month"].astype(str).tolist(),
        y_column="total_connections",
        title="Conexões por mês",
        x_label="Ano-Mês",
        y_label="Total de conexões",
        color="#4EA1FF",
    )
else:
    st.warning("Não há dados disponíveis para o gráfico de evolução mensal.")

st.markdown("## Conexões com e-mail ao longo do tempo")

if not df_summary.empty:
    render_time_series_chart(
        df_summary,
        x_labels=df_summary["connection_year_month"].astype(str).tolist(),
        y_column="connections_with_email",
        title="Conexões com e-mail por mês",
        x_label="Ano-Mês",
        y_label="Quantidade",
        color="#26C6DA",
        kind="bar",
    )
else:
    st.warning("Não há dados disponíveis para o gráfico de conexões com e-mail.")

st.markdown("## Distribuição da rede")

left, right = st.columns(2)

with left:
    st.markdown("### Top empresas")
    if not df_companies.empty:
        render_horizontal_bar_chart(
            df_companies,
            label_column="company_clean",
            value_column="total_connections",
            title="Top 10 empresas",
            x_label="Total de conexões",
            y_label="Empresa",
            color="#7C9CF5",
        )
    else:
        st.warning("Não há dados suficientes para top empresas.")

with right:
    st.markdown("### Top cargos")
    if not df_positions.empty:
        render_horizontal_bar_chart(
            df_positions,
            label_column="position_clean",
            value_column="total_connections",
            title="Top 10 cargos",
            x_label="Total de conexões",
            y_label="Cargo",
            color="#F2C14E",
        )
    else:
        st.warning("Não há dados suficientes para top cargos.")

st.divider()

st.markdown("## Tabelas de apoio")

tab1, tab2, tab3 = st.tabs(
    ["Resumo mensal", "Top empresas", "Detalhamento recente"]
)

with tab1:
    render_dataframe(df_summary)

with tab2:
    render_dataframe(df_companies)

with tab3:
    render_dataframe(df_detail)

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
