from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_FILE = Path(__file__).resolve()
PROJECT_ROOT = next(parent for parent in PROJECT_FILE.parents if (parent / "linkedin_career_intelligence").exists())
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.streamlit_utils import (
    apply_datetime_conversion,
    apply_title_case,
    configure_page,
    load_query,
    render_dataframe,
    render_horizontal_bar_chart,
    render_metric_row,
    render_time_series_chart,
    safe_float,
    safe_int,
)


configure_page("Career", "Análise da trajetória profissional")


def load_career_progression() -> pd.DataFrame:
    return load_query(
        """
        select
            start_year,
            start_month,
            start_year_month,
            total_positions_started,
            current_positions_started,
            unique_companies,
            unique_titles,
            avg_duration_months
        from main.mart_career_progression
        order by start_year, start_month
        """
    )


def load_positions_detail() -> pd.DataFrame:
    return load_query(
        """
        select
            company_name,
            title,
            location,
            started_on,
            finished_on,
            is_current,
            duration_months,
            career_track
        from main.int_positions_enriched
        order by started_on desc
        """
    )


def load_career_overview() -> pd.DataFrame:
    return load_query(
        """
        select
            count(*) as total_positions,
            sum(case when is_current then 1 else 0 end) as current_positions,
            count(distinct company_name_clean) as unique_companies,
            count(distinct title_clean) as unique_titles,
            avg(duration_months) as avg_duration_months
        from main.int_positions_enriched
        """
    )


def load_top_companies(limit: int = 10) -> pd.DataFrame:
    return load_query(
        f"""
        select
            company_name_clean,
            count(*) as total_positions
        from main.int_positions_enriched
        where company_name_clean is not null
          and trim(company_name_clean) <> ''
        group by company_name_clean
        order by total_positions desc, company_name_clean asc
        limit {limit}
        """
    )


def load_top_titles(limit: int = 10) -> pd.DataFrame:
    return load_query(
        f"""
        select
            title_clean,
            count(*) as total_positions
        from main.int_positions_enriched
        where title_clean is not null
          and trim(title_clean) <> ''
        group by title_clean
        order by total_positions desc, title_clean asc
        limit {limit}
        """
    )


df_progression = load_career_progression()
df_detail = load_positions_detail()
df_overview = load_career_overview()
df_top_companies = load_top_companies()
df_top_titles = load_top_titles()

if not df_top_companies.empty:
    df_top_companies = apply_title_case(df_top_companies, ["company_name_clean"])

if not df_top_titles.empty:
    df_top_titles = apply_title_case(df_top_titles, ["title_clean"])

if not df_detail.empty:
    df_detail = apply_title_case(df_detail, ["company_name", "title", "location"])
    df_detail = apply_datetime_conversion(df_detail, ["started_on", "finished_on"])

if not df_overview.empty:
    overview_row = df_overview.iloc[0]

    total_positions = safe_int(overview_row["total_positions"])
    current_positions = safe_int(overview_row["current_positions"])
    unique_companies = safe_int(overview_row["unique_companies"])
    unique_titles = safe_int(overview_row["unique_titles"])
    avg_duration_months = round(safe_float(overview_row["avg_duration_months"]), 2)
else:
    total_positions = 0
    current_positions = 0
    unique_companies = 0
    unique_titles = 0
    avg_duration_months = 0

render_metric_row(
    [
        ("Total de posições", total_positions),
        ("Posições atuais", current_positions),
        ("Empresas únicas", unique_companies),
        ("Cargos únicos", unique_titles),
        ("Duração média (meses)", avg_duration_months),
    ]
)

st.divider()

st.markdown("## Evolução das posições iniciadas")

if not df_progression.empty:
    render_time_series_chart(
        df_progression,
        x_labels=df_progression["start_year_month"].astype(str).tolist(),
        y_column="total_positions_started",
        title="Posições iniciadas ao longo do tempo",
        x_label="Ano-Mês",
        y_label="Total de posições",
        color="#4EA1FF",
    )

st.markdown("## Duração média das posições")

if not df_progression.empty:
    render_time_series_chart(
        df_progression,
        x_labels=df_progression["start_year_month"].astype(str).tolist(),
        y_column="avg_duration_months",
        title="Duração média das posições iniciadas",
        x_label="Ano-Mês",
        y_label="Meses",
        color="#26C6DA",
        kind="bar",
    )

st.markdown("## Distribuição da carreira")

left, right = st.columns(2)

with left:
    st.markdown("### Top empresas")
    if not df_top_companies.empty:
        render_horizontal_bar_chart(
            df_top_companies,
            label_column="company_name_clean",
            value_column="total_positions",
            title="Empresas mais recorrentes",
            x_label="Quantidade de posições",
            y_label="Empresa",
            color="#7C9CF5",
        )

with right:
    st.markdown("### Top cargos")
    if not df_top_titles.empty:
        render_horizontal_bar_chart(
            df_top_titles,
            label_column="title_clean",
            value_column="total_positions",
            title="Cargos mais recorrentes",
            x_label="Quantidade de posições",
            y_label="Cargo",
            color="#F2C14E",
        )

st.divider()

st.markdown("## Histórico profissional")
render_dataframe(df_detail)

st.markdown("## Leitura inicial dos dados")

if not df_progression.empty:
    progression_for_insights = df_progression.dropna(subset=["total_positions_started"]).copy()
    if progression_for_insights.empty:
        st.info("Ainda não há dados suficientes para gerar insights automáticos.")
    else:
        best_idx = int(progression_for_insights["total_positions_started"].idxmax())
        best_row = progression_for_insights.loc[best_idx]

        best_period = str(best_row["start_year_month"])
        best_total = safe_int(best_row["total_positions_started"])

        st.info(
            f"O período com maior volume de posições iniciadas foi **{best_period}**, com **{best_total}** posição(ões)."
        )

        st.info(
            f"A duração média das posições no histórico analisado é de **{avg_duration_months} meses**."
        )
else:
    st.info("Ainda não há dados suficientes para gerar insights automáticos.")
