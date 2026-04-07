import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    run_query,
    safe_float,
    safe_int,
    style_matplotlib,
)


st.set_page_config(page_title="Career", layout="wide")
apply_app_theme()


@st.cache_data
def load_career_progression() -> pd.DataFrame:
    query = """
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
    return run_query(query)


@st.cache_data
def load_positions_detail() -> pd.DataFrame:
    query = """
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
    return run_query(query)


@st.cache_data
def load_career_overview() -> pd.DataFrame:
    query = """
        select
            count(*) as total_positions,
            sum(case when is_current then 1 else 0 end) as current_positions,
            count(distinct company_name_clean) as unique_companies,
            count(distinct title_clean) as unique_titles,
            avg(duration_months) as avg_duration_months
        from main.int_positions_enriched
    """
    return run_query(query)


@st.cache_data
def load_top_companies(limit: int = 10) -> pd.DataFrame:
    query = f"""
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
    return run_query(query)


@st.cache_data
def load_top_titles(limit: int = 10) -> pd.DataFrame:
    query = f"""
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
    return run_query(query)


def format_title_case(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().title()


st.title("Career")
st.subheader("Análise da trajetória profissional")

df_progression = load_career_progression()
df_detail = load_positions_detail()
df_overview = load_career_overview()
df_top_companies = load_top_companies()
df_top_titles = load_top_titles()

if not df_top_companies.empty:
    df_top_companies["company_name_clean"] = df_top_companies["company_name_clean"].apply(format_title_case)

if not df_top_titles.empty:
    df_top_titles["title_clean"] = df_top_titles["title_clean"].apply(format_title_case)

if not df_detail.empty:
    df_detail["company_name"] = df_detail["company_name"].fillna("").apply(format_title_case)
    df_detail["title"] = df_detail["title"].fillna("").apply(format_title_case)
    df_detail["location"] = df_detail["location"].fillna("").apply(format_title_case)

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

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total de posições", f"{total_positions}")
c2.metric("Posições atuais", f"{current_positions}")
c3.metric("Empresas únicas", f"{unique_companies}")
c4.metric("Cargos únicos", f"{unique_titles}")
c5.metric("Duração média (meses)", f"{avg_duration_months}")

st.divider()

st.markdown("## Evolução das posições iniciadas")

if not df_progression.empty:
    x_positions = list(range(len(df_progression)))
    x_labels = df_progression["start_year_month"].astype(str).tolist()
    fig1, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(x_positions, df_progression["total_positions_started"], marker="o", color="#4EA1FF", linewidth=2.4)
    ax1.set_title("Posições iniciadas ao longo do tempo")
    ax1.set_xlabel("Ano-Mês")
    ax1.set_ylabel("Total de posições")
    apply_straight_xticks(ax1, x_labels, max_ticks=10)
    style_matplotlib(fig1, ax1)
    plt.tight_layout()
    st.pyplot(fig1)

st.markdown("## Duração média das posições")

if not df_progression.empty:
    x_positions = list(range(len(df_progression)))
    x_labels = df_progression["start_year_month"].astype(str).tolist()
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    ax2.bar(x_positions, df_progression["avg_duration_months"], color="#26C6DA")
    ax2.set_title("Duração média das posições iniciadas")
    ax2.set_xlabel("Ano-Mês")
    ax2.set_ylabel("Meses")
    apply_straight_xticks(ax2, x_labels, max_ticks=10)
    style_matplotlib(fig2, ax2)
    plt.tight_layout()
    st.pyplot(fig2)

st.markdown("## Distribuição da carreira")

left, right = st.columns(2)

with left:
    st.markdown("### Top empresas")
    if not df_top_companies.empty:
        fig3, ax3 = plt.subplots(figsize=(10, 6))
        ax3.barh(df_top_companies["company_name_clean"], df_top_companies["total_positions"], color="#7C9CF5")
        ax3.set_title("Empresas mais recorrentes")
        ax3.set_xlabel("Quantidade de posições")
        ax3.set_ylabel("Empresa")
        ax3.invert_yaxis()
        style_matplotlib(fig3, ax3)
        plt.tight_layout()
        st.pyplot(fig3)

with right:
    st.markdown("### Top cargos")
    if not df_top_titles.empty:
        fig4, ax4 = plt.subplots(figsize=(10, 6))
        ax4.barh(df_top_titles["title_clean"], df_top_titles["total_positions"], color="#F2C14E")
        ax4.set_title("Cargos mais recorrentes")
        ax4.set_xlabel("Quantidade de posições")
        ax4.set_ylabel("Cargo")
        ax4.invert_yaxis()
        style_matplotlib(fig4, ax4)
        plt.tight_layout()
        st.pyplot(fig4)

st.divider()

st.markdown("## Histórico profissional")
st.dataframe(df_detail, width="stretch")

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
