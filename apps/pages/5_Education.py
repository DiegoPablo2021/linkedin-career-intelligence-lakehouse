import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    run_query,
    style_matplotlib,
)


st.set_page_config(page_title="Education", layout="wide")
apply_app_theme()


@st.cache_data
def load_education_summary() -> pd.DataFrame:
    query = """
        select
            start_year,
            start_month,
            start_year_month,
            total_education_started,
            current_education_started,
            unique_schools,
            unique_degrees,
            avg_education_duration_months
        from main.mart_education_summary
        order by start_year, start_month
    """
    return run_query(query)


@st.cache_data
def load_education_overview() -> pd.DataFrame:
    query = """
        select
            count(*) as total_education_records,
            sum(case when is_current_education then 1 else 0 end) as current_education_records,
            count(distinct school_name_clean) as total_unique_schools,
            count(distinct degree_name_clean) as total_unique_degrees,
            avg(education_duration_months) as avg_education_duration_months
        from main.int_education_enriched
    """
    return run_query(query)


@st.cache_data
def load_top_schools(limit: int = 10) -> pd.DataFrame:
    query = f"""
        select
            school_name_clean,
            count(*) as total_records
        from main.int_education_enriched
        where school_name_clean is not null
          and trim(school_name_clean) <> ''
        group by school_name_clean
        order by total_records desc, school_name_clean asc
        limit {limit}
    """
    return run_query(query)


@st.cache_data
def load_education_track_distribution() -> pd.DataFrame:
    query = """
        select
            education_track,
            count(*) as total_records
        from main.int_education_enriched
        group by education_track
        order by total_records desc, education_track asc
    """
    return run_query(query)


@st.cache_data
def load_education_detail() -> pd.DataFrame:
    query = """
        select
            school_name,
            degree_name,
            notes,
            started_on,
            finished_on,
            is_current_education,
            education_duration_months,
            education_track
        from main.int_education_enriched
        order by started_on desc
    """
    return run_query(query)


def format_title_case(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().title()


def format_track_label(value: str) -> str:
    mapping = {
        "tecnologo": "Tecnólogo",
        "bacharelado": "Bacharelado",
        "licenciatura": "Licenciatura",
        "pos_graduacao": "Pós-graduação",
        "mba": "MBA",
        "mestrado": "Mestrado",
        "doutorado": "Doutorado",
        "outros": "Outros",
    }
    return mapping.get(str(value), str(value).title())


st.title("Education")
st.subheader("Análise da trajetória educacional")

df_summary = load_education_summary()
df_overview = load_education_overview()
df_schools = load_top_schools()
df_tracks = load_education_track_distribution()
df_detail = load_education_detail()

if not df_schools.empty:
    df_schools["school_name_clean"] = df_schools["school_name_clean"].apply(format_title_case)

if not df_tracks.empty:
    df_tracks["education_track"] = df_tracks["education_track"].apply(format_track_label)

if not df_detail.empty:
    df_detail["school_name"] = df_detail["school_name"].fillna("").apply(format_title_case)
    df_detail["degree_name"] = df_detail["degree_name"].fillna("").apply(format_title_case)
    df_detail["education_track"] = df_detail["education_track"].apply(format_track_label)
    df_detail["started_on"] = pd.to_datetime(df_detail["started_on"], errors="coerce")
    df_detail["finished_on"] = pd.to_datetime(df_detail["finished_on"], errors="coerce")

if not df_overview.empty:
    overview_row = df_overview.iloc[0]

    total_education_records = int(overview_row["total_education_records"])
    current_education_records = int(overview_row["current_education_records"])
    total_unique_schools = int(overview_row["total_unique_schools"])
    total_unique_degrees = int(overview_row["total_unique_degrees"])
    avg_education_duration_months = round(float(overview_row["avg_education_duration_months"]), 2)
else:
    total_education_records = 0
    current_education_records = 0
    total_unique_schools = 0
    total_unique_degrees = 0
    avg_education_duration_months = 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total de formações", f"{total_education_records}")
c2.metric("Formações em andamento", f"{current_education_records}")
c3.metric("Instituições únicas", f"{total_unique_schools}")
c4.metric("Formações únicas", f"{total_unique_degrees}")
c5.metric("Duração média (meses)", f"{avg_education_duration_months}")

st.divider()

st.markdown("## Evolução das formações iniciadas")

if not df_summary.empty:
    x_positions = list(range(len(df_summary)))
    x_labels = df_summary["start_year_month"].astype(str).tolist()
    fig1, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(x_positions, df_summary["total_education_started"], marker="o", color="#4EA1FF", linewidth=2.4)
    ax1.set_title("Formações iniciadas ao longo do tempo")
    ax1.set_xlabel("Ano-Mês")
    ax1.set_ylabel("Total de formações")
    apply_straight_xticks(ax1, x_labels, max_ticks=10)
    style_matplotlib(fig1, ax1)
    plt.tight_layout()
    st.pyplot(fig1)
else:
    st.warning("Não há dados disponíveis para o gráfico de evolução educacional.")

st.markdown("## Duração média das formações")

if not df_summary.empty:
    x_positions = list(range(len(df_summary)))
    x_labels = df_summary["start_year_month"].astype(str).tolist()
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    ax2.bar(x_positions, df_summary["avg_education_duration_months"], color="#26C6DA")
    ax2.set_title("Duração média das formações iniciadas")
    ax2.set_xlabel("Ano-Mês")
    ax2.set_ylabel("Meses")
    apply_straight_xticks(ax2, x_labels, max_ticks=10)
    style_matplotlib(fig2, ax2)
    plt.tight_layout()
    st.pyplot(fig2)
else:
    st.warning("Não há dados disponíveis para o gráfico de duração média.")

st.markdown("## Distribuição educacional")

left, right = st.columns(2)

with left:
    st.markdown("### Top instituições")
    if not df_schools.empty:
        fig3, ax3 = plt.subplots(figsize=(10, 6))
        ax3.barh(df_schools["school_name_clean"], df_schools["total_records"], color="#7C9CF5")
        ax3.set_title("Top instituições")
        ax3.set_xlabel("Quantidade de registros")
        ax3.set_ylabel("Instituição")
        ax3.invert_yaxis()
        style_matplotlib(fig3, ax3)
        plt.tight_layout()
        st.pyplot(fig3)
    else:
        st.warning("Não há dados suficientes para top instituições.")

with right:
    st.markdown("### Trilhas educacionais")
    if not df_tracks.empty:
        fig4, ax4 = plt.subplots(figsize=(10, 6))
        ax4.barh(df_tracks["education_track"], df_tracks["total_records"], color="#F2C14E")
        ax4.set_title("Distribuição por trilha educacional")
        ax4.set_xlabel("Quantidade de registros")
        ax4.set_ylabel("Trilha")
        ax4.invert_yaxis()
        style_matplotlib(fig4, ax4)
        plt.tight_layout()
        st.pyplot(fig4)
    else:
        st.warning("Não há dados suficientes para distribuição por trilha.")

st.divider()

st.markdown("## Tabelas de apoio")

tab1, tab2, tab3 = st.tabs(
    ["Resumo mensal", "Top instituições", "Detalhamento educacional"]
)

with tab1:
    st.dataframe(df_summary, width="stretch")

with tab2:
    st.dataframe(df_schools, width="stretch")

with tab3:
    st.dataframe(df_detail, width="stretch")

st.markdown("## Leitura inicial dos dados")

if not df_summary.empty:
    best_idx = int(df_summary["total_education_started"].idxmax())
    best_row = df_summary.iloc[best_idx]

    best_period = str(best_row["start_year_month"])
    best_total = int(best_row["total_education_started"])

    st.info(
        f"O período com maior volume de formações iniciadas foi **{best_period}**, com **{best_total}** registro(s)."
    )

    st.info(
        f"A duração média das formações no histórico analisado é de **{avg_education_duration_months} meses**."
    )

    if not df_tracks.empty:
        top_track_idx = int(df_tracks["total_records"].idxmax())
        top_track_row = df_tracks.iloc[top_track_idx]
        top_track = str(top_track_row["education_track"])
        top_track_total = int(top_track_row["total_records"])

        st.info(
            f"A trilha educacional mais frequente no histórico é **{top_track}**, com **{top_track_total}** registro(s)."
        )
else:
    st.info("Ainda não há dados suficientes para gerar insights automáticos.")
