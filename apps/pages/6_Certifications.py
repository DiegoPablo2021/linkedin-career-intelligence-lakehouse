import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    run_query,
    style_matplotlib,
)


st.set_page_config(page_title="Certifications", layout="wide")
apply_app_theme()


@st.cache_data
def load_certifications_summary() -> pd.DataFrame:
    query = """
        select
            start_year,
            start_month,
            start_year_month,
            total_certifications,
            unique_certifications,
            unique_authorities,
            avg_duration_months
        from main.mart_certifications_summary
        order by start_year, start_month
    """
    return run_query(query)


@st.cache_data
def load_certifications_overview() -> pd.DataFrame:
    query = """
        select
            count(*) as total_certification_records,
            count(distinct name_clean) as total_unique_certifications,
            count(distinct authority_clean) as total_unique_authorities,
            avg(certification_duration_months) as avg_certification_duration_months
        from main.int_certifications_enriched
    """
    return run_query(query)


@st.cache_data
def load_top_authorities(limit: int = 10) -> pd.DataFrame:
    query = f"""
        select
            authority_clean,
            count(*) as total_records
        from main.int_certifications_enriched
        where authority_clean is not null
          and trim(authority_clean) <> ''
        group by authority_clean
        order by total_records desc, authority_clean asc
        limit {limit}
    """
    return run_query(query)


@st.cache_data
def load_certification_track_distribution() -> pd.DataFrame:
    query = """
        select
            certification_track,
            count(*) as total_records
        from main.int_certifications_enriched
        group by certification_track
        order by total_records desc, certification_track asc
    """
    return run_query(query)


@st.cache_data
def load_certifications_detail() -> pd.DataFrame:
    query = """
        select
            name,
            authority,
            url,
            started_on,
            finished_on,
            license_number,
            certification_duration_months,
            certification_track
        from main.int_certifications_enriched
        order by started_on desc
    """
    return run_query(query)


def format_title_case(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().title()


def format_track_label(value: str) -> str:
    mapping = {
        "microsoft": "Microsoft",
        "aws": "AWS",
        "google": "Google",
        "oracle": "Oracle",
        "outros": "Outros",
    }
    return mapping.get(str(value), str(value).title())


st.title("Certifications")
st.subheader("Análise da trajetória de certificações")

df_summary = load_certifications_summary()
df_overview = load_certifications_overview()
df_authorities = load_top_authorities()
df_tracks = load_certification_track_distribution()
df_detail = load_certifications_detail()

if not df_authorities.empty:
    df_authorities["authority_clean"] = df_authorities["authority_clean"].apply(format_title_case)

if not df_tracks.empty:
    df_tracks["certification_track"] = df_tracks["certification_track"].apply(format_track_label)

if not df_detail.empty:
    df_detail["name"] = df_detail["name"].fillna("").apply(format_title_case)
    df_detail["authority"] = df_detail["authority"].fillna("").apply(format_title_case)
    df_detail["certification_track"] = df_detail["certification_track"].apply(format_track_label)
    df_detail["started_on"] = pd.to_datetime(df_detail["started_on"], errors="coerce")
    df_detail["finished_on"] = pd.to_datetime(df_detail["finished_on"], errors="coerce")

if not df_overview.empty:
    overview_row = df_overview.iloc[0]

    total_certification_records = int(overview_row["total_certification_records"])
    total_unique_certifications = int(overview_row["total_unique_certifications"])
    total_unique_authorities = int(overview_row["total_unique_authorities"])
    avg_certification_duration_months = round(float(overview_row["avg_certification_duration_months"]), 2)
else:
    total_certification_records = 0
    total_unique_certifications = 0
    total_unique_authorities = 0
    avg_certification_duration_months = 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total de certificações", f"{total_certification_records}")
c2.metric("Certificações únicas", f"{total_unique_certifications}")
c3.metric("Authorities únicas", f"{total_unique_authorities}")
c4.metric("Duração média (meses)", f"{avg_certification_duration_months}")

st.divider()

st.markdown("## Evolução das certificações iniciadas")

if not df_summary.empty:
    x_positions = list(range(len(df_summary)))
    x_labels = df_summary["start_year_month"].astype(str).tolist()
    fig1, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(x_positions, df_summary["total_certifications"], marker="o", color="#4EA1FF", linewidth=2.4)
    ax1.set_title("Certificações ao longo do tempo")
    ax1.set_xlabel("Ano-Mês")
    ax1.set_ylabel("Total de certificações")
    apply_straight_xticks(ax1, x_labels, max_ticks=10)
    style_matplotlib(fig1, ax1)
    plt.tight_layout()
    st.pyplot(fig1)
else:
    st.warning("Não há dados disponíveis para o gráfico de evolução das certificações.")

st.markdown("## Duração média das certificações")

if not df_summary.empty:
    x_positions = list(range(len(df_summary)))
    x_labels = df_summary["start_year_month"].astype(str).tolist()
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    ax2.bar(x_positions, df_summary["avg_duration_months"], color="#26C6DA")
    ax2.set_title("Duração média das certificações")
    ax2.set_xlabel("Ano-Mês")
    ax2.set_ylabel("Meses")
    apply_straight_xticks(ax2, x_labels, max_ticks=10)
    style_matplotlib(fig2, ax2)
    plt.tight_layout()
    st.pyplot(fig2)
else:
    st.warning("Não há dados disponíveis para o gráfico de duração média.")

st.markdown("## Distribuição das certificações")

left, right = st.columns(2)

with left:
    st.markdown("### Top authorities")
    if not df_authorities.empty:
        fig3, ax3 = plt.subplots(figsize=(10, 6))
        ax3.barh(df_authorities["authority_clean"], df_authorities["total_records"], color="#7C9CF5")
        ax3.set_title("Top authorities")
        ax3.set_xlabel("Quantidade de registros")
        ax3.set_ylabel("Authority")
        ax3.invert_yaxis()
        style_matplotlib(fig3, ax3)
        plt.tight_layout()
        st.pyplot(fig3)
    else:
        st.warning("Não há dados suficientes para top authorities.")

with right:
    st.markdown("### Trilhas de certificação")
    if not df_tracks.empty:
        fig4, ax4 = plt.subplots(figsize=(10, 6))
        ax4.barh(df_tracks["certification_track"], df_tracks["total_records"], color="#F2C14E")
        ax4.set_title("Distribuição por trilha de certificação")
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
    ["Resumo mensal", "Top authorities", "Detalhamento de certificações"]
)

with tab1:
    st.dataframe(df_summary, width="stretch")

with tab2:
    st.dataframe(df_authorities, width="stretch")

with tab3:
    st.dataframe(df_detail, width="stretch")

st.markdown("## Leitura inicial dos dados")

if not df_summary.empty:
    best_idx = int(df_summary["total_certifications"].idxmax())
    best_row = df_summary.iloc[best_idx]

    best_period = str(best_row["start_year_month"])
    best_total = int(best_row["total_certifications"])

    st.info(
        f"O período com maior volume de certificações iniciadas foi **{best_period}**, com **{best_total}** registro(s)."
    )

    st.info(
        f"A duração média das certificações no histórico analisado é de **{avg_certification_duration_months} meses**."
    )

    if not df_tracks.empty:
        top_track_idx = int(df_tracks["total_records"].idxmax())
        top_track_row = df_tracks.iloc[top_track_idx]
        top_track = str(top_track_row["certification_track"])
        top_track_total = int(top_track_row["total_records"])

        st.info(
            f"A trilha de certificação mais frequente no histórico é **{top_track}**, com **{top_track_total}** registro(s)."
        )
else:
    st.info("Ainda não há dados suficientes para gerar insights automáticos.")
