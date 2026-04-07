import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    render_card,
    render_question,
    run_query,
    style_matplotlib,
)


st.set_page_config(page_title="Languages", layout="wide")
apply_app_theme()


@st.cache_data
def load_languages_summary() -> pd.DataFrame:
    query = """
        select
            proficiency_track,
            total_languages,
            unique_languages
        from main.mart_languages_summary
        order by total_languages desc, proficiency_track asc
    """
    return run_query(query)


@st.cache_data
def load_languages_overview() -> pd.DataFrame:
    query = """
        select
            count(*) as total_language_records,
            count(distinct language_name_clean) as total_unique_languages,
            count(distinct proficiency_clean) as total_unique_proficiencies
        from main.int_languages_enriched
    """
    return run_query(query)


@st.cache_data
def load_languages_detail() -> pd.DataFrame:
    query = """
        select
            name,
            proficiency,
            language_name_clean,
            proficiency_clean,
            proficiency_track
        from main.int_languages_enriched
        order by language_name_clean asc
    """
    return run_query(query)


@st.cache_data
def load_language_distribution() -> pd.DataFrame:
    query = """
        select
            language_name_clean,
            count(*) as total_records
        from main.int_languages_enriched
        group by language_name_clean
        order by total_records desc, language_name_clean asc
    """
    return run_query(query)


def format_title_case(value: str) -> str:
    if value is None:
        return ""
    return str(value).strip().title()


def format_track_label(value: str) -> str:
    mapping = {
        "nativo": "Nativo",
        "fluente": "Fluente",
        "profissional": "Profissional",
        "intermediario_profissional": "Intermediário Profissional",
        "basico": "Básico",
        "outros": "Outros",
    }
    return mapping.get(str(value), str(value).title())


st.title("Languages")
st.subheader("Análise dos idiomas e níveis de proficiência")

df_summary = load_languages_summary()
df_overview = load_languages_overview()
df_detail = load_languages_detail()
df_distribution = load_language_distribution()

if not df_summary.empty:
    df_summary["proficiency_track"] = df_summary["proficiency_track"].apply(format_track_label)

if not df_detail.empty:
    df_detail["name"] = df_detail["name"].fillna("").apply(format_title_case)
    df_detail["language_name_clean"] = df_detail["language_name_clean"].fillna("").apply(format_title_case)
    df_detail["proficiency_track"] = df_detail["proficiency_track"].apply(format_track_label)

if not df_distribution.empty:
    df_distribution["language_name_clean"] = df_distribution["language_name_clean"].apply(format_title_case)

if not df_overview.empty:
    overview_row = df_overview.iloc[0]
    total_language_records = int(overview_row["total_language_records"])
    total_unique_languages = int(overview_row["total_unique_languages"])
    total_unique_proficiencies = int(overview_row["total_unique_proficiencies"])
else:
    total_language_records = 0
    total_unique_languages = 0
    total_unique_proficiencies = 0

c1, c2, c3 = st.columns(3)
c1.metric("Total de registros", f"{total_language_records}")
c2.metric("Idiomas únicos", f"{total_unique_languages}")
c3.metric("Proficiências únicas", f"{total_unique_proficiencies}")

st.divider()

st.markdown("## Distribuição por trilha de proficiência")

if not df_summary.empty and len(df_summary) > 1:
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(df_summary["proficiency_track"], df_summary["total_languages"], color="#4EA1FF")
    ax1.set_title("Quantidade de registros por trilha de proficiência")
    ax1.set_xlabel("Trilha")
    ax1.set_ylabel("Quantidade")
    apply_straight_xticks(ax1, df_summary["proficiency_track"].astype(str).tolist(), max_ticks=6)
    style_matplotlib(fig1, ax1)
    plt.tight_layout()
    st.pyplot(fig1)
else:
    if not df_detail.empty:
        language_row = df_detail.iloc[0]
        proficiency_scores = {
            "nativo": 100,
            "fluente": 90,
            "profissional": 75,
            "intermediario_profissional": 65,
            "basico": 40,
            "outros": 50,
        }
        proficiency_score = proficiency_scores.get(str(language_row["proficiency_track"]).lower(), 50)

        c_left, c_right = st.columns(2)
        with c_left:
            render_card(
                "Idioma principal",
                str(language_row["language_name_clean"]).title(),
                f"Nível registrado: {str(language_row['proficiency_track']).title()}.",
                tone="blue",
            )
        with c_right:
            render_card(
                "Intensidade de proficiência",
                f"{proficiency_score}%",
                "Leitura qualitativa para representar o nível atual sem forçar um gráfico de barra única.",
                tone="teal",
            )

        st.progress(proficiency_score / 100)
        render_question(
            "Como ler esse dado de idioma?",
            "Hoje a base mostra um idioma principal com aplicação profissional.",
            "Como há apenas um registro, um resumo qualitativo comunica melhor do que um gráfico com uma única barra.",
        )
    else:
        st.warning("Não há dados disponíveis para distribuição por proficiência.")

st.markdown("## Idiomas registrados")

if not df_distribution.empty and len(df_distribution) > 1:
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.barh(df_distribution["language_name_clean"], df_distribution["total_records"], color="#26C6DA")
    ax2.set_title("Idiomas registrados")
    ax2.set_xlabel("Quantidade")
    ax2.set_ylabel("Idioma")
    ax2.invert_yaxis()
    style_matplotlib(fig2, ax2)
    plt.tight_layout()
    st.pyplot(fig2)
else:
    if not df_distribution.empty:
        st.info("A base atual registra apenas um idioma, então a visualização foi substituída por leitura executiva acima.")
    else:
        st.warning("Não há dados disponíveis para idiomas registrados.")

st.divider()

st.markdown("## Tabelas de apoio")

tab1, tab2 = st.tabs(
    ["Resumo por proficiência", "Detalhamento de idiomas"]
)

with tab1:
    st.dataframe(df_summary, width="stretch")

with tab2:
    st.dataframe(df_detail, width="stretch")

st.markdown("## Leitura inicial dos dados")

if not df_summary.empty:
    best_idx = int(df_summary["total_languages"].idxmax())
    best_row = df_summary.iloc[best_idx]

    top_track = str(best_row["proficiency_track"])
    top_total = int(best_row["total_languages"])

    st.info(
        f"A trilha de proficiência mais frequente é **{top_track}**, com **{top_total}** registro(s)."
    )

    st.info(
        f"O conjunto analisado possui **{total_unique_languages}** idioma(s) único(s) com **{total_unique_proficiencies}** nível(is) de proficiência distinto(s)."
    )
else:
    st.info("Ainda não há dados suficientes para gerar insights automáticos.")
