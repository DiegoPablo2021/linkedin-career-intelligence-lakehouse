import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_label_mapping,
    apply_title_case,
    configure_page,
    load_query,
    render_card,
    render_dataframe,
    render_horizontal_bar_chart,
    render_metric_row,
    render_question,
    render_time_series_chart,
)


configure_page("Languages", "Análise dos idiomas e níveis de proficiência")


def load_languages_summary() -> pd.DataFrame:
    return load_query(
        """
        select
            proficiency_track,
            total_languages,
            unique_languages
        from main.mart_languages_summary
        order by total_languages desc, proficiency_track asc
        """
    )


def load_languages_overview() -> pd.DataFrame:
    return load_query(
        """
        select
            count(*) as total_language_records,
            count(distinct language_name_clean) as total_unique_languages,
            count(distinct proficiency_clean) as total_unique_proficiencies
        from main.int_languages_enriched
        """
    )


def load_languages_detail() -> pd.DataFrame:
    return load_query(
        """
        select
            name,
            proficiency,
            language_name_clean,
            proficiency_clean,
            proficiency_track
        from main.int_languages_enriched
        order by language_name_clean asc
        """
    )


def load_language_distribution() -> pd.DataFrame:
    return load_query(
        """
        select
            language_name_clean,
            count(*) as total_records
        from main.int_languages_enriched
        group by language_name_clean
        order by total_records desc, language_name_clean asc
        """
    )


PROFICIENCY_TRACK_LABELS = {
    "nativo": "Nativo",
    "fluente": "Fluente",
    "profissional": "Profissional",
    "intermediario_profissional": "Intermediário Profissional",
    "basico": "Básico",
    "outros": "Outros",
}

df_summary = load_languages_summary()
df_overview = load_languages_overview()
df_detail = load_languages_detail()
df_distribution = load_language_distribution()

if not df_summary.empty:
    df_summary = apply_label_mapping(df_summary, "proficiency_track", PROFICIENCY_TRACK_LABELS)

if not df_detail.empty:
    df_detail = apply_title_case(df_detail, ["name", "language_name_clean"])
    df_detail = apply_label_mapping(df_detail, "proficiency_track", PROFICIENCY_TRACK_LABELS)

if not df_distribution.empty:
    df_distribution = apply_title_case(df_distribution, ["language_name_clean"])

if not df_overview.empty:
    overview_row = df_overview.iloc[0]
    total_language_records = int(overview_row["total_language_records"])
    total_unique_languages = int(overview_row["total_unique_languages"])
    total_unique_proficiencies = int(overview_row["total_unique_proficiencies"])
else:
    total_language_records = 0
    total_unique_languages = 0
    total_unique_proficiencies = 0

render_metric_row(
    [
        ("Total de registros", total_language_records),
        ("Idiomas únicos", total_unique_languages),
        ("Proficiências únicas", total_unique_proficiencies),
    ]
)

st.divider()

st.markdown("## Distribuição por trilha de proficiência")

if not df_summary.empty and len(df_summary) > 1:
    render_time_series_chart(
        df_summary,
        x_labels=df_summary["proficiency_track"].astype(str).tolist(),
        y_column="total_languages",
        title="Quantidade de registros por trilha de proficiência",
        x_label="Trilha",
        y_label="Quantidade",
        color="#4EA1FF",
        kind="bar",
        max_ticks=6,
        figsize=(10, 5),
    )
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
    render_horizontal_bar_chart(
        df_distribution,
        label_column="language_name_clean",
        value_column="total_records",
        title="Idiomas registrados",
        x_label="Quantidade",
        y_label="Idioma",
        color="#26C6DA",
        figsize=(10, 5),
    )
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
    render_dataframe(df_summary)

with tab2:
    render_dataframe(df_detail)

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
