import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_datetime_conversion,
    apply_label_mapping,
    apply_title_case,
    configure_page,
    load_query,
    render_dataframe,
    render_horizontal_bar_chart,
    render_metric_row,
    render_time_series_chart,
)


configure_page("Education", "Análise da trajetória educacional")


def load_education_summary() -> pd.DataFrame:
    return load_query(
        """
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
    )


def load_education_overview() -> pd.DataFrame:
    return load_query(
        """
        select
            count(*) as total_education_records,
            sum(case when is_current_education then 1 else 0 end) as current_education_records,
            count(distinct school_name_clean) as total_unique_schools,
            count(distinct degree_name_clean) as total_unique_degrees,
            avg(education_duration_months) as avg_education_duration_months
        from main.int_education_enriched
        """
    )


def load_top_schools(limit: int = 10) -> pd.DataFrame:
    return load_query(
        f"""
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
    )


def load_education_track_distribution() -> pd.DataFrame:
    return load_query(
        """
        select
            education_track,
            count(*) as total_records
        from main.int_education_enriched
        group by education_track
        order by total_records desc, education_track asc
        """
    )


def load_education_detail() -> pd.DataFrame:
    return load_query(
        """
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
    )


EDUCATION_TRACK_LABELS = {
    "tecnologo": "Tecnólogo",
    "bacharelado": "Bacharelado",
    "licenciatura": "Licenciatura",
    "pos_graduacao": "Pós-graduação",
    "mba": "MBA",
    "mestrado": "Mestrado",
    "doutorado": "Doutorado",
    "outros": "Outros",
}

df_summary = load_education_summary()
df_overview = load_education_overview()
df_schools = load_top_schools()
df_tracks = load_education_track_distribution()
df_detail = load_education_detail()

if not df_schools.empty:
    df_schools = apply_title_case(df_schools, ["school_name_clean"])

if not df_tracks.empty:
    df_tracks = apply_label_mapping(df_tracks, "education_track", EDUCATION_TRACK_LABELS)

if not df_detail.empty:
    df_detail = apply_title_case(df_detail, ["school_name", "degree_name"])
    df_detail = apply_label_mapping(df_detail, "education_track", EDUCATION_TRACK_LABELS)
    df_detail = apply_datetime_conversion(df_detail, ["started_on", "finished_on"])

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

render_metric_row(
    [
        ("Total de formações", total_education_records),
        ("Formações em andamento", current_education_records),
        ("Instituições únicas", total_unique_schools),
        ("Formações únicas", total_unique_degrees),
        ("Duração média (meses)", avg_education_duration_months),
    ]
)

st.divider()

st.markdown("## Evolução das formações iniciadas")

if not df_summary.empty:
    render_time_series_chart(
        df_summary,
        x_labels=df_summary["start_year_month"].astype(str).tolist(),
        y_column="total_education_started",
        title="Formações iniciadas ao longo do tempo",
        x_label="Ano-Mês",
        y_label="Total de formações",
        color="#4EA1FF",
    )
else:
    st.warning("Não há dados disponíveis para o gráfico de evolução educacional.")

st.markdown("## Duração média das formações")

if not df_summary.empty:
    render_time_series_chart(
        df_summary,
        x_labels=df_summary["start_year_month"].astype(str).tolist(),
        y_column="avg_education_duration_months",
        title="Duração média das formações iniciadas",
        x_label="Ano-Mês",
        y_label="Meses",
        color="#26C6DA",
        kind="bar",
    )
else:
    st.warning("Não há dados disponíveis para o gráfico de duração média.")

st.markdown("## Distribuição educacional")

left, right = st.columns(2)

with left:
    st.markdown("### Top instituições")
    if not df_schools.empty:
        render_horizontal_bar_chart(
            df_schools,
            label_column="school_name_clean",
            value_column="total_records",
            title="Top instituições",
            x_label="Quantidade de registros",
            y_label="Instituição",
            color="#7C9CF5",
        )
    else:
        st.warning("Não há dados suficientes para top instituições.")

with right:
    st.markdown("### Trilhas educacionais")
    if not df_tracks.empty:
        render_horizontal_bar_chart(
            df_tracks,
            label_column="education_track",
            value_column="total_records",
            title="Distribuição por trilha educacional",
            x_label="Quantidade de registros",
            y_label="Trilha",
            color="#F2C14E",
        )
    else:
        st.warning("Não há dados suficientes para distribuição por trilha.")

st.divider()

st.markdown("## Tabelas de apoio")

tab1, tab2, tab3 = st.tabs(
    ["Resumo mensal", "Top instituições", "Detalhamento educacional"]
)

with tab1:
    render_dataframe(df_summary)

with tab2:
    render_dataframe(df_schools)

with tab3:
    render_dataframe(df_detail)

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
