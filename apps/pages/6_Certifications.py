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


configure_page("Certifications", "Análise da trajetória de certificações")


def load_certifications_summary() -> pd.DataFrame:
    return load_query(
        """
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
    )


def load_certifications_overview() -> pd.DataFrame:
    return load_query(
        """
        select
            count(*) as total_certification_records,
            count(distinct name_clean) as total_unique_certifications,
            count(distinct authority_clean) as total_unique_authorities,
            avg(certification_duration_months) as avg_certification_duration_months
        from main.int_certifications_enriched
        """
    )


def load_top_authorities(limit: int = 10) -> pd.DataFrame:
    return load_query(
        f"""
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
    )


def load_certification_track_distribution() -> pd.DataFrame:
    return load_query(
        """
        select
            certification_track,
            count(*) as total_records
        from main.int_certifications_enriched
        group by certification_track
        order by total_records desc, certification_track asc
        """
    )


def load_certifications_detail() -> pd.DataFrame:
    return load_query(
        """
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
    )


CERTIFICATION_TRACK_LABELS = {
    "microsoft": "Microsoft",
    "aws": "AWS",
    "google": "Google",
    "oracle": "Oracle",
    "outros": "Outros",
}

df_summary = load_certifications_summary()
df_overview = load_certifications_overview()
df_authorities = load_top_authorities()
df_tracks = load_certification_track_distribution()
df_detail = load_certifications_detail()

if not df_authorities.empty:
    df_authorities = apply_title_case(df_authorities, ["authority_clean"])

if not df_tracks.empty:
    df_tracks = apply_label_mapping(df_tracks, "certification_track", CERTIFICATION_TRACK_LABELS)

if not df_detail.empty:
    df_detail = apply_title_case(df_detail, ["name", "authority"])
    df_detail = apply_label_mapping(df_detail, "certification_track", CERTIFICATION_TRACK_LABELS)
    df_detail = apply_datetime_conversion(df_detail, ["started_on", "finished_on"])

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

render_metric_row(
    [
        ("Total de certificações", total_certification_records),
        ("Certificações únicas", total_unique_certifications),
        ("Authorities únicas", total_unique_authorities),
        ("Duração média (meses)", avg_certification_duration_months),
    ]
)

st.divider()

st.markdown("## Evolução das certificações iniciadas")

if not df_summary.empty:
    render_time_series_chart(
        df_summary,
        x_labels=df_summary["start_year_month"].astype(str).tolist(),
        y_column="total_certifications",
        title="Certificações ao longo do tempo",
        x_label="Ano-Mês",
        y_label="Total de certificações",
        color="#4EA1FF",
    )
else:
    st.warning("Não há dados disponíveis para o gráfico de evolução das certificações.")

st.markdown("## Duração média das certificações")

if not df_summary.empty:
    render_time_series_chart(
        df_summary,
        x_labels=df_summary["start_year_month"].astype(str).tolist(),
        y_column="avg_duration_months",
        title="Duração média das certificações",
        x_label="Ano-Mês",
        y_label="Meses",
        color="#26C6DA",
        kind="bar",
    )
else:
    st.warning("Não há dados disponíveis para o gráfico de duração média.")

st.markdown("## Distribuição das certificações")

left, right = st.columns(2)

with left:
    st.markdown("### Top authorities")
    if not df_authorities.empty:
        render_horizontal_bar_chart(
            df_authorities,
            label_column="authority_clean",
            value_column="total_records",
            title="Top authorities",
            x_label="Quantidade de registros",
            y_label="Authority",
            color="#7C9CF5",
        )
    else:
        st.warning("Não há dados suficientes para top authorities.")

with right:
    st.markdown("### Trilhas de certificação")
    if not df_tracks.empty:
        render_horizontal_bar_chart(
            df_tracks,
            label_column="certification_track",
            value_column="total_records",
            title="Distribuição por trilha de certificação",
            x_label="Quantidade de registros",
            y_label="Trilha",
            color="#F2C14E",
        )
    else:
        st.warning("Não há dados suficientes para distribuição por trilha.")

st.divider()

st.markdown("## Tabelas de apoio")

tab1, tab2, tab3 = st.tabs(
    ["Resumo mensal", "Top authorities", "Detalhamento de certificações"]
)

with tab1:
    render_dataframe(df_summary)

with tab2:
    render_dataframe(df_authorities)

with tab3:
    render_dataframe(df_detail)

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
