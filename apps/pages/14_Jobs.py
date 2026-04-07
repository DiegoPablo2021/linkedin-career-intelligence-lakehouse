import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    run_query,
    style_matplotlib,
)


st.set_page_config(page_title="Jobs", layout="wide")
apply_app_theme()


@st.cache_data
def load_applications_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_job_applications_summary order by application_year, application_month, job_family")


@st.cache_data
def load_alerts_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_saved_job_alerts_summary order by total_alerts desc")


@st.cache_data
def load_applications_detail() -> pd.DataFrame:
    return run_query("select * from main.int_job_applications_enriched order by application_date desc")


@st.cache_data
def load_alerts_detail() -> pd.DataFrame:
    return run_query("select * from main.int_saved_job_alerts_enriched order by saved_search_id")


st.title("Jobs")
st.subheader("Candidaturas e monitoramento de vagas")

df_apps = load_applications_summary()
df_alerts = load_alerts_summary()
df_apps_detail = load_applications_detail()
df_alerts_detail = load_alerts_detail()

c1, c2, c3 = st.columns(3)
c1.metric("Candidaturas", int(df_apps_detail.shape[0]) if not df_apps_detail.empty else 0)
c2.metric("Empresas candidatas", int(df_apps_detail["company_name_clean"].nunique()) if not df_apps_detail.empty else 0)
c3.metric("Alertas salvos", int(df_alerts_detail.shape[0]) if not df_alerts_detail.empty else 0)

st.divider()

left, right = st.columns(2)
with left:
    st.markdown("## Candidaturas por família de vaga")
    if not df_apps.empty:
        chart_df = df_apps.groupby("job_family", as_index=False)["total_applications"].sum()
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(chart_df["job_family"], chart_df["total_applications"], color="#4EA1FF")
        ax.set_title("Famílias de vaga")
        ax.set_xlabel("Família")
        ax.set_ylabel("Total")
        apply_straight_xticks(ax, chart_df["job_family"].astype(str).tolist(), max_ticks=6)
        style_matplotlib(fig, ax)
        plt.tight_layout()
        st.pyplot(fig)
        st.dataframe(df_apps, width="stretch")

with right:
    st.markdown("## Alertas de vagas")
    if not df_alerts.empty:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(df_alerts["alert_frequency"], df_alerts["total_alerts"], color="#26C6DA")
        ax.set_title("Alertas por frequência")
        ax.set_xlabel("Frequência")
        ax.set_ylabel("Total")
        apply_straight_xticks(ax, df_alerts["alert_frequency"].astype(str).tolist(), max_ticks=6)
        style_matplotlib(fig, ax)
        plt.tight_layout()
        st.pyplot(fig)
        st.dataframe(df_alerts, width="stretch")

st.divider()
st.markdown("## Candidaturas detalhadas")
st.dataframe(df_apps_detail, width="stretch")

st.markdown("## Alertas detalhados")
st.dataframe(df_alerts_detail, width="stretch")
