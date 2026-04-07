import streamlit as st
import pandas as pd

from linkedin_career_intelligence.streamlit_utils import run_query

st.set_page_config(page_title="Company Follows", page_icon="🏢", layout="wide")

st.title("Company Follows")
st.subheader("Empresas que você segue no LinkedIn")

@st.cache_data
def load_summary():
    query = """
        select
            organization_clean,
            follow_count,
            first_follow_date,
            last_follow_date
        from main.mart_company_follows_summary
        order by follow_count desc, organization_clean
    """
    return run_query(query)


@st.cache_data
def load_detail():
    query = """
        select
            organization,
            organization_clean,
            followed_on,
            year,
            month,
            year_month
        from main.int_company_follows_enriched
        order by followed_on desc nulls last
    """
    return run_query(query)


df_summary = load_summary()
df_detail = load_detail()

if df_summary.empty:
    st.warning("Nenhum dado de empresas seguidas encontrado.")
    st.stop()

# =========================
# KPIs
# =========================
total_companies = df_summary["organization_clean"].nunique()
total_follows = int(df_summary["follow_count"].sum())

first_follow = pd.to_datetime(df_summary["first_follow_date"], errors="coerce").min()
last_follow = pd.to_datetime(df_summary["last_follow_date"], errors="coerce").max()

col1, col2, col3, col4 = st.columns(4)

col1.metric("Empresas seguidas", total_companies)
col2.metric("Total de follows", total_follows)
col3.metric(
    "Primeiro follow",
    first_follow.strftime("%d/%m/%Y") if pd.notnull(first_follow) else "-"
)
col4.metric(
    "Último follow",
    last_follow.strftime("%d/%m/%Y") if pd.notnull(last_follow) else "-"
)

st.divider()

# =========================
# Ranking
# =========================
st.markdown("## Ranking de empresas seguidas")

top_n = st.slider("Quantidade no ranking", 5, 30, 10)

top_df = df_summary.head(top_n)

st.bar_chart(
    top_df.set_index("organization_clean")["follow_count"]
)

st.dataframe(top_df, use_container_width=True)

st.divider()

# =========================
# Evolução temporal
# =========================
st.markdown("## Evolução temporal dos follows")

df_timeline = df_detail.copy()
df_timeline["followed_on"] = pd.to_datetime(df_timeline["followed_on"], errors="coerce")
df_timeline = df_timeline[df_timeline["followed_on"].notna()]

if not df_timeline.empty:
    timeline_grouped = (
        df_timeline.groupby("year_month")
        .size()
        .reset_index(name="total_follows")
        .sort_values("year_month")
    )

    st.line_chart(
        timeline_grouped.set_index("year_month")["total_follows"]
    )

    st.dataframe(timeline_grouped, use_container_width=True)
else:
    st.info("Sem dados suficientes para análise temporal.")

st.divider()

# =========================
# Insights automáticos
# =========================
st.markdown("## Insights automáticos")

top_company = df_summary.iloc[0]["organization_clean"]
top_count = int(df_summary.iloc[0]["follow_count"])

if not df_timeline.empty:
    best_month_row = timeline_grouped.sort_values("total_follows", ascending=False).iloc[0]
    best_month = best_month_row["year_month"]
    best_month_total = int(best_month_row["total_follows"])
else:
    best_month = "-"
    best_month_total = 0

st.success(
    f"A empresa mais relevante no seu radar é **{top_company}**, com **{top_count} registros**."
)

st.info(
    f"O período com maior atividade de follows foi **{best_month}**, com **{best_month_total} empresas seguidas**."
)

# Classificação simples de interesse
if total_companies > 50:
    level = "alto"
elif total_companies > 20:
    level = "moderado"
else:
    level = "focado"

st.warning(
    f"Seu nível de exploração de empresas é **{level}**, com **{total_companies} organizações monitoradas**."
)

st.divider()

# =========================
# Tabela detalhada
# =========================
st.markdown("## Detalhamento")

st.dataframe(df_detail, use_container_width=True)
