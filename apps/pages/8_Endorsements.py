import streamlit as st
import pandas as pd

from linkedin_career_intelligence.streamlit_utils import run_query

st.set_page_config(page_title="Endorsements", page_icon="🏅", layout="wide")

st.title("Endorsements")
st.subheader("Análise dos endossos recebidos no LinkedIn")

@st.cache_data
def load_endorsements_summary():
    query = """
        select
            skill_name_clean,
            endorsement_count,
            unique_endorsers,
            first_endorsement_date,
            last_endorsement_date
        from main.mart_endorsement_received_info_summary
        order by endorsement_count desc, skill_name_clean
    """
    return run_query(query)


@st.cache_data
def load_endorsements_detail():
    query = """
        select
            endorsement_date,
            skill_name,
            skill_name_clean,
            endorser_first_name,
            endorser_last_name,
            endorser_full_name,
            endorser_public_url,
            endorsement_status,
            endorsement_status_group
        from main.int_endorsement_received_info_enriched
        order by endorsement_date desc nulls last
    """
    return run_query(query)


df_summary = load_endorsements_summary()
df_detail = load_endorsements_detail()

if df_summary.empty:
    st.warning("Nenhum dado de endorsements encontrado.")
    st.stop()

# =========================
# KPIs
# =========================
total_skills = df_summary["skill_name_clean"].nunique()
total_endorsements = int(df_summary["endorsement_count"].sum())
total_unique_endorsers = int(df_summary["unique_endorsers"].sum())

first_endorsement = pd.to_datetime(df_summary["first_endorsement_date"], errors="coerce").min()
last_endorsement = pd.to_datetime(df_summary["last_endorsement_date"], errors="coerce").max()

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Skills endossadas", f"{total_skills}")
col2.metric("Total de endorsements", f"{total_endorsements}")
col3.metric("Endorsers únicos (somados)", f"{total_unique_endorsers}")
col4.metric(
    "Primeiro endorsement",
    first_endorsement.strftime("%d/%m/%Y") if pd.notnull(first_endorsement) else "-"
)
col5.metric(
    "Último endorsement",
    last_endorsement.strftime("%d/%m/%Y") if pd.notnull(last_endorsement) else "-"
)

st.divider()

# =========================
# Top skills
# =========================
st.markdown("## Top skills mais endossadas")

top_n = st.slider("Quantidade de skills no ranking", min_value=5, max_value=30, value=10)

top_skills = df_summary.head(top_n).copy()

st.bar_chart(
    top_skills.set_index("skill_name_clean")["endorsement_count"]
)

st.dataframe(
    top_skills,
    use_container_width=True
)

st.divider()

# =========================
# Distribuição por status
# =========================
st.markdown("## Distribuição por status do endorsement")

status_df = (
    df_detail.groupby("endorsement_status_group", dropna=False)
    .size()
    .reset_index(name="total")
    .sort_values("total", ascending=False)
)

if not status_df.empty:
    st.bar_chart(status_df.set_index("endorsement_status_group")["total"])
    st.dataframe(status_df, use_container_width=True)
else:
    st.info("Não há dados de status para exibir.")

st.divider()

# =========================
# Evolução temporal
# =========================
st.markdown("## Evolução temporal dos endorsements")

df_timeline = df_detail.copy()
df_timeline["endorsement_date"] = pd.to_datetime(df_timeline["endorsement_date"], errors="coerce")
df_timeline = df_timeline[df_timeline["endorsement_date"].notna()].copy()

if not df_timeline.empty:
    df_timeline["year_month"] = df_timeline["endorsement_date"].dt.to_period("M").astype(str)

    timeline_grouped = (
        df_timeline.groupby("year_month")
        .size()
        .reset_index(name="total_endorsements")
        .sort_values("year_month")
    )

    st.line_chart(
        timeline_grouped.set_index("year_month")["total_endorsements"]
    )
    st.dataframe(timeline_grouped, use_container_width=True)
else:
    st.info("Não há datas válidas para montar a evolução temporal.")

st.divider()

# =========================
# Insights automáticos
# =========================
st.markdown("## Insights automáticos")

top_skill = df_summary.iloc[0]["skill_name_clean"] if not df_summary.empty else "-"
top_skill_count = int(df_summary.iloc[0]["endorsement_count"]) if not df_summary.empty else 0

status_top = status_df.iloc[0]["endorsement_status_group"] if not status_df.empty else "-"
status_top_count = int(status_df.iloc[0]["total"]) if not status_df.empty else 0

if not df_timeline.empty and "timeline_grouped" in locals() and not timeline_grouped.empty:
    best_month_row = timeline_grouped.sort_values("total_endorsements", ascending=False).iloc[0]
    best_month = best_month_row["year_month"]
    best_month_total = int(best_month_row["total_endorsements"])
else:
    best_month = "-"
    best_month_total = 0

st.success(
    f"A habilidade mais validada pela sua rede é **{top_skill}**, com **{top_skill_count} endorsements**."
)

st.info(
    f"O status mais recorrente é **{status_top}**, com **{status_top_count} registros**."
)

st.warning(
    f"O período com maior concentração de endorsements foi **{best_month}**, com **{best_month_total} endorsements**."
)

st.divider()

# =========================
# Detalhamento
# =========================
st.markdown("## Detalhamento dos endorsements")

st.dataframe(
    df_detail,
    use_container_width=True
)
