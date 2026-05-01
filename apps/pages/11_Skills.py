import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    render_card,
    render_question,
    run_query,
    style_matplotlib,
)


st.set_page_config(page_title="Skills", layout="wide")
apply_app_theme()


@st.cache_data
def load_summary() -> pd.DataFrame:
    return run_query(
        """
        select *
        from main.mart_skills_summary
        """
    )


@st.cache_data
def load_detail() -> pd.DataFrame:
    return run_query(
        """
        select *
        from main.int_skills_enriched
        order by skill_category, skill_name
        """
    )


st.title("Skills")
st.subheader("Análise das habilidades registradas no LinkedIn")

df_summary = load_summary()
df_detail = load_detail()

total_skills = int(df_detail.shape[0]) if not df_detail.empty else 0
unique_skills = int(df_detail["skill_name_clean"].nunique()) if not df_detail.empty else 0
top_category = df_summary.iloc[0]["skill_category"] if not df_summary.empty else "-"

c1, c2, c3 = st.columns(3)
with c1:
    render_card(
        "Skills totais",
        str(total_skills),
        "Quantidade de registros de skills tratada no pipeline analítico.",
        tone="blue",
    )
with c2:
    render_card(
        "Skills únicas",
        str(unique_skills),
        "Contagem distinta após limpeza, padronização e deduplicação.",
        tone="teal",
    )
with c3:
    render_card(
        "Categoria dominante",
        str(top_category).replace("_", " ").title(),
        "Categoria com maior presença relativa dentro do portfólio atual.",
        tone="gold",
    )

st.divider()

insight_left, insight_right = st.columns(2)
with insight_left:
    dominant_share = round((int(df_summary.iloc[0]["total_skills"]) / total_skills) * 100, 1) if not df_summary.empty and total_skills else 0.0
    render_card(
        "Concentração principal",
        f"{dominant_share}%",
        "Participação da categoria dominante dentro do total de skills carregadas.",
        tone="blue",
        min_height=160,
    )
with insight_right:
    render_question(
        "O portfólio de skills é concentrado ou distribuído?",
        "Predominantemente concentrado em uma categoria principal.",
        "Ainda assim, as categorias secundárias mostram amplitude técnica complementar.",
        min_height=160,
    )

st.markdown("## Distribuição por categoria")
if not df_summary.empty:
    chart_df = df_summary.copy()
    chart_df["skill_category"] = chart_df["skill_category"].astype(str).str.replace("_", " ").str.title()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(chart_df["skill_category"], chart_df["total_skills"], color="#4EA1FF")
    ax.set_title("Skills por categoria")
    ax.set_xlabel("Categoria")
    ax.set_ylabel("Total")
    apply_straight_xticks(ax, chart_df["skill_category"].astype(str).tolist(), max_ticks=6)
    style_matplotlib(fig, ax)
    plt.tight_layout()
    st.pyplot(fig)

tab1, tab2 = st.tabs(["Resumo por categoria", "Skills detalhadas"])
with tab1:
    st.dataframe(df_summary, width="stretch", hide_index=True)
with tab2:
    st.dataframe(df_detail, width="stretch", hide_index=True)
