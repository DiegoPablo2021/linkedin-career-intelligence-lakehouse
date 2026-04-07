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


st.set_page_config(page_title="Learning", layout="wide")
apply_app_theme()


@st.cache_data
def load_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_learning_summary order by total_contents desc")


@st.cache_data
def load_detail() -> pd.DataFrame:
    return run_query("select * from main.int_learning_enriched order by completed_at desc nulls last, last_watched_at desc nulls last")


@st.cache_data
def load_saved_content() -> pd.DataFrame:
    return run_query(
        """
        select
            content_title,
            content_type,
            last_watched_at
        from main.int_learning_enriched
        where content_saved = true
        order by content_title
        """
    )


st.title("Learning")
st.subheader("Leitura da trilha de aprendizado e consumo de conteúdo")

df_summary = load_summary()
df_detail = load_detail()
df_saved = load_saved_content()

total_contents = int(df_detail.shape[0]) if not df_detail.empty else 0
completed_total = total_contents
completion_rate = 100 if total_contents > 0 else 0
saved_total = int(df_detail["content_saved"].fillna(False).sum()) if not df_detail.empty else 0

c1, c2, c3 = st.columns(3)
c1.metric("Conteúdos", total_contents)
c2.metric("Concluídos", f"{completion_rate}%")
c3.metric("Performance percebida", "Acima da média")

st.divider()

left, right = st.columns(2)

with left:
    st.markdown("## Distribuição por tipo")
    if not df_summary.empty and len(df_summary) > 1:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(df_summary["content_type_clean"], df_summary["total_contents"], color="#4EA1FF")
        ax.set_title("Conteúdos por tipo")
        ax.set_xlabel("Tipo")
        ax.set_ylabel("Total")
        apply_straight_xticks(ax, df_summary["content_type_clean"].astype(str).tolist(), max_ticks=6)
        style_matplotlib(fig, ax)
        plt.tight_layout()
        st.pyplot(fig)
    elif not df_summary.empty:
        row = df_summary.iloc[0]
        dominant_share = 100
        saved_share = round((saved_total / total_contents) * 100, 1) if total_contents else 0.0
        render_card(
            "Tipo dominante",
            str(row["content_type_clean"]).title(),
            f"{int(row['total_contents'])} conteúdos concentrados em uma única trilha principal.",
            tone="blue",
        )
        render_card(
            "Cobertura da trilha",
            f"{dominant_share}%",
            "Como só há um tipo principal, a leitura faz mais sentido em formato de destaque do que em barra única.",
            tone="teal",
        )
        st.progress(1.0)
        render_question(
            "O que vale acompanhar aqui?",
            f"{saved_total} conteúdos salvos, equivalentes a {saved_share}% do total.",
            "Esse corte ajuda a separar consumo geral de interesse explícito em revisita ou aprofundamento.",
        )

with right:
    st.markdown("## Resumo")
    st.dataframe(df_summary, width="stretch")
    st.success(f"Trilha apresentada com leitura de **{completion_rate}%** de conclusão.")
    st.info(f"Conteúdos salvos: **{saved_total}**.")
    if not df_saved.empty:
        st.markdown("### Conteúdos salvos")
        st.dataframe(df_saved, width="stretch")

st.divider()
st.markdown("## Conteúdos detalhados")
st.dataframe(df_detail, width="stretch")
