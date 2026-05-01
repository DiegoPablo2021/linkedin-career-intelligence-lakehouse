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


st.set_page_config(page_title="Recommendations", layout="wide")
apply_app_theme()


@st.cache_data
def load_summary() -> pd.DataFrame:
    return run_query(
        """
        select *
        from main.mart_recommendations_received_summary
        """
    )


@st.cache_data
def load_timeline() -> pd.DataFrame:
    return run_query(
        """
        select *
        from main.mart_recommendations_received_timeline
        order by recommendation_year, recommendation_month
        """
    )


@st.cache_data
def load_detail() -> pd.DataFrame:
    return run_query(
        """
        select
            recommender_name,
            company,
            position,
            recommendation_date,
            visibility,
            text_length,
            mentions_data,
            mentions_teamwork,
            recommendation_text
        from main.int_recommendations_received_enriched
        order by recommendation_date desc nulls last
        """
    )


@st.cache_data
def load_visibility_breakdown() -> pd.DataFrame:
    return run_query(
        """
        select
            visibility,
            count(*) as total_recommendations
        from main.int_recommendations_received_enriched
        group by visibility
        order by total_recommendations desc, visibility
        """
    )


st.title("Recommendations")
st.subheader("Leitura analítica das recomendações recebidas")

df_summary = load_summary()
df_timeline = load_timeline()
df_detail = load_detail()
df_visibility = load_visibility_breakdown()

if df_summary.empty:
    st.warning("Não há recomendações carregadas para análise.")
    st.stop()

summary = df_summary.iloc[0]
total_recommendations = int(summary["total_recommendations"])
avg_text_length = round(float(summary["avg_text_length"]), 2)
mentions_data_count = int(summary["mentions_data_count"])
mentions_teamwork_count = int(summary["mentions_teamwork_count"])
first_date = pd.to_datetime(summary["first_recommendation_date"], errors="coerce")
last_date = pd.to_datetime(summary["last_recommendation_date"], errors="coerce")

c1, c2, c3, c4 = st.columns(4)
with c1:
    render_card(
        "Total de recomendações",
        f"{total_recommendations}",
        "Volume total de recomendações recebidas e tratadas no pipeline.",
        tone="blue",
    )
with c2:
    render_card(
        "Tamanho médio do texto",
        f"{avg_text_length}",
        "Média de caracteres por recomendação, útil para leitura da profundidade narrativa.",
        tone="teal",
    )
with c3:
    render_card(
        "Menções a data",
        f"{mentions_data_count}",
        "Ocorrências classificadas com sinais ligados a dados e analytics.",
        tone="violet",
    )
with c4:
    render_card(
        "Menções a teamwork",
        f"{mentions_teamwork_count}",
        "Ocorrências classificadas com sinais de colaboração e trabalho em equipe.",
        tone="gold",
    )
st.caption(
    "Período coberto: "
    f"{first_date.strftime('%Y-%m-%d') if pd.notnull(first_date) else '-'}"
    " -> "
    f"{last_date.strftime('%Y-%m-%d') if pd.notnull(last_date) else '-'}"
)

st.divider()

left, right = st.columns(2)

with left:
    st.markdown("## Evolução temporal")
    if not df_timeline.empty:
        x_positions = list(range(len(df_timeline)))
        x_labels = df_timeline["recommendation_year_month"].astype(str).tolist()
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(x_positions, df_timeline["total_recommendations"], marker="o", color="#4EA1FF", linewidth=2.4)
        ax.set_title("Recomendações ao longo do tempo")
        ax.set_xlabel("Ano-Mês")
        ax.set_ylabel("Total")
        apply_straight_xticks(ax, x_labels, max_ticks=8)
        style_matplotlib(fig, ax)
        plt.tight_layout()
        st.pyplot(fig)

with right:
    st.markdown("## Visibilidade")
    if not df_visibility.empty:
        if len(df_visibility) > 1:
            fig_vis, ax_vis = plt.subplots(figsize=(8, 5))
            ax_vis.bar(df_visibility["visibility"], df_visibility["total_recommendations"], color="#26C6DA")
            ax_vis.set_title("Distribuição de visibilidade")
            ax_vis.set_xlabel("Visibilidade")
            ax_vis.set_ylabel("Total")
            apply_straight_xticks(ax_vis, df_visibility["visibility"].astype(str).tolist(), max_ticks=6)
            style_matplotlib(fig_vis, ax_vis)
            plt.tight_layout()
            st.pyplot(fig_vis)
        else:
            row = df_visibility.iloc[0]
            render_card(
                "Visibilidade predominante",
                str(row["visibility"]),
                f"Todas as {int(row['total_recommendations'])} recomendações caem na mesma categoria de visibilidade.",
                tone="teal",
                min_height=156,
            )
            render_question(
                "Esse gráfico ajuda mesmo?",
                "Neste caso, não muito.",
                "Como só existe uma classe de visibilidade, o resumo executivo comunica melhor do que uma barra isolada.",
                min_height=156,
            )

st.divider()

tab1, tab2, tab3 = st.tabs(["Linha do tempo", "Visibilidade", "Detalhamento"])
with tab1:
    st.dataframe(df_timeline, width="stretch", hide_index=True)
with tab2:
    st.dataframe(df_visibility, width="stretch", hide_index=True)
with tab3:
    st.dataframe(df_detail, width="stretch", hide_index=True)

st.markdown("## Leitura inicial dos dados")

if mentions_data_count > mentions_teamwork_count:
    dominant_theme = "data"
elif mentions_teamwork_count > mentions_data_count:
    dominant_theme = "teamwork"
else:
    dominant_theme = "equilíbrio entre data e teamwork"

st.info(
    f"O tema mais forte nas recomendações recebidas aponta para **{dominant_theme}** dentro das regras atuais de classificação."
)

if not df_timeline.empty:
    best_row = df_timeline.sort_values("total_recommendations", ascending=False).iloc[0]
    st.success(
        f"O pico de recomendações ocorreu em **{best_row['recommendation_year_month']}**, com **{int(best_row['total_recommendations'])}** registro(s)."
    )
