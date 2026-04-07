import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from linkedin_career_intelligence.streamlit_utils import (
    apply_app_theme,
    apply_straight_xticks,
    run_query,
    style_matplotlib,
)


st.set_page_config(page_title="Engagement", layout="wide")
apply_app_theme()


@st.cache_data
def load_invitations_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_invitations_summary order by invitation_year, invitation_month, direction")


@st.cache_data
def load_events_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_events_summary order by event_year, event_month, status")


@st.cache_data
def load_volunteering_summary() -> pd.DataFrame:
    return run_query("select * from main.mart_volunteering_summary order by total_volunteering_roles desc")


@st.cache_data
def load_invitations_detail() -> pd.DataFrame:
    return run_query("select * from main.int_invitations_enriched order by sent_at desc")


@st.cache_data
def load_events_detail() -> pd.DataFrame:
    return run_query("select * from main.int_events_enriched order by started_at desc")


@st.cache_data
def load_volunteering_detail() -> pd.DataFrame:
    return run_query("select * from main.int_volunteering_enriched order by started_on desc")


st.title("Engagement")
st.subheader("Convites, eventos e voluntariado como sinais de presença profissional")

df_inv = load_invitations_summary()
df_evt = load_events_summary()
df_vol = load_volunteering_summary()
df_inv_detail = load_invitations_detail()
df_evt_detail = load_events_detail()
df_vol_detail = load_volunteering_detail()

c1, c2, c3 = st.columns(3)
c1.metric("Convites", int(df_inv_detail.shape[0]) if not df_inv_detail.empty else 0)
c2.metric("Eventos", int(df_evt_detail.shape[0]) if not df_evt_detail.empty else 0)
c3.metric("Voluntariado", int(df_vol_detail.shape[0]) if not df_vol_detail.empty else 0)

st.divider()

left, right = st.columns(2)
with left:
    st.markdown("## Convites por direção")
    if not df_inv.empty:
        chart_df = df_inv.groupby("direction", as_index=False)["total_invitations"].sum()
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.bar(chart_df["direction"], chart_df["total_invitations"], color="#4EA1FF")
        ax.set_title("Convites por direção")
        ax.set_xlabel("Direção")
        ax.set_ylabel("Total")
        apply_straight_xticks(ax, chart_df["direction"].astype(str).tolist(), max_ticks=6)
        style_matplotlib(fig, ax)
        plt.tight_layout()
        st.pyplot(fig)
        st.dataframe(df_inv, width="stretch")

with right:
    st.markdown("## Eventos por status")
    if not df_evt.empty:
        chart_df = df_evt.groupby("status", as_index=False)["total_events"].sum()
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.bar(chart_df["status"], chart_df["total_events"], color="#26C6DA")
        ax.set_title("Eventos por status")
        ax.set_xlabel("Status")
        ax.set_ylabel("Total")
        apply_straight_xticks(ax, chart_df["status"].astype(str).tolist(), max_ticks=6)
        style_matplotlib(fig, ax)
        plt.tight_layout()
        st.pyplot(fig)
        st.dataframe(df_evt, width="stretch")

st.divider()
st.markdown("## Voluntariado")
st.dataframe(df_vol, width="stretch")
st.dataframe(df_vol_detail, width="stretch")

st.markdown("## Detalhes de convites")
st.dataframe(df_inv_detail, width="stretch")

st.markdown("## Detalhes de eventos")
st.dataframe(df_evt_detail, width="stretch")
