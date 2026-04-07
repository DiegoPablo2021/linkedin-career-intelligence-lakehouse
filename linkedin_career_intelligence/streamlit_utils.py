from __future__ import annotations

import pandas as pd
import streamlit as st
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from linkedin_career_intelligence.config import get_settings
from linkedin_career_intelligence.duckdb_utils import connect_duckdb


def get_db_path() -> str:
    return str(get_settings().db_path)


def is_demo_mode() -> bool:
    return get_settings().is_demo_db


def run_query(query: str) -> pd.DataFrame:
    conn = connect_duckdb(read_only=True)
    df = conn.execute(query).fetchdf()
    conn.close()
    return df


def ui_text(pt_text: str, en_text: str) -> str:
    return pt_text


def safe_int(value: object, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return default
    return int(value)


def safe_float(value: object, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    return float(value)


def safe_sum(*values: object) -> int:
    return sum(safe_int(value) for value in values)


def apply_app_theme() -> None:
    bg_primary = "#0E1117"
    bg_secondary = "#111827"
    text_primary = "#F5F7FA"
    text_secondary = "#B7C7DE"
    border_color = "#233044"
    grid_color = "#233044"

    css = """
    <style>
    :root {
        --cci-bg-primary: __BG_PRIMARY__;
        --cci-bg-secondary: __BG_SECONDARY__;
        --cci-text-primary: __TEXT_PRIMARY__;
        --cci-text-secondary: __TEXT_SECONDARY__;
        --cci-border: __BORDER_COLOR__;
        --cci-grid: __GRID_COLOR__;
    }

    .block-container {
        padding-top: 2.25rem;
    }

    .stApp {
        background: var(--cci-bg-primary);
    }

    .cci-hero {
        background: linear-gradient(135deg, rgba(78, 161, 255, 0.18), rgba(22, 163, 74, 0.12));
        border: 1px solid var(--cci-border);
        border-radius: 22px;
        padding: 1.35rem 1.5rem;
        margin-bottom: 1.1rem;
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.18);
    }

    .cci-card {
        border-radius: 20px;
        padding: 1.1rem 1.15rem;
        min-height: 160px;
        border: 1px solid var(--cci-border);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.02);
        margin-bottom: 1rem;
    }

    .cci-card-blue {
        background: linear-gradient(180deg, rgba(17, 36, 62, 0.96), rgba(11, 20, 36, 0.96));
    }

    .cci-card-teal {
        background: linear-gradient(180deg, rgba(12, 52, 61, 0.96), rgba(8, 25, 31, 0.96));
    }

    .cci-card-gold {
        background: linear-gradient(180deg, rgba(73, 56, 15, 0.96), rgba(33, 24, 8, 0.96));
    }

    .cci-card-violet {
        background: linear-gradient(180deg, rgba(41, 30, 73, 0.96), rgba(19, 13, 35, 0.96));
    }

    .cci-card-slate {
        background: linear-gradient(180deg, rgba(28, 36, 50, 0.96), rgba(15, 20, 29, 0.96));
    }

    .cci-card-title {
        color: var(--cci-text-primary);
        font-size: 0.92rem;
        font-weight: 700;
        margin-bottom: 0.55rem;
    }

    .cci-card-value {
        color: var(--cci-text-primary);
        font-size: 2rem;
        font-weight: 800;
        line-height: 1.15;
        margin-bottom: 0.45rem;
    }

    .cci-card-body {
        color: var(--cci-text-secondary);
        font-size: 0.96rem;
        line-height: 1.45;
    }

    .cci-question {
        border-radius: 18px;
        padding: 1rem 1.1rem;
        background: var(--cci-bg-secondary);
        border: 1px solid var(--cci-border);
        margin-bottom: 0.85rem;
    }

    .cci-question-title {
        color: #7CC4FF;
        font-size: 0.88rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 0.35rem;
    }

    .cci-question-answer {
        color: var(--cci-text-primary);
        font-size: 1rem;
        font-weight: 600;
        line-height: 1.45;
    }

    .cci-question-detail {
        color: var(--cci-text-secondary);
        font-size: 0.95rem;
        margin-top: 0.35rem;
        line-height: 1.45;
    }

    .cci-demo-banner {
        border-radius: 16px;
        padding: 1rem 1.15rem;
        margin-bottom: 1.2rem;
        border: 1px solid rgba(124, 196, 255, 0.28);
        background: linear-gradient(135deg, rgba(78, 161, 255, 0.16), rgba(38, 198, 218, 0.09));
        color: var(--cci-text-primary);
        font-size: 0.95rem;
        line-height: 1.6;
        white-space: normal;
        overflow-wrap: anywhere;
        word-break: break-word;
    }
    </style>
    """
    css = (
        css.replace("__BG_PRIMARY__", bg_primary)
        .replace("__BG_SECONDARY__", bg_secondary)
        .replace("__TEXT_PRIMARY__", text_primary)
        .replace("__TEXT_SECONDARY__", text_secondary)
        .replace("__BORDER_COLOR__", border_color)
        .replace("__GRID_COLOR__", grid_color)
    )
    st.markdown(css, unsafe_allow_html=True)
    if is_demo_mode():
        st.markdown(
            """
            <div class="cci-demo-banner">
                <strong>Public demo mode:</strong> this deployment uses a sanitized DuckDB file with anonymized names,
                masked contact channels, and generic narrative text. The local project continues to use the private source database.
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_card(title: str, value: str, body: str, tone: str = "blue") -> None:
    st.markdown(
        f"""
        <div class="cci-card cci-card-{tone}">
            <div class="cci-card-title">{title}</div>
            <div class="cci-card-value">{value}</div>
            <div class="cci-card-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_question(question: str, answer: str, detail: str = "") -> None:
    detail_html = f'<div class="cci-question-detail">{detail}</div>' if detail else ""
    st.markdown(
        f"""
        <div class="cci-question">
            <div class="cci-question-title">{question}</div>
            <div class="cci-question-answer">{answer}</div>
            {detail_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_matplotlib(fig: Figure, ax: Axes) -> None:
    fig.patch.set_facecolor("#0E1117")
    ax.set_facecolor("#111827")
    ax.title.set_color("#F8FAFC")
    ax.xaxis.label.set_color("#C6D4E5")
    ax.yaxis.label.set_color("#C6D4E5")
    ax.tick_params(colors="#C6D4E5")
    for spine in ax.spines.values():
        spine.set_color("#2A3B55")
    ax.grid(axis="y", color="#233044", linestyle="--", linewidth=0.7, alpha=0.6)
    ax.set_axisbelow(True)


def apply_straight_xticks(ax: Axes, labels: list[str], max_ticks: int = 8) -> list[int]:
    total = len(labels)
    if total == 0:
        return []

    if total <= max_ticks:
        selected = list(range(total))
    else:
        denominator = max(1, max_ticks - 1)
        selected = sorted(
            {
                int(round((idx * (total - 1)) / denominator))
                for idx in range(max_ticks)
            }
        )

    ax.set_xticks(selected, [labels[idx] for idx in selected])
    ax.tick_params(axis="x", rotation=0)
    return selected
