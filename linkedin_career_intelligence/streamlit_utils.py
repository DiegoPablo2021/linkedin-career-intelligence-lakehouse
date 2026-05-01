from __future__ import annotations

import matplotlib.pyplot as plt
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


@st.cache_data(show_spinner=False)
def load_query(query: str) -> pd.DataFrame:
    return run_query(query)


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


def format_title_case(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().title()


def map_display_value(value: object, mapping: dict[str, str]) -> str:
    if value is None or pd.isna(value):
        return ""
    normalized = str(value).strip()
    return mapping.get(normalized, normalized.title())


def apply_title_case(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    formatted = df.copy()
    for column in columns:
        if column in formatted.columns:
            formatted[column] = formatted[column].apply(format_title_case)
    return formatted


def apply_label_mapping(df: pd.DataFrame, column: str, mapping: dict[str, str]) -> pd.DataFrame:
    formatted = df.copy()
    if column in formatted.columns:
        formatted[column] = formatted[column].apply(lambda value: map_display_value(value, mapping))
    return formatted


def apply_datetime_conversion(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    formatted = df.copy()
    for column in columns:
        if column in formatted.columns:
            formatted[column] = pd.to_datetime(formatted[column], errors="coerce")
    return formatted


def configure_page(page_title: str, subtitle: str | None = None, *, layout: str = "wide") -> None:
    st.set_page_config(page_title=page_title, layout=layout)
    apply_app_theme()
    st.title(page_title)
    if subtitle:
        st.subheader(subtitle)


def render_metric_row(metrics: list[tuple[str, object]]) -> None:
    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics):
        column.metric(label, f"{value}")


def render_dataframe(df: pd.DataFrame, *, hide_index: bool = False) -> None:
    st.dataframe(df, width="stretch", hide_index=hide_index)


def render_author_spotlight(
    *,
    eyebrow: str,
    name: str,
    role: str,
    image_url: str,
    links: list[tuple[str, str]],
) -> None:
    valid_links = []
    for label, url in links:
        if not url:
            continue
        if str(url).startswith("text:"):
            valid_links.append(f'<span class="cci-author-link cci-author-link-text">{label}</span>')
        elif str(url).startswith("mailto:"):
            valid_links.append(f'<a class="cci-author-link" href="{url}" target="_self">{label}</a>')
        else:
            valid_links.append(
                f'<a class="cci-author-link" href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'
            )
    links_html = "".join(valid_links)
    st.markdown(
        f"""
        <div class="cci-author-card">
            <div class="cci-author-eyebrow">{eyebrow}</div>
            <div class="cci-author-layout">
                <img class="cci-author-avatar" src="{image_url}" alt="{name}" />
                <div class="cci-author-copy">
                    <div class="cci-author-name">{name}</div>
                    <div class="cci-author-role">{role}</div>
                    <div class="cci-author-links">{links_html}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
        padding-top: 3.4rem;
        padding-bottom: 2rem;
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
        padding: 1.05rem 1.25rem;
        margin-top: 0.55rem;
        margin-bottom: 1.2rem;
        border: 1px solid rgba(124, 196, 255, 0.28);
        background: linear-gradient(135deg, rgba(78, 161, 255, 0.16), rgba(38, 198, 218, 0.09));
        color: var(--cci-text-primary);
        font-size: 0.92rem;
        line-height: 1.6;
        white-space: normal;
        overflow-wrap: normal;
        word-break: normal;
        width: 100%;
        box-sizing: border-box;
    }

    .cci-author-card {
        background: linear-gradient(180deg, rgba(17, 36, 62, 0.92), rgba(11, 20, 36, 0.96));
        border: 1px solid var(--cci-border);
        border-radius: 22px;
        padding: 1.15rem 1.25rem;
        margin-bottom: 1.25rem;
        box-shadow: 0 18px 40px rgba(0, 0, 0, 0.18);
    }

    .cci-author-eyebrow {
        color: var(--cci-text-primary);
        font-size: 0.88rem;
        font-weight: 700;
        margin-bottom: 0.9rem;
    }

    .cci-author-layout {
        display: flex;
        align-items: center;
        gap: 1rem;
        flex-wrap: wrap;
    }

    .cci-author-avatar {
        width: 96px;
        height: 96px;
        border-radius: 999px;
        object-fit: cover;
        border: 3px solid rgba(78, 161, 255, 0.92);
        flex: 0 0 auto;
    }

    .cci-author-copy {
        min-width: 220px;
        flex: 1 1 320px;
    }

    .cci-author-name {
        color: var(--cci-text-primary);
        font-size: 1.85rem;
        font-weight: 800;
        line-height: 1.2;
        margin-bottom: 0.35rem;
    }

    .cci-author-role {
        color: var(--cci-text-secondary);
        font-size: 1.02rem;
        line-height: 1.5;
        margin-bottom: 0.7rem;
    }

    .cci-author-links {
        display: flex;
        flex-wrap: wrap;
        gap: 0.9rem;
    }

    .cci-author-link {
        color: #26A7FF;
        font-size: 0.98rem;
        font-weight: 600;
        text-decoration: none;
    }

    .cci-author-link-text {
        cursor: text;
    }

    .cci-author-link:hover {
        text-decoration: underline;
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
                <strong>Demo pública:</strong> dados sanitizados, contatos mascarados e texto narrativo genérico. Os dados privados de origem permanecem no ambiente local.
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_card(
    title: str,
    value: str,
    body: str,
    tone: str = "blue",
    *,
    min_height: int = 160,
) -> None:
    st.markdown(
        f"""
        <div class="cci-card cci-card-{tone}" style="min-height: {min_height}px;">
            <div class="cci-card-title">{title}</div>
            <div class="cci-card-value">{value}</div>
            <div class="cci-card-body">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_question(question: str, answer: str, detail: str = "", *, min_height: int | None = None) -> None:
    detail_html = f'<div class="cci-question-detail">{detail}</div>' if detail else ""
    min_height_style = f'style="min-height: {min_height}px;"' if min_height is not None else ""
    st.markdown(
        f"""
        <div class="cci-question" {min_height_style}>
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


def render_time_series_chart(
    df: pd.DataFrame,
    *,
    x_labels: list[str],
    y_column: str,
    title: str,
    x_label: str,
    y_label: str,
    color: str,
    kind: str = "line",
    max_ticks: int = 10,
    figsize: tuple[float, float] = (12, 5),
) -> None:
    x_positions = list(range(len(df)))
    fig, ax = plt.subplots(figsize=figsize)
    if kind == "line":
        ax.plot(x_positions, df[y_column], marker="o", color=color, linewidth=2.4)
    elif kind == "bar":
        ax.bar(x_positions, df[y_column], color=color)
    else:
        raise ValueError(f"Unsupported chart kind: {kind}")

    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    apply_straight_xticks(ax, x_labels, max_ticks=max_ticks)
    style_matplotlib(fig, ax)
    plt.tight_layout()
    st.pyplot(fig)


def render_horizontal_bar_chart(
    df: pd.DataFrame,
    *,
    label_column: str,
    value_column: str,
    title: str,
    x_label: str,
    y_label: str,
    color: str,
    figsize: tuple[float, float] = (10, 6),
) -> None:
    fig, ax = plt.subplots(figsize=figsize)
    ax.barh(df[label_column], df[value_column], color=color)
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.invert_yaxis()
    style_matplotlib(fig, ax)
    plt.tight_layout()
    st.pyplot(fig)
