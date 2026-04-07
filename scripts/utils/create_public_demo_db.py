from __future__ import annotations

from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd

from linkedin_career_intelligence.config import get_settings


PERSON_EXACT_COLUMNS = {
    "first_name",
    "last_name",
    "full_name",
    "maiden_name",
    "recommender_name",
    "endorser_full_name",
}
PERSON_SUFFIXES = ("_name",)
PERSON_EXCLUDE_KEYWORDS = (
    "organization",
    "company",
    "school",
    "skill",
    "category",
    "track",
    "label",
    "visibility",
    "content_type",
    "job_family",
    "status",
)
ORGANIZATION_KEYWORDS = ("company", "organization", "school", "institution", "issuer")
ROLE_KEYWORDS = ("position", "title", "role")
SAFE_URL_KEYWORDS = ("url", "website", "linkedin", "twitter", "messenger")
SAFE_TEXT_COLUMNS = {
    "headline": "Analytics engineer focused on reproducible data pipelines and portfolio storytelling.",
    "summary": (
        "Public demo summary. Personal narrative was sanitized while preserving the analytical structure, "
        "career positioning and storytelling flow used by the application."
    ),
    "recommendation_text": (
        "Public recommendation text sanitized for demo deployment. The original message was replaced while "
        "keeping the analytical experience of the page."
    ),
}
NULL_COLUMNS = {"address", "zip_code", "birth_date"}


def tokenize_series(series: pd.Series, prefix: str, formatter: Callable[[int], str] | None = None) -> pd.Series:
    formatter = formatter or (lambda index: f"{prefix} {index:03d}")
    cleaned = series.astype("string")
    unique_values = [value for value in cleaned.dropna().unique().tolist() if str(value).strip()]
    mapping = {value: formatter(index) for index, value in enumerate(sorted(unique_values), start=1)}
    return cleaned.map(mapping).astype("string")


def mask_email_series(series: pd.Series) -> pd.Series:
    return tokenize_series(series, "contact", formatter=lambda index: f"contact{index:03d}@example.com")


def mask_url_series(series: pd.Series, slug: str) -> pd.Series:
    return tokenize_series(series, slug, formatter=lambda index: f"https://example.com/{slug}/{index:03d}")


def is_person_column(column_name: str) -> bool:
    normalized = column_name.lower()
    if normalized in PERSON_EXACT_COLUMNS:
        return True
    if normalized.endswith(PERSON_SUFFIXES) and not any(keyword in normalized for keyword in PERSON_EXCLUDE_KEYWORDS):
        return True
    return False


def is_textual_series(series: pd.Series) -> bool:
    if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return True

    numeric_candidate = pd.to_numeric(non_null, errors="coerce")
    if numeric_candidate.notna().all():
        return False

    return True


def coarsen_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        normalized = column.lower()
        series = df[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            df[column] = series.dt.to_period("M").dt.to_timestamp()
            continue

        looks_like_datetime = any(token in normalized for token in ("date", "_at", "_on", "timestamp"))
        if not looks_like_datetime or not pd.api.types.is_object_dtype(series):
            continue

        parsed = pd.to_datetime(series, errors="coerce")
        if parsed.notna().sum() == 0:
            continue
        df[column] = parsed.dt.to_period("M").dt.to_timestamp()
    return df


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    sanitized = df.copy()
    sanitized = coarsen_datetime_columns(sanitized)

    for column in sanitized.columns:
        normalized = column.lower()
        series = sanitized[column]

        if normalized == "email_address":
            sanitized[column] = mask_email_series(series)
            continue

        if normalized in NULL_COLUMNS:
            sanitized[column] = pd.Series([pd.NA] * len(sanitized), dtype="string")
            continue

        if not is_textual_series(series):
            continue

        if is_person_column(normalized):
            sanitized[column] = tokenize_series(series, "Pessoa")
            continue

        if any(keyword in normalized for keyword in ORGANIZATION_KEYWORDS):
            sanitized[column] = tokenize_series(series, "Empresa")
            continue

        if any(keyword in normalized for keyword in ROLE_KEYWORDS):
            sanitized[column] = tokenize_series(series, "Cargo")
            continue

        if "location" in normalized:
            sanitized[column] = tokenize_series(series, "Regiao")
            continue

        if normalized in SAFE_TEXT_COLUMNS:
            sanitized[column] = series.astype("string").where(series.isna(), SAFE_TEXT_COLUMNS[normalized])
            continue

        if any(keyword in normalized for keyword in SAFE_URL_KEYWORDS):
            sanitized[column] = mask_url_series(series, normalized.replace("_", "-"))
            continue

    if {"first_name", "last_name", "full_name"}.issubset(sanitized.columns):
        full_name = (
            sanitized["first_name"].fillna("").astype("string").str.strip()
            + " "
            + sanitized["last_name"].fillna("").astype("string").str.strip()
        ).str.strip()
        sanitized["full_name"] = full_name.where(full_name != "", pd.NA)

    return sanitized


def list_main_objects(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        """
        select table_name
        from information_schema.tables
        where table_schema = 'main'
        order by table_name
        """
    ).fetchall()
    return [row[0] for row in rows]


def build_demo_database(source_db_path: Path, target_db_path: Path) -> None:
    source_conn = duckdb.connect(str(source_db_path), read_only=True)
    target_db_path.parent.mkdir(parents=True, exist_ok=True)
    if target_db_path.exists():
        target_db_path.unlink()

    target_conn = duckdb.connect(str(target_db_path))
    target_conn.execute("create schema if not exists main")

    main_objects = list_main_objects(source_conn)
    if not main_objects:
        raise RuntimeError("No objects found in schema main. Run the pipeline before generating the demo database.")

    for table_name in main_objects:
        table_df = source_conn.execute(f'select * from main."{table_name}"').fetchdf()
        sanitized_df = sanitize_dataframe(table_df)
        target_conn.register("sanitized_df", sanitized_df)
        target_conn.execute(f'create or replace table main."{table_name}" as select * from sanitized_df')
        target_conn.unregister("sanitized_df")
        print(f"Sanitized main.{table_name} ({len(sanitized_df):,} rows)")

    target_conn.close()
    source_conn.close()


def main() -> None:
    settings = get_settings()
    if not settings.default_db_path.exists():
        raise FileNotFoundError(
            f"Source database not found at {settings.default_db_path}. Run the local pipeline before generating the public demo."
        )

    build_demo_database(settings.default_db_path, settings.demo_db_path)
    print(f"\nPublic demo database created at: {settings.demo_db_path}")


if __name__ == "__main__":
    main()
