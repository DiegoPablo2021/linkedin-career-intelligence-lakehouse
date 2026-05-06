from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from linkedin_career_intelligence.config import ProjectSettings, get_settings


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def connect_duckdb(
    read_only: bool = False,
    settings: ProjectSettings | None = None,
) -> duckdb.DuckDBPyConnection:
    settings = settings or get_settings()
    ensure_parent_dir(settings.db_path)
    return duckdb.connect(str(settings.db_path), read_only=read_only)


def ensure_core_schemas(
    conn: duckdb.DuckDBPyConnection, extra_schemas: Iterable[str] | None = None
) -> None:
    schemas = ["raw", "bronze", "silver", "gold"]
    if extra_schemas:
        schemas.extend(extra_schemas)

    for schema in dict.fromkeys(schemas):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _evolve_table_schema_for_append(
    conn: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
) -> None:
    existing_columns = {
        row[0].lower()
        for row in conn.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ? and table_name = ?
            """,
            [schema_name, table_name],
        ).fetchall()
    }
    df_columns = conn.execute("describe select * from df_temp").fetchall()
    for column_name, column_type, *_ in df_columns:
        if column_name.lower() in existing_columns:
            continue
        conn.execute(
            f"""
            alter table {_quote_identifier(schema_name)}.{_quote_identifier(table_name)}
            add column {_quote_identifier(column_name)} {column_type}
            """
        )


def write_dataframe(
    df: pd.DataFrame,
    schema_name: str,
    table_name: str,
    *,
    mode: str = "replace",
    settings: ProjectSettings | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    active_conn = conn or connect_duckdb(settings=settings)
    should_close_conn = conn is None
    ensure_core_schemas(active_conn, extra_schemas=[schema_name])
    active_conn.register("df_temp", df)

    try:
        if mode == "replace":
            active_conn.execute(
                f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS
                SELECT *
                FROM df_temp
                """
            )
        elif mode == "append":
            active_conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} AS
                SELECT *
                FROM df_temp
                LIMIT 0
                """
            )
            _evolve_table_schema_for_append(active_conn, schema_name, table_name)
            active_conn.execute(
                f"""
                INSERT INTO {schema_name}.{table_name} BY NAME
                SELECT *
                FROM df_temp
                """
            )
        else:
            raise ValueError(f"Unsupported write mode: {mode}")
    finally:
        active_conn.unregister("df_temp")
        if should_close_conn:
            active_conn.close()


def write_dataframe_to_bronze(
    df: pd.DataFrame,
    table_name: str,
    *,
    mode: str = "replace",
    settings: ProjectSettings | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> None:
    write_dataframe(
        df,
        "bronze",
        table_name,
        mode=mode,
        settings=settings,
        conn=conn,
    )
