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


def write_dataframe(
    df: pd.DataFrame,
    schema_name: str,
    table_name: str,
    *,
    mode: str = "replace",
    settings: ProjectSettings | None = None,
) -> None:
    conn = connect_duckdb(settings=settings)
    ensure_core_schemas(conn, extra_schemas=[schema_name])
    conn.register("df_temp", df)

    if mode == "replace":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {schema_name}.{table_name} AS
            SELECT *
            FROM df_temp
            """
        )
    elif mode == "append":
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} AS
            SELECT *
            FROM df_temp
            LIMIT 0
            """
        )
        conn.execute(
            f"""
            INSERT INTO {schema_name}.{table_name}
            SELECT *
            FROM df_temp
            """
        )
    else:
        conn.unregister("df_temp")
        conn.close()
        raise ValueError(f"Unsupported write mode: {mode}")

    conn.unregister("df_temp")
    conn.close()


def write_dataframe_to_bronze(
    df: pd.DataFrame,
    table_name: str,
    *,
    mode: str = "replace",
    settings: ProjectSettings | None = None,
) -> None:
    write_dataframe(
        df,
        "bronze",
        table_name,
        mode=mode,
        settings=settings,
    )
