from __future__ import annotations

from typing import Any
from pathlib import Path

import pandas as pd

from linkedin_career_intelligence.config import get_settings


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def connect_duckdb(read_only: bool = False) -> Any:
    import duckdb

    settings = get_settings()
    ensure_parent_dir(settings.db_path)
    return duckdb.connect(str(settings.db_path), read_only=read_only)


def ensure_core_schemas(conn: Any) -> None:
    for schema in ("raw", "bronze", "silver", "gold"):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def write_dataframe_to_bronze(df: pd.DataFrame, table_name: str) -> None:
    conn = connect_duckdb()
    ensure_core_schemas(conn)
    conn.register("df_temp", df)
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE bronze.{table_name} AS
        SELECT *
        FROM df_temp
        """
    )
    conn.close()
