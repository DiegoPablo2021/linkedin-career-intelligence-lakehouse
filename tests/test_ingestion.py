from __future__ import annotations

from pathlib import Path
from typing import cast

import pandas as pd
import pytest

from linkedin_career_intelligence.config import ProjectSettings, get_settings
from linkedin_career_intelligence.duckdb_utils import connect_duckdb, write_dataframe
from linkedin_career_intelligence.ingestion import (
    TABLES,
    load_table,
    transform_connections,
    transform_education,
    transform_recommendations_received,
    transform_skills,
)


def test_transform_education_normalizes_columns_and_flags_current() -> None:
    raw = pd.DataFrame(
        {
            "School": ["UFSC"],
            "Degree": ["Sistemas de Informacao"],
            "Description": ["Curso superior"],
            "Started": ["2020-02-01"],
            "Finished": [None],
        }
    )

    result = transform_education(raw)

    assert list(result.columns) == [
        "school_name",
        "degree_name",
        "notes",
        "started_on",
        "finished_on",
        "is_current_education",
    ]
    assert result.loc[0, "school_name"] == "UFSC"
    assert bool(result.loc[0, "is_current_education"]) is True


def test_transform_recommendations_received_maps_expected_columns() -> None:
    raw = pd.DataFrame(
        {
            "First Name": ["Ana"],
            "Last Name": ["Silva"],
            "Company": ["ACME"],
            "Job Title": ["Data Analyst"],
            "Text": ["Excelente parceria profissional."],
            "Creation Date": ["04/05/26, 09:15 AM"],
            "Status": ["VISIBLE"],
        }
    )

    result = transform_recommendations_received(raw)

    assert "position" in result.columns
    assert "recommendation_text" in result.columns
    assert result.loc[0, "position"] == "Data Analyst"
    assert pd.notna(result.loc[0, "recommendation_date"])


def test_transform_connections_handles_skiprow_output_shape() -> None:
    raw = pd.DataFrame(
        {
            "First Name": ["Diego"],
            "Last Name": ["Silva"],
            "URL": ["https://linkedin.com/in/teste"],
            "Email Address": ["diego@email.com"],
            "Company": ["Open Data"],
            "Position": ["Engineer"],
            "Connected On": ["2026-04-01"],
        }
    )

    result = transform_connections(raw)

    assert list(result.columns) == [
        "first_name",
        "last_name",
        "url",
        "email_address",
        "company",
        "position",
        "connected_on",
    ]
    connected_on = cast(pd.Timestamp, result.loc[0, "connected_on"])
    assert connected_on.date().isoformat() == "2026-04-01"


def test_transform_skills_removes_duplicate_and_blank_rows() -> None:
    raw = pd.DataFrame({"Name": ["Python", " Python ", "", None, "Python", "SQL"]})

    result = transform_skills(raw)

    assert result.to_dict(orient="records") == [
        {"skill_name": "Python"},
        {"skill_name": "SQL"},
    ]


def test_settings_points_to_warehouse_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LINKEDIN_DB_PATH", raising=False)
    monkeypatch.delenv("LINKEDIN_DUCKDB_PATH", raising=False)
    settings = get_settings()
    assert settings.db_path.name == "linkedin_career_intelligence.duckdb"
    assert settings.db_path.parent.name == "warehouse"


def test_write_dataframe_supports_append_mode(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    settings = build_temp_settings(tmp_path)

    write_dataframe(
        pd.DataFrame([{"id": 1, "name": "alpha"}]),
        "bronze",
        "append_test",
        mode="append",
        settings=settings,
    )
    write_dataframe(
        pd.DataFrame([{"id": 2, "name": "beta"}]),
        "bronze",
        "append_test",
        mode="append",
        settings=settings,
    )

    conn = connect_duckdb(settings=settings, read_only=True)
    rows = conn.execute(
        "select id, name from bronze.append_test order by id"
    ).fetchall()
    conn.close()

    assert rows == [(1, "alpha"), (2, "beta")]


def test_load_table_persists_bronze_and_ingestion_audit(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    settings = build_temp_settings(tmp_path)
    export_dir = settings.export_dir("complete")
    export_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"Name": ["Python", "Python", "SQL"]}).to_csv(
        export_dir / TABLES["skills"].csv_name,
        index=False,
        encoding="utf-8-sig",
    )

    result = load_table("skills", settings=settings)

    assert result.to_dict(orient="records") == [
        {"skill_name": "Python"},
        {"skill_name": "SQL"},
    ]

    conn = connect_duckdb(settings=settings, read_only=True)
    bronze_rows = conn.execute(
        "select skill_name from bronze.skills order by skill_name"
    ).fetchall()
    audit_rows = conn.execute(
        """
        select
            table_key,
            bronze_table,
            source_file,
            row_count,
            duplicate_rows_after_transform,
            contract_owner
        from bronze.ingestion_audit
        """
    ).fetchall()
    conn.close()

    assert bronze_rows == [("Python",), ("SQL",)]
    assert audit_rows == [
        ("skills", "skills", "Skills.csv", 2, 0, "LinkedIn member"),
    ]


def build_temp_settings(tmp_path: Path) -> ProjectSettings:
    project_root = tmp_path
    data_dir = project_root / "data"
    warehouse_dir = project_root / "warehouse"
    demo_dir = project_root / "demo"
    default_db_path = warehouse_dir / "linkedin_career_intelligence.duckdb"
    demo_db_path = demo_dir / "linkedin_career_intelligence_demo.duckdb"
    return ProjectSettings(
        project_root=project_root,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        manual_exports_dir=data_dir / "raw" / "linkedin_exports",
        warehouse_dir=warehouse_dir,
        demo_dir=demo_dir,
        db_path=default_db_path,
        default_db_path=default_db_path,
        demo_db_path=demo_db_path,
        is_demo_db=False,
    )
