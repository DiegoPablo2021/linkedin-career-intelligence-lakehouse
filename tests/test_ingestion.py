from __future__ import annotations

from pathlib import Path
from typing import cast
import json

import pandas as pd
import pytest

from linkedin_career_intelligence.config import ProjectSettings, get_settings
from linkedin_career_intelligence.contracts import TableContract, assert_contract, validate_contract
from linkedin_career_intelligence.duckdb_utils import connect_duckdb, write_dataframe
from linkedin_career_intelligence.ingestion import (
    TABLES,
    load_all_tables,
    load_table,
    transform_connections,
    transform_education,
    transform_recommendations_received,
    transform_skills,
)
from scripts.run_pipeline import pipeline_database_guard


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


def test_write_dataframe_append_evolves_schema_by_name(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    settings = build_temp_settings(tmp_path)

    write_dataframe(
        pd.DataFrame([{"id": 1, "name": "alpha"}]),
        "bronze",
        "append_schema_test",
        mode="append",
        settings=settings,
    )
    write_dataframe(
        pd.DataFrame([{"id": 2, "name": "beta", "status": "ok"}]),
        "bronze",
        "append_schema_test",
        mode="append",
        settings=settings,
    )

    conn = connect_duckdb(settings=settings, read_only=True)
    rows = conn.execute(
        "select id, name, status from bronze.append_schema_test order by id"
    ).fetchall()
    conn.close()

    assert rows == [(1, "alpha", None), (2, "beta", "ok")]


def test_assert_contract_rejects_duplicate_unique_columns() -> None:
    df = pd.DataFrame({"skill_name": ["Python", "Python"]})
    contract = TableContract(
        required_columns=("skill_name",),
        unique_columns=("skill_name",),
    )

    with pytest.raises(ValueError, match="Duplicate rows found after transformation"):
        assert_contract(df, contract)


def test_assert_contract_rejects_blank_non_empty_columns() -> None:
    df = pd.DataFrame({"skill_name": ["Python", "", None]})
    contract = TableContract(
        required_columns=("skill_name",),
        non_empty_columns=("skill_name",),
    )

    with pytest.raises(ValueError, match="Null/blank rate above allowed threshold"):
        assert_contract(df, contract)


def test_validate_contract_reports_null_rates_per_column() -> None:
    df = pd.DataFrame({"email_address": ["ana@example.com", None, ""]})
    contract = TableContract(
        required_columns=("email_address",),
        max_null_rate_by_column={"email_address": 0.20},
    )

    validation = validate_contract(df, contract)

    assert validation.null_rates["email_address"] == pytest.approx(2 / 3)
    assert validation.null_rate_violations["email_address"] == pytest.approx(2 / 3)


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
            source_row_count,
            row_count_after_transform,
            rows_removed_during_transform,
            duplicate_rows_after_transform,
            null_rate_by_column,
            contract_owner
        from bronze.ingestion_audit
        """
    ).fetchall()
    conn.close()

    assert bronze_rows == [("Python",), ("SQL",)]
    assert audit_rows == [
        (
            "skills",
            "skills",
            "Skills.csv",
            3,
            2,
            1,
            0,
            json.dumps({"skill_name": 0.0}),
            "LinkedIn member",
        ),
    ]


def test_load_all_tables_rolls_back_on_failure(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    settings = build_temp_settings(tmp_path)
    export_dir = settings.export_dir("complete")
    export_dir.mkdir(parents=True, exist_ok=True)
    (export_dir / TABLES["connections"].csv_name).write_text(
        "\n".join(
            [
                "metadata line 1",
                "metadata line 2",
                "metadata line 3",
                "First Name,Last Name,URL,Email Address,Company,Position,Connected On",
                "Diego,Silva,https://linkedin.com/in/teste,diego@email.com,Open Data,Engineer,2026-04-01",
            ]
        ),
        encoding="utf-8-sig",
    )

    with pytest.raises(FileNotFoundError):
        load_all_tables(settings=settings)

    conn = connect_duckdb(settings=settings, read_only=True)
    connections_exists = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = 'bronze' and table_name = 'connections'
        """
    ).fetchone()
    audit_exists = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = 'bronze' and table_name = 'ingestion_audit'
        """
    ).fetchone()
    conn.close()

    assert connections_exists == (0,)
    assert audit_exists == (0,)


def test_pipeline_database_guard_restores_database_after_failure(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text("estado-original", encoding="utf-8")

    with pytest.raises(RuntimeError):
        with pipeline_database_guard(db_path):
            db_path.write_text("estado-corrompido", encoding="utf-8")
            raise RuntimeError("falha forçada")

    assert db_path.read_text(encoding="utf-8") == "estado-original"


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
        warehouse_dir=warehouse_dir,
        demo_dir=demo_dir,
        db_path=default_db_path,
        default_db_path=default_db_path,
        demo_db_path=demo_db_path,
        is_demo_db=False,
    )
