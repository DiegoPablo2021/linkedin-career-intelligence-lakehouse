from __future__ import annotations

import pandas as pd

from linkedin_career_intelligence.config import get_settings
from linkedin_career_intelligence.ingestion import (
    transform_education,
    transform_recommendations_received,
    transform_connections,
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
    assert str(result.loc[0, "connected_on"].date()) == "2026-04-01"


def test_settings_points_to_warehouse_database() -> None:
    settings = get_settings()
    assert settings.db_path.name == "linkedin_career_intelligence.duckdb"
    assert settings.db_path.parent.name == "warehouse"
