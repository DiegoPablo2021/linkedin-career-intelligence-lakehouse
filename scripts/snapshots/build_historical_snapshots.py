from __future__ import annotations

import json
import logging
import subprocess
import sys
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.config import get_settings
from linkedin_career_intelligence.duckdb_utils import connect_duckdb
from scripts.snapshots.snapshot_contracts import (
    ACTUAL_MONTHLY_AGGREGATE,
    ACTUAL_PIPELINE_AUDIT,
    CUMULATIVE_FROM_EVENT_HISTORY,
    SNAPSHOT_METHOD_DEFINITIONS,
    SNAPSHOT_TABLE_CONTRACTS,
    SnapshotTableContract,
)


LOGGER = logging.getLogger("historical_snapshots")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_git_commit(project_root: Path) -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def list_existing_tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        f"{row[0]}.{row[1]}"
        for row in conn.execute(
            """
            select table_schema, table_name
            from information_schema.tables
            """
        ).fetchall()
    }


def assert_required_sources(conn: duckdb.DuckDBPyConnection, table_names: list[str]) -> None:
    existing = list_existing_tables(conn)
    missing = sorted(set(table_names) - existing)
    if missing:
        raise RuntimeError(f"Missing required marts/tables in DuckDB: {missing}")


def month_end_from_string(series: pd.Series) -> pd.Series:
    return pd.PeriodIndex(series.astype(str), freq="M").to_timestamp("M")


def safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator_safe = denominator.where(denominator != 0)
    return pd.to_numeric(numerator / denominator_safe, errors="coerce").fillna(0.0)


def write_main_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    dataframe: pd.DataFrame,
) -> None:
    conn.register("snapshot_df_temp", dataframe)
    try:
        conn.execute(
            f"""
            create or replace table {table_name} as
            select *
            from snapshot_df_temp
            """
        )
    finally:
        conn.unregister("snapshot_df_temp")


def append_main_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    dataframe: pd.DataFrame,
) -> None:
    conn.register("snapshot_df_temp", dataframe)
    try:
        conn.execute(
            f"""
            create table if not exists {table_name} as
            select *
            from snapshot_df_temp
            limit 0
            """
        )
        conn.execute(
            f"""
            insert into {table_name}
            select *
            from snapshot_df_temp
            """
        )
    finally:
        conn.unregister("snapshot_df_temp")


def build_dim_snapshot_method() -> pd.DataFrame:
    return pd.DataFrame([asdict(item) for item in SNAPSHOT_METHOD_DEFINITIONS])


def build_dim_snapshot_run(run_id: str, settings_path: Path, started_at: datetime, finished_at: datetime, project_root: Path) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "snapshot_run_id": run_id,
                "run_started_at": started_at,
                "run_finished_at": finished_at,
                "phase_name": "phase_1_actual_only",
                "contains_simulated_data": False,
                "source_warehouse_path": str(settings_path),
                "project_git_commit": get_git_commit(project_root),
                "notes": "Only actual monthly aggregates, cumulative event history, and actual pipeline audit were used.",
            }
        ]
    )


def build_network_growth_snapshot(conn: duckdb.DuckDBPyConnection, run_id: str) -> pd.DataFrame:
    monthly = conn.execute(
        """
        select
            connection_year_month as snapshot_year_month,
            total_connections as monthly_new_connections,
            connections_with_email as monthly_new_connections_with_email,
            unique_companies as unique_companies_in_month,
            unique_positions as unique_positions_in_month
        from mart_connections_summary
        order by connection_year_month
        """
    ).fetchdf()
    monthly["snapshot_date"] = month_end_from_string(monthly["snapshot_year_month"])
    monthly["total_connections_cumulative"] = monthly["monthly_new_connections"].cumsum()
    monthly["connections_with_email_cumulative"] = monthly["monthly_new_connections_with_email"].cumsum()
    monthly["connections_with_email_pct"] = safe_pct(
        monthly["connections_with_email_cumulative"],
        monthly["total_connections_cumulative"],
    )
    monthly["connections_mom_delta"] = monthly["monthly_new_connections"].diff().fillna(0).astype(int)
    previous = monthly["monthly_new_connections"].shift(1)
    monthly["connections_mom_pct"] = ((monthly["monthly_new_connections"] - previous) / previous.where(previous != 0)).fillna(0.0)
    monthly["snapshot_run_id"] = run_id
    monthly["is_simulated_snapshot"] = False
    monthly["snapshot_method"] = CUMULATIVE_FROM_EVENT_HISTORY
    monthly["snapshot_source"] = "mart_connections_summary"
    monthly["snapshot_created_at"] = utcnow()
    return monthly[
        [
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "monthly_new_connections",
            "total_connections_cumulative",
            "monthly_new_connections_with_email",
            "connections_with_email_cumulative",
            "connections_with_email_pct",
            "unique_companies_in_month",
            "unique_positions_in_month",
            "connections_mom_delta",
            "connections_mom_pct",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
            "snapshot_created_at",
        ]
    ]


def build_applications_snapshot(conn: duckdb.DuckDBPyConnection, run_id: str) -> pd.DataFrame:
    monthly = conn.execute(
        """
        select
            application_year_month as snapshot_year_month,
            sum(total_applications) as total_applications,
            sum(applications_with_resume) as applications_with_resume,
            sum(applications_with_questionnaire) as applications_with_questionnaire,
            count(distinct job_family) as job_family_count
        from mart_job_applications_summary
        group by application_year_month
        order by application_year_month
        """
    ).fetchdf()
    monthly["snapshot_date"] = month_end_from_string(monthly["snapshot_year_month"])
    monthly["cumulative_total_applications"] = monthly["total_applications"].cumsum()
    monthly["cumulative_applications_with_resume"] = monthly["applications_with_resume"].cumsum()
    monthly["cumulative_applications_with_questionnaire"] = monthly["applications_with_questionnaire"].cumsum()
    monthly["applications_with_resume_pct"] = safe_pct(
        monthly["applications_with_resume"], monthly["total_applications"]
    )
    monthly["applications_with_questionnaire_pct"] = safe_pct(
        monthly["applications_with_questionnaire"], monthly["total_applications"]
    )
    monthly["applications_mom_delta"] = monthly["total_applications"].diff().fillna(0).astype(int)
    previous = monthly["total_applications"].shift(1)
    monthly["applications_mom_pct"] = ((monthly["total_applications"] - previous) / previous.where(previous != 0)).fillna(0.0)
    monthly["snapshot_run_id"] = run_id
    monthly["is_simulated_snapshot"] = False
    monthly["snapshot_method"] = CUMULATIVE_FROM_EVENT_HISTORY
    monthly["snapshot_source"] = "mart_job_applications_summary"
    monthly["snapshot_created_at"] = utcnow()
    return monthly[
        [
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "total_applications",
            "cumulative_total_applications",
            "applications_with_resume",
            "cumulative_applications_with_resume",
            "applications_with_questionnaire",
            "cumulative_applications_with_questionnaire",
            "applications_with_resume_pct",
            "applications_with_questionnaire_pct",
            "job_family_count",
            "applications_mom_delta",
            "applications_mom_pct",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
            "snapshot_created_at",
        ]
    ]


def build_presence_snapshot(conn: duckdb.DuckDBPyConnection, run_id: str) -> pd.DataFrame:
    events = conn.execute(
        """
        select
            event_year_month as snapshot_year_month,
            sum(total_events) as monthly_events,
            sum(events_with_url) as monthly_events_with_url
        from mart_events_summary
        group by event_year_month
        """
    ).fetchdf()
    invitations = conn.execute(
        """
        select
            invitation_year_month as snapshot_year_month,
            sum(total_invitations) as monthly_invitations,
            sum(invitations_with_message) as monthly_invitations_with_message
        from mart_invitations_summary
        group by invitation_year_month
        """
    ).fetchdf()
    recommendations = conn.execute(
        """
        select
            recommendation_year_month as snapshot_year_month,
            total_recommendations as monthly_recommendations,
            avg_text_length as average_recommendation_text_length,
            mentions_data_count
        from mart_recommendations_received_timeline
        """
    ).fetchdf()

    month_spine = sorted(
        set(events.get("snapshot_year_month", pd.Series(dtype=str)).tolist())
        | set(invitations.get("snapshot_year_month", pd.Series(dtype=str)).tolist())
        | set(recommendations.get("snapshot_year_month", pd.Series(dtype=str)).tolist())
    )
    monthly = pd.DataFrame({"snapshot_year_month": month_spine})
    for frame in (events, invitations, recommendations):
        monthly = monthly.merge(frame, how="left", on="snapshot_year_month")

    numeric_columns = [
        "monthly_events",
        "monthly_events_with_url",
        "monthly_invitations",
        "monthly_invitations_with_message",
        "monthly_recommendations",
        "average_recommendation_text_length",
        "mentions_data_count",
    ]
    for column in numeric_columns:
        if column in monthly.columns:
            monthly[column] = monthly[column].fillna(0)

    monthly["snapshot_date"] = month_end_from_string(monthly["snapshot_year_month"])
    monthly["cumulative_events"] = monthly["monthly_events"].cumsum()
    monthly["cumulative_invitations"] = monthly["monthly_invitations"].cumsum()
    monthly["cumulative_recommendations"] = monthly["monthly_recommendations"].cumsum()
    monthly["events_with_url_pct"] = safe_pct(monthly["monthly_events_with_url"], monthly["monthly_events"])
    monthly["invitations_with_message_pct"] = safe_pct(
        monthly["monthly_invitations_with_message"], monthly["monthly_invitations"]
    )
    monthly["recommendations_mention_data_pct"] = safe_pct(
        monthly["mentions_data_count"], monthly["monthly_recommendations"]
    )
    monthly["presence_score"] = (
        monthly["events_with_url_pct"] * 0.35
        + monthly["invitations_with_message_pct"] * 0.35
        + monthly["recommendations_mention_data_pct"] * 0.30
    ).clip(0, 1)
    monthly["engagement_score"] = (
        safe_pct(monthly["monthly_recommendations"], monthly["cumulative_recommendations"].replace(0, pd.NA)).fillna(0) * 0.25
        + monthly["invitations_with_message_pct"] * 0.35
        + safe_pct(monthly["monthly_invitations"], monthly["cumulative_invitations"].replace(0, pd.NA)).fillna(0) * 0.20
        + safe_pct(monthly["monthly_events"], monthly["cumulative_events"].replace(0, pd.NA)).fillna(0) * 0.20
    ).clip(0, 1)
    monthly["snapshot_run_id"] = run_id
    monthly["is_simulated_snapshot"] = False
    monthly["snapshot_method"] = CUMULATIVE_FROM_EVENT_HISTORY
    monthly["snapshot_source"] = (
        "mart_events_summary|mart_invitations_summary|mart_recommendations_received_timeline"
    )
    monthly["snapshot_created_at"] = utcnow()
    return monthly[
        [
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "monthly_events",
            "monthly_events_with_url",
            "events_with_url_pct",
            "monthly_invitations",
            "monthly_invitations_with_message",
            "invitations_with_message_pct",
            "monthly_recommendations",
            "average_recommendation_text_length",
            "mentions_data_count",
            "recommendations_mention_data_pct",
            "cumulative_events",
            "cumulative_invitations",
            "cumulative_recommendations",
            "presence_score",
            "engagement_score",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
            "snapshot_created_at",
        ]
    ]


def build_career_education_snapshot(conn: duckdb.DuckDBPyConnection, run_id: str) -> pd.DataFrame:
    career = conn.execute(
        """
        select
            start_year_month as snapshot_year_month,
            total_positions_started as positions_started_in_month,
            current_positions_started as current_positions_started_in_month,
            avg_duration_months as avg_position_duration_months
        from mart_career_progression
        """
    ).fetchdf()
    education = conn.execute(
        """
        select
            start_year_month as snapshot_year_month,
            total_education_started as education_started_in_month,
            current_education_started as current_education_started_in_month,
            avg_education_duration_months
        from mart_education_summary
        """
    ).fetchdf()
    certifications = conn.execute(
        """
        select
            start_year_month as snapshot_year_month,
            total_certifications as certifications_started_in_month,
            avg_duration_months as avg_certification_duration_months
        from mart_certifications_summary
        """
    ).fetchdf()

    month_spine = sorted(
        set(career.get("snapshot_year_month", pd.Series(dtype=str)).tolist())
        | set(education.get("snapshot_year_month", pd.Series(dtype=str)).tolist())
        | set(certifications.get("snapshot_year_month", pd.Series(dtype=str)).tolist())
    )
    monthly = pd.DataFrame({"snapshot_year_month": month_spine})
    for frame in (career, education, certifications):
        monthly = monthly.merge(frame, how="left", on="snapshot_year_month")

    fill_zero_columns = [
        "positions_started_in_month",
        "current_positions_started_in_month",
        "education_started_in_month",
        "current_education_started_in_month",
        "certifications_started_in_month",
    ]
    for column in fill_zero_columns:
        monthly[column] = monthly[column].fillna(0)

    avg_columns = [
        "avg_position_duration_months",
        "avg_education_duration_months",
        "avg_certification_duration_months",
    ]
    for column in avg_columns:
        monthly[column] = monthly[column].fillna(0.0)

    monthly["snapshot_date"] = month_end_from_string(monthly["snapshot_year_month"])
    monthly["positions_started_cumulative"] = monthly["positions_started_in_month"].cumsum()
    monthly["current_positions_cumulative"] = monthly["current_positions_started_in_month"].cumsum()
    monthly["education_started_cumulative"] = monthly["education_started_in_month"].cumsum()
    monthly["current_education_cumulative"] = monthly["current_education_started_in_month"].cumsum()
    monthly["certifications_cumulative"] = monthly["certifications_started_in_month"].cumsum()

    monthly["position_duration_weighted_sum"] = (
        monthly["avg_position_duration_months"] * monthly["positions_started_in_month"]
    ).cumsum()
    monthly["education_duration_weighted_sum"] = (
        monthly["avg_education_duration_months"] * monthly["education_started_in_month"]
    ).cumsum()
    monthly["certification_duration_weighted_sum"] = (
        monthly["avg_certification_duration_months"] * monthly["certifications_started_in_month"]
    ).cumsum()

    monthly["avg_position_duration_cumulative"] = safe_pct(
        monthly["position_duration_weighted_sum"], monthly["positions_started_cumulative"]
    )
    monthly["avg_education_duration_cumulative"] = safe_pct(
        monthly["education_duration_weighted_sum"], monthly["education_started_cumulative"]
    )
    monthly["avg_certification_duration_cumulative"] = safe_pct(
        monthly["certification_duration_weighted_sum"], monthly["certifications_cumulative"]
    )

    monthly["career_maturity_score"] = (
        (monthly["positions_started_cumulative"] / 8).clip(0, 1) * 0.35
        + (monthly["education_started_cumulative"] / 4).clip(0, 1) * 0.20
        + (monthly["certifications_cumulative"] / 10).clip(0, 1) * 0.20
        + (monthly["avg_position_duration_cumulative"] / 24).clip(0, 1) * 0.15
        + (monthly["avg_education_duration_cumulative"] / 36).clip(0, 1) * 0.10
    ).clip(0, 1)
    monthly["snapshot_run_id"] = run_id
    monthly["is_simulated_snapshot"] = False
    monthly["snapshot_method"] = CUMULATIVE_FROM_EVENT_HISTORY
    monthly["snapshot_source"] = "mart_career_progression|mart_education_summary|mart_certifications_summary"
    monthly["snapshot_created_at"] = utcnow()
    return monthly[
        [
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "positions_started_in_month",
            "positions_started_cumulative",
            "current_positions_started_in_month",
            "current_positions_cumulative",
            "avg_position_duration_months",
            "avg_position_duration_cumulative",
            "education_started_in_month",
            "education_started_cumulative",
            "current_education_started_in_month",
            "current_education_cumulative",
            "avg_education_duration_months",
            "avg_education_duration_cumulative",
            "certifications_started_in_month",
            "certifications_cumulative",
            "avg_certification_duration_months",
            "avg_certification_duration_cumulative",
            "career_maturity_score",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
            "snapshot_created_at",
        ]
    ]


def build_data_quality_snapshot(conn: duckdb.DuckDBPyConnection, run_id: str) -> pd.DataFrame:
    existing = list_existing_tables(conn)
    if {
        "main.mart_ingestion_audit_health_timeline",
        "main.mart_ingestion_audit_null_rate_timeline",
    }.issubset(existing):
        health = conn.execute(
            """
            select
                loaded_on as snapshot_date,
                loaded_year_month as snapshot_year_month,
                table_key,
                export_type,
                source_row_count,
                row_count_after_transform,
                rows_removed_during_transform,
                duplicate_rows_after_transform,
                row_retention_rate,
                health_status,
                duplicate_alert_flag,
                row_removal_alert_flag,
                successful_load_flag
            from mart_ingestion_audit_health_timeline
            """
        ).fetchdf()
        nulls = conn.execute(
            """
            select
                loaded_on as snapshot_date,
                loaded_year_month as snapshot_year_month,
                table_key,
                count(distinct monitored_column) as monitored_columns_count,
                avg(null_rate_before_transform) as avg_null_rate_before_transform,
                avg(null_rate_after_transform) as avg_null_rate_after_transform,
                avg(null_rate_delta) as avg_null_rate_delta,
                sum(case when null_rate_alert_flag then 1 else 0 end) as null_rate_alert_count
            from mart_ingestion_audit_null_rate_timeline
            group by loaded_on, loaded_year_month, table_key
            """
        ).fetchdf()
        merged = health.merge(
            nulls,
            how="left",
            on=["snapshot_date", "snapshot_year_month", "table_key"],
        )
        merged["snapshot_source"] = "mart_ingestion_audit_health_timeline|mart_ingestion_audit_null_rate_timeline"
    elif "bronze.ingestion_audit" in existing:
        merged = conn.execute(
            """
            select
                cast(loaded_at_utc as date) as snapshot_date,
                strftime(cast(loaded_at_utc as date), '%Y-%m') as snapshot_year_month,
                table_key,
                export_type,
                cast(null as bigint) as source_row_count,
                row_count as row_count_after_transform,
                cast(null as bigint) as rows_removed_during_transform,
                duplicate_rows_after_transform,
                cast(null as double) as row_retention_rate,
                case
                    when duplicate_rows_after_transform > 0 then 'atencao'
                    else 'saudavel'
                end as health_status,
                duplicate_rows_after_transform > 0 as duplicate_alert_flag,
                false as row_removal_alert_flag,
                1 as successful_load_flag,
                cast(null as bigint) as monitored_columns_count,
                cast(null as double) as avg_null_rate_before_transform,
                cast(null as double) as avg_null_rate_after_transform,
                cast(null as double) as avg_null_rate_delta,
                cast(null as bigint) as null_rate_alert_count
            from bronze.ingestion_audit
            """
        ).fetchdf()
        merged["snapshot_source"] = "bronze.ingestion_audit"
    else:
        raise RuntimeError(
            "No actual pipeline audit source is available for fact_snapshot_data_quality. "
            "Expected marts mart_ingestion_audit_* or bronze.ingestion_audit."
        )

    merged["snapshot_date"] = pd.to_datetime(merged["snapshot_date"])
    merged["snapshot_run_id"] = run_id
    merged["is_simulated_snapshot"] = False
    merged["snapshot_method"] = ACTUAL_PIPELINE_AUDIT
    merged["snapshot_created_at"] = utcnow()
    return merged[
        [
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "table_key",
            "export_type",
            "source_row_count",
            "row_count_after_transform",
            "rows_removed_during_transform",
            "duplicate_rows_after_transform",
            "row_retention_rate",
            "health_status",
            "duplicate_alert_flag",
            "row_removal_alert_flag",
            "successful_load_flag",
            "monitored_columns_count",
            "avg_null_rate_before_transform",
            "avg_null_rate_after_transform",
            "avg_null_rate_delta",
            "null_rate_alert_count",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
            "snapshot_created_at",
        ]
    ]


def validate_contract(dataframe: pd.DataFrame, contract: SnapshotTableContract) -> list[str]:
    errors: list[str] = []
    missing_columns = [column for column in contract.required_columns if column not in dataframe.columns]
    if missing_columns:
        errors.append(f"{contract.table_name}: missing required columns {missing_columns}")
        return errors

    if dataframe.empty and contract.table_name.startswith("fact_"):
        errors.append(f"{contract.table_name}: no rows were generated")

    if "snapshot_date" in dataframe.columns and dataframe["snapshot_date"].isna().any():
        errors.append(f"{contract.table_name}: snapshot_date contains nulls")

    if "is_simulated_snapshot" in dataframe.columns:
        simulated_values = dataframe["is_simulated_snapshot"].dropna().unique().tolist()
        if any(bool(value) for value in simulated_values):
            errors.append(f"{contract.table_name}: found simulated rows during phase 1")

    if "snapshot_method" in dataframe.columns and dataframe["snapshot_method"].isna().any():
        errors.append(f"{contract.table_name}: snapshot_method contains nulls")

    for column in contract.non_negative_columns:
        if column in dataframe.columns and (dataframe[column].fillna(0) < 0).any():
            errors.append(f"{contract.table_name}: negative values found in {column}")

    for column in contract.percentage_columns:
        if column in dataframe.columns:
            series = dataframe[column].dropna()
            if not series.empty and ((series < 0).any() or (series > 1).any()):
                errors.append(f"{contract.table_name}: percentage column out of range in {column}")

    return errors


def build_all_snapshots(conn: duckdb.DuckDBPyConnection, run_id: str) -> dict[str, pd.DataFrame]:
    return {
        "fact_snapshot_network_growth": build_network_growth_snapshot(conn, run_id),
        "fact_snapshot_applications": build_applications_snapshot(conn, run_id),
        "fact_snapshot_presence": build_presence_snapshot(conn, run_id),
        "fact_snapshot_career_education": build_career_education_snapshot(conn, run_id),
        "fact_snapshot_data_quality": build_data_quality_snapshot(conn, run_id),
    }


def collect_summary(snapshot_frames: dict[str, pd.DataFrame]) -> dict[str, dict[str, object]]:
    summary: dict[str, dict[str, object]] = {}
    for table_name, dataframe in snapshot_frames.items():
        summary[table_name] = {
            "row_count": int(len(dataframe)),
            "snapshot_method": sorted(dataframe["snapshot_method"].dropna().unique().tolist()),
        }
    return summary


def main() -> None:
    configure_logging()
    settings = get_settings()
    started_at = utcnow()
    run_id = str(uuid.uuid4())
    LOGGER.info("Building phase 1 historical snapshots with run_id=%s", run_id)

    required_sources = [
        "main.mart_connections_summary",
        "main.mart_job_applications_summary",
        "main.mart_events_summary",
        "main.mart_invitations_summary",
        "main.mart_recommendations_received_timeline",
        "main.mart_career_progression",
        "main.mart_education_summary",
        "main.mart_certifications_summary",
    ]

    conn = connect_duckdb(settings=settings)
    try:
        assert_required_sources(conn, required_sources)

        snapshot_frames = build_all_snapshots(conn, run_id)
        method_dim = build_dim_snapshot_method()
        finished_at = utcnow()
        run_dim = build_dim_snapshot_run(
            run_id=run_id,
            settings_path=settings.db_path,
            started_at=started_at,
            finished_at=finished_at,
            project_root=settings.project_root,
        )

        frames_for_validation = {
            **snapshot_frames,
            "dim_snapshot_method": method_dim,
            "dim_snapshot_run": run_dim,
        }

        errors: list[str] = []
        for contract in SNAPSHOT_TABLE_CONTRACTS:
            dataframe = frames_for_validation.get(contract.table_name)
            if dataframe is None:
                errors.append(f"{contract.table_name}: dataframe was not built")
                continue
            errors.extend(validate_contract(dataframe, contract))

        if errors:
            raise RuntimeError("Snapshot validation failed:\n- " + "\n- ".join(errors))

        write_main_table(conn, "fact_snapshot_network_growth", snapshot_frames["fact_snapshot_network_growth"])
        write_main_table(conn, "fact_snapshot_applications", snapshot_frames["fact_snapshot_applications"])
        write_main_table(conn, "fact_snapshot_presence", snapshot_frames["fact_snapshot_presence"])
        write_main_table(conn, "fact_snapshot_career_education", snapshot_frames["fact_snapshot_career_education"])
        write_main_table(conn, "fact_snapshot_data_quality", snapshot_frames["fact_snapshot_data_quality"])
        write_main_table(conn, "dim_snapshot_method", method_dim)
        append_main_table(conn, "dim_snapshot_run", run_dim)

        persisted_counts = {
            table_name: conn.execute(f"select count(*) from {table_name}").fetchone()[0]
            for table_name in (
                "fact_snapshot_network_growth",
                "fact_snapshot_applications",
                "fact_snapshot_presence",
                "fact_snapshot_career_education",
                "fact_snapshot_data_quality",
                "dim_snapshot_method",
                "dim_snapshot_run",
            )
        }

        summary = {
            "snapshot_run_id": run_id,
            "phase_name": "phase_1_actual_only",
            "tables": collect_summary(snapshot_frames),
            "dimensions": {
                "dim_snapshot_method": int(len(method_dim)),
                "dim_snapshot_run_appended_rows": int(len(run_dim)),
            },
            "persisted_row_counts": persisted_counts,
            "contains_simulated_data": False,
            "warehouse_path": str(settings.db_path),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
