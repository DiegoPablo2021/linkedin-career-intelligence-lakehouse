from __future__ import annotations

from dataclasses import dataclass


ACTUAL_MONTHLY_AGGREGATE = "actual_monthly_aggregate"
CUMULATIVE_FROM_EVENT_HISTORY = "cumulative_from_event_history"
ACTUAL_PIPELINE_AUDIT = "actual_pipeline_audit"


@dataclass(frozen=True)
class SnapshotMethodDefinition:
    snapshot_method: str
    method_group: str
    method_description: str
    is_simulated: bool


@dataclass(frozen=True)
class SnapshotTableContract:
    table_name: str
    required_columns: tuple[str, ...]
    non_negative_columns: tuple[str, ...] = ()
    percentage_columns: tuple[str, ...] = ()


SNAPSHOT_METHOD_DEFINITIONS = (
    SnapshotMethodDefinition(
        snapshot_method=ACTUAL_MONTHLY_AGGREGATE,
        method_group="actual",
        method_description="Monthly aggregate built directly from real monthly marts generated from real event dates.",
        is_simulated=False,
    ),
    SnapshotMethodDefinition(
        snapshot_method=CUMULATIVE_FROM_EVENT_HISTORY,
        method_group="derived_actual",
        method_description="Cumulative series built from real dated events or real monthly aggregates, without simulated backfill.",
        is_simulated=False,
    ),
    SnapshotMethodDefinition(
        snapshot_method=ACTUAL_PIPELINE_AUDIT,
        method_group="actual",
        method_description="Historical audit series built directly from real pipeline audit and observability records.",
        is_simulated=False,
    ),
)


SNAPSHOT_TABLE_CONTRACTS = (
    SnapshotTableContract(
        table_name="fact_snapshot_network_growth",
        required_columns=(
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "total_connections_cumulative",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
        ),
        non_negative_columns=(
            "monthly_new_connections",
            "total_connections_cumulative",
            "monthly_new_connections_with_email",
            "connections_with_email_cumulative",
            "unique_companies_in_month",
            "unique_positions_in_month",
            "connections_mom_delta",
        ),
        percentage_columns=("connections_with_email_pct", "connections_mom_pct"),
    ),
    SnapshotTableContract(
        table_name="fact_snapshot_applications",
        required_columns=(
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "total_applications",
            "cumulative_total_applications",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
        ),
        non_negative_columns=(
            "total_applications",
            "cumulative_total_applications",
            "applications_with_resume",
            "applications_with_questionnaire",
            "job_family_count",
            "applications_mom_delta",
        ),
        percentage_columns=(
            "applications_with_resume_pct",
            "applications_with_questionnaire_pct",
            "applications_mom_pct",
        ),
    ),
    SnapshotTableContract(
        table_name="fact_snapshot_presence",
        required_columns=(
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
        ),
        non_negative_columns=(
            "monthly_events",
            "monthly_invitations",
            "monthly_recommendations",
            "cumulative_events",
            "cumulative_invitations",
            "cumulative_recommendations",
        ),
        percentage_columns=(
            "events_with_url_pct",
            "invitations_with_message_pct",
            "recommendations_mention_data_pct",
            "presence_score",
            "engagement_score",
        ),
    ),
    SnapshotTableContract(
        table_name="fact_snapshot_career_education",
        required_columns=(
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
        ),
        non_negative_columns=(
            "positions_started_in_month",
            "positions_started_cumulative",
            "current_positions_cumulative",
            "education_started_in_month",
            "education_started_cumulative",
            "current_education_cumulative",
            "certifications_started_in_month",
            "certifications_cumulative",
        ),
        percentage_columns=("career_maturity_score",),
    ),
    SnapshotTableContract(
        table_name="fact_snapshot_data_quality",
        required_columns=(
            "snapshot_run_id",
            "snapshot_date",
            "snapshot_year_month",
            "table_key",
            "is_simulated_snapshot",
            "snapshot_method",
            "snapshot_source",
        ),
        non_negative_columns=(
            "source_row_count",
            "row_count_after_transform",
            "rows_removed_during_transform",
            "duplicate_rows_after_transform",
            "monitored_columns_count",
            "null_rate_alert_count",
        ),
        percentage_columns=(
            "row_retention_rate",
            "avg_null_rate_before_transform",
            "avg_null_rate_after_transform",
        ),
    ),
    SnapshotTableContract(
        table_name="dim_snapshot_method",
        required_columns=(
            "snapshot_method",
            "method_group",
            "method_description",
            "is_simulated",
        ),
    ),
    SnapshotTableContract(
        table_name="dim_snapshot_run",
        required_columns=(
            "snapshot_run_id",
            "run_started_at",
            "run_finished_at",
            "phase_name",
            "contains_simulated_data",
            "source_warehouse_path",
        ),
    ),
)
