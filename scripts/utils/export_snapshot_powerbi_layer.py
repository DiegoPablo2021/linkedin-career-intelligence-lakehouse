from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import sys

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.duckdb_utils import connect_duckdb


@dataclass(frozen=True)
class ExportSpec:
    output_name: str
    source_table: str
    query: str
    description: str


EXPORT_SPECS: list[ExportSpec] = [
    ExportSpec(
        output_name="fact_snapshot_network_growth",
        source_table="main.fact_snapshot_network_growth",
        description="Snapshot histórico de crescimento da rede e cobertura de e-mail.",
        query="""
        select
            snapshot_run_id,
            cast(snapshot_date as date) as snapshot_date,
            snapshot_year_month,
            monthly_new_connections,
            total_connections_cumulative,
            monthly_new_connections_with_email,
            connections_with_email_cumulative,
            connections_with_email_pct,
            unique_companies_in_month,
            unique_positions_in_month,
            connections_mom_delta,
            connections_mom_pct,
            is_simulated_snapshot,
            snapshot_method,
            snapshot_source,
            snapshot_created_at
        from main.fact_snapshot_network_growth
        order by snapshot_date
        """,
    ),
    ExportSpec(
        output_name="fact_snapshot_applications",
        source_table="main.fact_snapshot_applications",
        description="Snapshot histórico de aplicações e indicadores de candidatura.",
        query="""
        select
            snapshot_run_id,
            cast(snapshot_date as date) as snapshot_date,
            snapshot_year_month,
            total_applications,
            cumulative_total_applications,
            applications_with_resume,
            cumulative_applications_with_resume,
            applications_with_questionnaire,
            cumulative_applications_with_questionnaire,
            applications_with_resume_pct,
            applications_with_questionnaire_pct,
            job_family_count,
            applications_mom_delta,
            applications_mom_pct,
            is_simulated_snapshot,
            snapshot_method,
            snapshot_source,
            snapshot_created_at
        from main.fact_snapshot_applications
        order by snapshot_date
        """,
    ),
    ExportSpec(
        output_name="fact_snapshot_presence",
        source_table="main.fact_snapshot_presence",
        description="Snapshot histórico de presença profissional, eventos, convites e recomendações.",
        query="""
        select
            snapshot_run_id,
            cast(snapshot_date as date) as snapshot_date,
            snapshot_year_month,
            monthly_events,
            monthly_events_with_url,
            events_with_url_pct,
            monthly_invitations,
            monthly_invitations_with_message,
            invitations_with_message_pct,
            monthly_recommendations,
            average_recommendation_text_length,
            mentions_data_count,
            recommendations_mention_data_pct,
            cumulative_events,
            cumulative_invitations,
            cumulative_recommendations,
            presence_score,
            engagement_score,
            is_simulated_snapshot,
            snapshot_method,
            snapshot_source,
            snapshot_created_at
        from main.fact_snapshot_presence
        order by snapshot_date
        """,
    ),
    ExportSpec(
        output_name="fact_snapshot_career_education",
        source_table="main.fact_snapshot_career_education",
        description="Snapshot histórico da evolução profissional, educação e certificações.",
        query="""
        select
            snapshot_run_id,
            cast(snapshot_date as date) as snapshot_date,
            snapshot_year_month,
            positions_started_in_month,
            positions_started_cumulative,
            current_positions_started_in_month,
            current_positions_cumulative,
            avg_position_duration_months,
            avg_position_duration_cumulative,
            education_started_in_month,
            education_started_cumulative,
            current_education_started_in_month,
            current_education_cumulative,
            avg_education_duration_months,
            avg_education_duration_cumulative,
            certifications_started_in_month,
            certifications_cumulative,
            avg_certification_duration_months,
            avg_certification_duration_cumulative,
            career_maturity_score,
            is_simulated_snapshot,
            snapshot_method,
            snapshot_source,
            snapshot_created_at
        from main.fact_snapshot_career_education
        order by snapshot_date
        """,
    ),
    ExportSpec(
        output_name="fact_snapshot_data_quality",
        source_table="main.fact_snapshot_data_quality",
        description="Snapshot histórico da camada de observability e data quality.",
        query="""
        select
            snapshot_run_id,
            cast(snapshot_date as date) as snapshot_date,
            snapshot_year_month,
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
            successful_load_flag,
            monitored_columns_count,
            avg_null_rate_before_transform,
            avg_null_rate_after_transform,
            avg_null_rate_delta,
            null_rate_alert_count,
            is_simulated_snapshot,
            snapshot_method,
            snapshot_source,
            snapshot_created_at
        from main.fact_snapshot_data_quality
        order by snapshot_date, table_key
        """,
    ),
    ExportSpec(
        output_name="dim_snapshot_method",
        source_table="main.dim_snapshot_method",
        description="Dimensão de métodos de geração de snapshot.",
        query="""
        select
            snapshot_method,
            method_group,
            method_description,
            is_simulated
        from main.dim_snapshot_method
        order by snapshot_method
        """,
    ),
    ExportSpec(
        output_name="dim_snapshot_run",
        source_table="main.dim_snapshot_run",
        description="Dimensão de execução e rastreabilidade da geração de snapshots.",
        query="""
        select
            snapshot_run_id,
            run_started_at,
            run_finished_at,
            phase_name,
            contains_simulated_data,
            source_warehouse_path,
            project_git_commit,
            notes
        from main.dim_snapshot_run
        order by run_started_at
        """,
    ),
]


def export_specs(
    specs: Sequence[ExportSpec],
    output_dir: Path,
) -> dict[str, dict[str, object]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, dict[str, object]] = {}

    conn = connect_duckdb(read_only=True)
    try:
        for spec in specs:
            frame = conn.execute(spec.query).fetchdf()
            output_path = output_dir / f"{spec.output_name}.csv"
            frame.to_csv(output_path, index=False, encoding="utf-8")
            summary[spec.output_name] = {
                "source_table": spec.source_table,
                "row_count": int(len(frame)),
                "columns": list(frame.columns),
                "output_path": str(output_path),
                "description": spec.description,
            }
    finally:
        conn.close()

    return summary


def main() -> None:
    output_dir = PROJECT_ROOT / "powerbi" / "exports"
    summary = export_specs(EXPORT_SPECS, output_dir)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
