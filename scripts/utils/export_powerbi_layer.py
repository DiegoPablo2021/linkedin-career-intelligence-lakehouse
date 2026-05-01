from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
import sys

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
        output_name="dim_profile",
        source_table="main.mart_profile_summary",
        description="Identidade e contexto principal do perfil.",
        query="""
        select
            first_name,
            last_name,
            headline,
            industry,
            geo_location,
            profile_track,
            summary_length,
            summary_size_category,
            primary_contact_url,
            primary_contact_label,
            portfolio_website
        from main.mart_profile_summary
        """,
    ),
    ExportSpec(
        output_name="dim_language_proficiency",
        source_table="main.mart_languages_summary",
        description="Resumo de idiomas e proficiências para consumo semântico.",
        query="""
        select
            proficiency_track,
            total_languages,
            unique_languages
        from main.mart_languages_summary
        """,
    ),
    ExportSpec(
        output_name="fact_connections_timeline",
        source_table="main.mart_connections_summary",
        description="Série temporal de conexões e cobertura de e-mail.",
        query="""
        select
            connection_year,
            connection_month,
            connection_year_month,
            total_connections,
            connections_with_email,
            unique_companies,
            unique_positions
        from main.mart_connections_summary
        order by connection_year, connection_month
        """,
    ),
    ExportSpec(
        output_name="fact_career_progression",
        source_table="main.mart_career_progression",
        description="Evolução temporal da trajetória profissional.",
        query="""
        select
            start_year,
            start_month,
            start_year_month,
            total_positions_started,
            current_positions_started,
            unique_companies,
            unique_titles,
            avg_duration_months
        from main.mart_career_progression
        order by start_year, start_month
        """,
    ),
    ExportSpec(
        output_name="fact_education_timeline",
        source_table="main.mart_education_summary",
        description="Linha do tempo da formação acadêmica.",
        query="""
        select
            start_year,
            start_month,
            start_year_month,
            total_education_started,
            current_education_started,
            unique_schools,
            unique_degrees,
            avg_education_duration_months
        from main.mart_education_summary
        order by start_year, start_month
        """,
    ),
    ExportSpec(
        output_name="fact_certifications_timeline",
        source_table="main.mart_certifications_summary",
        description="Linha do tempo das certificações.",
        query="""
        select
            start_year,
            start_month,
            start_year_month,
            total_certifications,
            unique_certifications,
            unique_authorities,
            avg_duration_months
        from main.mart_certifications_summary
        order by start_year, start_month
        """,
    ),
    ExportSpec(
        output_name="fact_skills_summary",
        source_table="main.mart_skills_summary",
        description="Resumo por categoria de skills.",
        query="""
        select
            skill_category,
            total_skills,
            unique_skills,
            avg_skill_name_length
        from main.mart_skills_summary
        order by total_skills desc, skill_category
        """,
    ),
    ExportSpec(
        output_name="fact_learning_summary",
        source_table="main.mart_learning_summary",
        description="Resumo de consumo de conteúdo do LinkedIn Learning.",
        query="""
        select
            content_type_clean,
            total_contents,
            completed_contents,
            saved_contents,
            contents_with_notes
        from main.mart_learning_summary
        order by total_contents desc, content_type_clean
        """,
    ),
    ExportSpec(
        output_name="fact_job_applications_timeline",
        source_table="main.mart_job_applications_summary",
        description="Linha do tempo de candidaturas por família de vaga.",
        query="""
        select
            application_year,
            application_month,
            application_year_month,
            job_family,
            total_applications,
            unique_companies,
            applications_with_resume,
            applications_with_questionnaire
        from main.mart_job_applications_summary
        order by application_year, application_month, job_family
        """,
    ),
    ExportSpec(
        output_name="fact_recommendations_timeline",
        source_table="main.mart_recommendations_received_timeline",
        description="Linha do tempo das recomendações recebidas.",
        query="""
        select
            recommendation_year,
            recommendation_month,
            recommendation_year_month,
            total_recommendations,
            avg_text_length,
            mentions_data_count,
            mentions_teamwork_count
        from main.mart_recommendations_received_timeline
        order by recommendation_year, recommendation_month
        """,
    ),
    ExportSpec(
        output_name="fact_recommendations_summary",
        source_table="main.mart_recommendations_received_summary",
        description="Resumo geral de recomendações e sinais reputacionais.",
        query="""
        select
            total_recommendations,
            avg_text_length,
            mentions_data_count,
            mentions_teamwork_count,
            first_recommendation_date,
            last_recommendation_date
        from main.mart_recommendations_received_summary
        """,
    ),
    ExportSpec(
        output_name="fact_endorsements_summary",
        source_table="main.mart_endorsement_received_info_summary",
        description="Resumo de endorsements por skill.",
        query="""
        select
            skill_name_clean,
            endorsement_count,
            unique_endorsers,
            first_endorsement_date,
            last_endorsement_date
        from main.mart_endorsement_received_info_summary
        order by endorsement_count desc, skill_name_clean
        """,
    ),
    ExportSpec(
        output_name="fact_company_follows",
        source_table="main.mart_company_follows_summary",
        description="Resumo das organizações seguidas na plataforma.",
        query="""
        select
            organization_clean,
            follow_count,
            first_follow_date,
            last_follow_date
        from main.mart_company_follows_summary
        order by follow_count desc, organization_clean
        """,
    ),
    ExportSpec(
        output_name="fact_events_summary",
        source_table="main.mart_events_summary",
        description="Resumo temporal de eventos e status de participação.",
        query="""
        select
            event_year,
            event_month,
            event_year_month,
            status,
            total_events,
            events_with_url
        from main.mart_events_summary
        order by event_year, event_month, status
        """,
    ),
    ExportSpec(
        output_name="fact_invitations_summary",
        source_table="main.mart_invitations_summary",
        description="Resumo temporal de convites por direção.",
        query="""
        select
            invitation_year,
            invitation_month,
            invitation_year_month,
            direction,
            total_invitations,
            invitations_with_message
        from main.mart_invitations_summary
        order by invitation_year, invitation_month, direction
        """,
    ),
    ExportSpec(
        output_name="fact_saved_job_alerts",
        source_table="main.mart_saved_job_alerts_summary",
        description="Resumo dos alertas de vagas salvos.",
        query="""
        select
            alert_frequency,
            total_alerts,
            unique_keyword_sets,
            remote_alerts,
            company_scoped_alerts
        from main.mart_saved_job_alerts_summary
        order by total_alerts desc, alert_frequency
        """,
    ),
    ExportSpec(
        output_name="fact_file_inventory",
        source_table="main.mart_file_inventory_summary",
        description="Resumo do inventário técnico de arquivos.",
        query="""
        select
            export_type,
            categoria_dado,
            status_leitura,
            volume_categoria,
            total_arquivos,
            total_linhas,
            total_colunas,
            total_tamanho_kb,
            primeira_execucao_inventario,
            ultima_execucao_inventario
        from main.mart_file_inventory_summary
        order by export_type, categoria_dado, status_leitura
        """,
    ),
    ExportSpec(
        output_name="fact_pipeline_health",
        source_table="main.mart_pipeline_health_summary",
        description="Indicadores executivos de saúde e cobertura do pipeline.",
        query="""
        select
            total_inventory_files,
            successful_reads,
            failed_reads,
            latest_inventory_timestamp,
            total_connections,
            total_positions,
            total_education_records,
            total_certifications,
            total_languages,
            total_endorsements,
            total_company_follows,
            total_recommendations,
            total_skills,
            total_invitations,
            total_events,
            total_learning_records,
            total_job_applications,
            total_saved_job_alerts,
            total_volunteering,
            total_email_addresses,
            total_phone_numbers
        from main.mart_pipeline_health_summary
        """,
    ),
    ExportSpec(
        output_name="fact_contact_account",
        source_table="main.mart_contact_account_summary",
        description="Indicadores de contatos e metadados de conta.",
        query="""
        select
            total_email_addresses,
            confirmed_email_addresses,
            primary_email_addresses,
            total_phone_numbers,
            first_registered_at,
            latest_registered_at,
            subscription_types,
            account_age_years
        from main.mart_contact_account_summary
        """,
    ),
]


TEMPORAL_SPECS: list[tuple[str, str, str]] = [
    ("main.mart_connections_summary", "connection_year_month", "year_month"),
    ("main.mart_career_progression", "start_year_month", "year_month"),
    ("main.mart_education_summary", "start_year_month", "year_month"),
    ("main.mart_certifications_summary", "start_year_month", "year_month"),
    ("main.mart_job_applications_summary", "application_year_month", "year_month"),
    ("main.mart_recommendations_received_timeline", "recommendation_year_month", "year_month"),
    ("main.mart_events_summary", "event_year_month", "year_month"),
    ("main.mart_invitations_summary", "invitation_year_month", "year_month"),
    ("main.mart_company_follows_summary", "first_follow_date", "date"),
    ("main.mart_company_follows_summary", "last_follow_date", "date"),
    ("main.mart_endorsement_received_info_summary", "first_endorsement_date", "date"),
    ("main.mart_endorsement_received_info_summary", "last_endorsement_date", "date"),
    ("main.mart_recommendations_received_summary", "first_recommendation_date", "date"),
    ("main.mart_recommendations_received_summary", "last_recommendation_date", "date"),
    ("main.mart_file_inventory_summary", "primeira_execucao_inventario", "timestamp"),
    ("main.mart_file_inventory_summary", "ultima_execucao_inventario", "timestamp"),
    ("main.mart_pipeline_health_summary", "latest_inventory_timestamp", "timestamp"),
    ("main.mart_contact_account_summary", "first_registered_at", "timestamp"),
    ("main.mart_contact_account_summary", "latest_registered_at", "timestamp"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exporta uma camada estável para consumo do Power BI a partir dos marts do DuckDB."
    )
    parser.add_argument(
        "--output-dir",
        default="powerbi/exports",
        help="Diretório de saída dos arquivos exportados.",
    )
    parser.add_argument(
        "--format",
        choices=("csv", "parquet", "both"),
        default="both",
        help="Formato de saída dos arquivos exportados.",
    )
    return parser.parse_args()


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def table_exists(conn: object, qualified_name: str) -> bool:
    schema_name, table_name = qualified_name.split(".", 1)
    result = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ? and table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    return bool(result and result[0])


def validate_sources(conn: object) -> None:
    missing = sorted(
        spec.source_table for spec in EXPORT_SPECS if not table_exists(conn, spec.source_table)
    )
    if missing:
        missing_list = "\n".join(f"- {item}" for item in missing)
        raise RuntimeError(
            "As seguintes tabelas esperadas nao foram encontradas no DuckDB:\n"
            f"{missing_list}\n\n"
            "Execute a ingestao e o dbt build antes de exportar a camada do Power BI."
        )


def build_dim_date(conn: object) -> pd.DataFrame:
    dates: list[pd.Timestamp] = []

    for table_name, column_name, column_kind in TEMPORAL_SPECS:
        if not table_exists(conn, table_name):
            continue

        frame = conn.execute(f"select {column_name} as value from {table_name}").fetchdf()
        if frame.empty:
            continue

        series = frame["value"].dropna()
        if series.empty:
            continue

        if column_kind == "year_month":
            parsed = pd.to_datetime(series.astype("string") + "-01", errors="coerce")
        else:
            parsed = pd.to_datetime(series, errors="coerce")

        parsed = parsed.dropna()
        if not parsed.empty:
            dates.extend(parsed.tolist())

    if not dates:
        today = pd.Timestamp.today().normalize()
        start_date = today.replace(month=1, day=1)
        end_date = start_date.replace(year=start_date.year + 1) - pd.Timedelta(days=1)
    else:
        start_date = min(dates).normalize().replace(day=1)
        end_date = max(dates).normalize()
        end_date = (end_date + pd.offsets.MonthEnd(0)).normalize()

    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    dim_date = pd.DataFrame({"date": date_range})
    dim_date["date_key"] = dim_date["date"].dt.strftime("%Y%m%d").astype("int64")
    dim_date["year"] = dim_date["date"].dt.year.astype("int64")
    dim_date["quarter_number"] = dim_date["date"].dt.quarter.astype("int64")
    dim_date["month_number"] = dim_date["date"].dt.month.astype("int64")
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date["month_name_short"] = dim_date["date"].dt.strftime("%b")
    dim_date["year_month"] = dim_date["date"].dt.strftime("%Y-%m")
    dim_date["year_month_label"] = dim_date["date"].dt.strftime("%Y-%b")
    dim_date["semester_number"] = (((dim_date["month_number"] - 1) // 6) + 1).astype("int64")
    dim_date["semester_label"] = "S" + dim_date["semester_number"].astype("string")
    dim_date["bimester_number"] = (((dim_date["month_number"] - 1) // 2) + 1).astype("int64")
    dim_date["bimester_label"] = "B" + dim_date["bimester_number"].astype("string")
    dim_date["week_of_year"] = dim_date["date"].dt.isocalendar().week.astype("int64")
    dim_date["day"] = dim_date["date"].dt.day.astype("int64")
    dim_date["day_of_week_number"] = dim_date["date"].dt.weekday.add(1).astype("int64")
    dim_date["day_of_week_name"] = dim_date["date"].dt.day_name()
    dim_date["is_weekend"] = dim_date["day_of_week_number"].isin([6, 7])
    dim_date["quarter_label"] = "Q" + dim_date["quarter_number"].astype("string")
    dim_date["half_year_label"] = dim_date["year"].astype("string") + "-" + dim_date["semester_label"]
    dim_date["bimester_year_label"] = dim_date["year"].astype("string") + "-" + dim_date["bimester_label"]
    dim_date["year_label"] = dim_date["year"].astype("string")
    return dim_date


def export_frame(df: pd.DataFrame, output_dir: Path, output_name: str, export_format: str) -> list[str]:
    written_files: list[str] = []

    if export_format in {"csv", "both"}:
        csv_path = output_dir / f"{output_name}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        written_files.append(str(csv_path))

    if export_format in {"parquet", "both"}:
        parquet_path = output_dir / f"{output_name}.parquet"
        df.to_parquet(parquet_path, index=False)
        written_files.append(str(parquet_path))

    return written_files


def build_manifest(records: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(records).sort_values(by=["output_name"]).reset_index(drop=True)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    ensure_output_dir(output_dir)

    conn = connect_duckdb(read_only=True)
    validate_sources(conn)

    manifest_rows: list[dict[str, object]] = []

    dim_date = build_dim_date(conn)
    date_files = export_frame(dim_date, output_dir, "dim_date", args.format)
    manifest_rows.append(
        {
            "output_name": "dim_date",
            "source_table": "derived_from_mart_dates",
            "row_count": len(dim_date),
            "column_count": len(dim_date.columns),
            "description": "Calendario robusto para filtros, inteligencia temporal e relacionamentos.",
            "files": json.dumps(date_files, ensure_ascii=False),
        }
    )

    for spec in EXPORT_SPECS:
        df = conn.execute(spec.query).fetchdf()
        written_files = export_frame(df, output_dir, spec.output_name, args.format)
        manifest_rows.append(
            {
                "output_name": spec.output_name,
                "source_table": spec.source_table,
                "row_count": len(df),
                "column_count": len(df.columns),
                "description": spec.description,
                "files": json.dumps(written_files, ensure_ascii=False),
            }
        )
        print(
            f"[ok] {spec.output_name}: {len(df)} linhas, "
            f"{len(df.columns)} colunas -> {', '.join(Path(path).name for path in written_files)}"
        )

    manifest = build_manifest(manifest_rows)
    manifest_files = export_frame(manifest, output_dir, "_export_manifest", args.format)
    print(f"[ok] dim_date: {len(dim_date)} linhas, {len(dim_date.columns)} colunas")
    print(f"[ok] manifesto: {', '.join(Path(path).name for path in manifest_files)}")
    conn.close()


if __name__ == "__main__":
    main()
