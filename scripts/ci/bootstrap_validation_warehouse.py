from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.config import get_settings
from linkedin_career_intelligence.duckdb_utils import write_dataframe_to_bronze


def build_validation_tables() -> dict[str, pd.DataFrame]:
    inventory_timestamp = pd.Timestamp("2026-04-30 12:00:00")

    return {
        "file_inventory": pd.DataFrame(
            [
                {
                    "export_name": "Basic LinkedIn Export",
                    "export_type": "basic",
                    "relative_path": "Profile.csv",
                    "file_name": "Profile.csv",
                    "file_stem": "Profile",
                    "folder_name": "root",
                    "file_size_bytes": 2048,
                    "file_size_kb": 2.0,
                    "row_count": 1,
                    "column_count": 13,
                    "column_names": '["first_name","last_name","headline"]',
                    "detected_encoding": "utf-8-sig",
                    "read_success": True,
                    "error_message": None,
                    "inventory_timestamp": inventory_timestamp,
                },
                {
                    "export_name": "Complete LinkedIn Export",
                    "export_type": "complete",
                    "relative_path": "Jobs/Job Applications.csv",
                    "file_name": "Job Applications.csv",
                    "file_stem": "Job Applications",
                    "folder_name": "Jobs",
                    "file_size_bytes": 4096,
                    "file_size_kb": 4.0,
                    "row_count": 2,
                    "column_count": 8,
                    "column_names": '["application_date","company_name","job_title"]',
                    "detected_encoding": "utf-8-sig",
                    "read_success": True,
                    "error_message": None,
                    "inventory_timestamp": inventory_timestamp,
                },
            ]
        ),
        "profile": pd.DataFrame(
            [
                {
                    "first_name": "Diego",
                    "last_name": "Pablo",
                    "maiden_name": "",
                    "address": "Sao Paulo",
                    "birth_date": pd.Timestamp("1990-01-01"),
                    "headline": "Analytics Engineer",
                    "summary": "Experienced analytics engineer focused on data platforms, governance and delivery.",
                    "industry": "Technology",
                    "zip_code": "00000-000",
                    "geo_location": "Sao Paulo",
                    "twitter_handles": "@diego",
                    "websites": "https://example.com",
                    "instant_messengers": "WhatsApp",
                }
            ]
        ),
        "connections": pd.DataFrame(
            [
                {
                    "first_name": "Ana",
                    "last_name": "Silva",
                    "url": "https://linkedin.com/in/ana-silva",
                    "email_address": "ana@example.com",
                    "company": "Contoso",
                    "position": "Data Analyst",
                    "connected_on": pd.Timestamp("2025-02-10"),
                },
                {
                    "first_name": "Bruno",
                    "last_name": "Costa",
                    "url": "https://linkedin.com/in/bruno-costa",
                    "email_address": pd.NA,
                    "company": "Fabrikam",
                    "position": "Data Engineer",
                    "connected_on": pd.Timestamp("2025-03-15"),
                },
            ]
        ),
        "positions": pd.DataFrame(
            [
                {
                    "company_name": "Contoso",
                    "title": "Data Analyst",
                    "description": "Built reporting pipelines.",
                    "location": "Remote",
                    "started_on": pd.Timestamp("2022-01-01"),
                    "finished_on": pd.Timestamp("2023-06-01"),
                    "is_current": False,
                },
                {
                    "company_name": "Fabrikam",
                    "title": "Analytics Engineer",
                    "description": "Own analytics engineering workflows.",
                    "location": "Sao Paulo",
                    "started_on": pd.Timestamp("2023-07-01"),
                    "finished_on": pd.NaT,
                    "is_current": True,
                },
            ]
        ),
        "education": pd.DataFrame(
            [
                {
                    "school_name": "UFSC",
                    "degree_name": "Bacharelado em Sistemas de Informacao",
                    "notes": "Strong systems foundation.",
                    "started_on": pd.Timestamp("2015-02-01"),
                    "finished_on": pd.Timestamp("2019-12-01"),
                    "is_current_education": False,
                },
                {
                    "school_name": "FIAP",
                    "degree_name": "MBA em Data Engineering",
                    "notes": "Advanced data stack.",
                    "started_on": pd.Timestamp("2024-01-01"),
                    "finished_on": pd.NaT,
                    "is_current_education": True,
                },
            ]
        ),
        "certifications": pd.DataFrame(
            [
                {
                    "name": "Azure Data Engineer Associate",
                    "url": "https://example.com/cert/azure",
                    "authority": "Microsoft",
                    "started_on": pd.Timestamp("2024-02-01"),
                    "finished_on": pd.Timestamp("2024-04-01"),
                    "license_number": "AZ-001",
                },
                {
                    "name": "AWS Data Analytics Specialty",
                    "url": "https://example.com/cert/aws",
                    "authority": "AWS",
                    "started_on": pd.Timestamp("2024-05-01"),
                    "finished_on": pd.Timestamp("2024-07-01"),
                    "license_number": "AWS-002",
                },
            ]
        ),
        "languages": pd.DataFrame(
            [
                {"name": "Portuguese", "proficiency": "Native or bilingual proficiency"},
                {"name": "English", "proficiency": "Full professional proficiency"},
            ]
        ),
        "endorsement_received_info": pd.DataFrame(
            [
                {
                    "endorsement_date": pd.Timestamp("2025-01-10"),
                    "skill_name": "Python",
                    "endorser_first_name": "Ana",
                    "endorser_last_name": "Silva",
                    "endorser_public_url": "https://linkedin.com/in/ana-silva",
                    "endorsement_status": "Accepted",
                },
                {
                    "endorsement_date": pd.Timestamp("2025-02-10"),
                    "skill_name": "SQL",
                    "endorser_first_name": "Bruno",
                    "endorser_last_name": "Costa",
                    "endorser_public_url": "https://linkedin.com/in/bruno-costa",
                    "endorsement_status": "Pending",
                },
            ]
        ),
        "company_follows": pd.DataFrame(
            [
                {"organization": "Microsoft", "followed_on": pd.Timestamp("2024-02-01")},
                {"organization": "AWS", "followed_on": pd.Timestamp("2024-03-01")},
            ]
        ),
        "recommendations_received": pd.DataFrame(
            [
                {
                    "first_name": "Ana",
                    "last_name": "Silva",
                    "company": "Contoso",
                    "position": "Manager",
                    "recommendation_text": "Strong delivery and communication.",
                    "recommendation_date": pd.Timestamp("2025-01-05 09:00:00"),
                    "visibility": "VISIBLE",
                },
                {
                    "first_name": "Bruno",
                    "last_name": "Costa",
                    "company": "Fabrikam",
                    "position": "Lead Engineer",
                    "recommendation_text": "Excellent data engineering partnership.",
                    "recommendation_date": pd.Timestamp("2025-03-12 15:30:00"),
                    "visibility": "VISIBLE",
                },
            ]
        ),
        "skills": pd.DataFrame(
            [
                {"skill_name": "Python"},
                {"skill_name": "SQL"},
                {"skill_name": "Azure"},
            ]
        ),
        "phone_numbers": pd.DataFrame(
            [{"extension": "", "phone_number": "+55 11 99999-9999", "phone_type": "mobile"}]
        ),
        "email_addresses": pd.DataFrame(
            [
                {
                    "email_address": "diego@example.com",
                    "confirmed": True,
                    "primary": True,
                    "updated_on": "2026-03-01",
                },
                {
                    "email_address": "diego.work@example.com",
                    "confirmed": True,
                    "primary": False,
                    "updated_on": "2026-03-15",
                },
            ]
        ),
        "registration": pd.DataFrame(
            [
                {
                    "registered_at": pd.Timestamp("2018-01-10 10:00:00"),
                    "registration_ip": "127.0.0.1",
                    "subscription_types": "premium",
                }
            ]
        ),
        "saved_job_alerts": pd.DataFrame(
            [
                {
                    "alert_parameters": "data engineer",
                    "query_context": "remote jobs",
                    "saved_search_id": "alert-001",
                }
            ]
        ),
        "volunteering": pd.DataFrame(
            [
                {
                    "company_name": "Open Data Community",
                    "role": "Mentor",
                    "cause": "Education",
                    "started_on": pd.Timestamp("2024-01-01"),
                    "finished_on": pd.NaT,
                    "description": "Mentoring junior professionals.",
                    "is_current_volunteering": True,
                }
            ]
        ),
        "invitations": pd.DataFrame(
            [
                {
                    "sender_name": "Diego Pablo",
                    "recipient_name": "Ana Silva",
                    "sent_at": pd.Timestamp("2025-03-01 08:00:00"),
                    "message": "Let us connect.",
                    "direction": "SENT",
                    "inviter_profile_url": "https://linkedin.com/in/diego-pablo",
                    "invitee_profile_url": "https://linkedin.com/in/ana-silva",
                },
                {
                    "sender_name": "Bruno Costa",
                    "recipient_name": "Diego Pablo",
                    "sent_at": pd.Timestamp("2025-03-20 11:00:00"),
                    "message": "Nice to meet you.",
                    "direction": "RECEIVED",
                    "inviter_profile_url": "https://linkedin.com/in/bruno-costa",
                    "invitee_profile_url": "https://linkedin.com/in/diego-pablo",
                },
            ]
        ),
        "events": pd.DataFrame(
            [
                {
                    "event_name": "Data Summit",
                    "event_time": "Apr 10, 2025 08:00 AM - Apr 10, 2025 05:00 PM",
                    "status": "Attended",
                    "external_url": "https://example.com/events/data-summit",
                    "started_at": pd.Timestamp("2025-04-10 08:00:00"),
                    "finished_at": pd.Timestamp("2025-04-10 17:00:00"),
                }
            ]
        ),
        "learning": pd.DataFrame(
            [
                {
                    "content_title": "Modern Data Modeling",
                    "content_description": "Dimensional modeling and marts.",
                    "content_type": "course",
                    "last_watched_at": pd.Timestamp("2025-02-01"),
                    "completed_at": pd.Timestamp("2025-02-05"),
                    "content_saved": True,
                    "notes_taken": "Great notes on marts.",
                },
                {
                    "content_title": "Python for Analytics",
                    "content_description": "Python patterns for data work.",
                    "content_type": "video",
                    "last_watched_at": pd.Timestamp("2025-03-01"),
                    "completed_at": pd.NaT,
                    "content_saved": False,
                    "notes_taken": pd.NA,
                },
            ]
        ),
        "job_applications": pd.DataFrame(
            [
                {
                    "application_date": pd.Timestamp("2025-03-12 09:00:00"),
                    "contact_email": "recruiter@example.com",
                    "contact_phone_number": "+1 555 111 2222",
                    "company_name": "Northwind",
                    "job_title": "Senior Data Engineer",
                    "job_url": "https://example.com/jobs/1",
                    "resume_name": "resume_v1.pdf",
                    "question_and_answers": "Q: SQL? A: Yes.",
                },
                {
                    "application_date": pd.Timestamp("2025-03-20 14:00:00"),
                    "contact_email": "talent@example.com",
                    "contact_phone_number": "+1 555 333 4444",
                    "company_name": "Adventure Works",
                    "job_title": "Analytics Engineer",
                    "job_url": "https://example.com/jobs/2",
                    "resume_name": "resume_v2.pdf",
                    "question_and_answers": "Q: dbt? A: Daily.",
                },
            ]
        ),
        "ingestion_audit": pd.DataFrame(
            [
                {
                    "table_key": "profile",
                    "bronze_table": "profile",
                    "source_file": "Profile.csv",
                    "source_path": "synthetic/Profile.csv",
                    "export_type": "basic",
                    "row_count": 1,
                    "column_count": 13,
                    "source_row_count": 1,
                    "source_column_count": 13,
                    "row_count_after_transform": 1,
                    "column_count_after_transform": 13,
                    "rows_removed_during_transform": 0,
                    "duplicate_rows_after_transform": 0,
                    "contract_owner": "LinkedIn member",
                    "contract_description": "Synthetic validation dataset.",
                    "required_columns": "first_name, last_name, headline",
                    "non_empty_columns": "first_name, last_name, headline",
                    "unique_columns": "",
                    "null_rate_by_column": '{"first_name": 0.0, "headline": 0.0, "last_name": 0.0}',
                    "source_null_rate_by_column": '{"First Name": 0.0, "Headline": 0.0, "Last Name": 0.0}',
                    "sensitive_columns": "first_name, last_name",
                    "loaded_at_utc": pd.Timestamp("2026-04-30 12:00:00", tz="UTC"),
                }
            ]
        ),
    }


def main() -> None:
    settings = get_settings()
    tables = build_validation_tables()

    for table_name, df in tables.items():
        write_dataframe_to_bronze(df, table_name, settings=settings)
        print(f"Wrote bronze.{table_name} with {len(df)} rows to {settings.db_path}")


if __name__ == "__main__":
    main()
