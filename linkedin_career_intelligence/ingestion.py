from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from linkedin_career_intelligence.config import ProjectSettings, get_settings
from linkedin_career_intelligence.contracts import TableContract, assert_contract
from linkedin_career_intelligence.duckdb_utils import write_dataframe_to_bronze


TransformFn = Callable[[pd.DataFrame], pd.DataFrame]
ReadKwargs = dict[str, Any]


@dataclass(frozen=True)
class TableConfig:
    key: str
    bronze_table: str
    csv_name: str
    export_type: str
    transform: TransformFn
    contract: TableContract
    read_kwargs: ReadKwargs = field(default_factory=dict)


def read_csv_safe(csv_path: str | Path, **read_kwargs: Any) -> pd.DataFrame:
    options: ReadKwargs = {
        "encoding": "utf-8-sig",
        "engine": "python",
    }
    options.update(read_kwargs)
    return pd.read_csv(str(csv_path), **options)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    renamed.columns = (
        renamed.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
    )
    return renamed


def ensure_columns(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    ensured = df.copy()
    for col in required_columns:
        if col not in ensured.columns:
            ensured[col] = pd.NA
    return ensured


def clean_text_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    cleaned = df.copy()
    for col in columns:
        normalized = cleaned[col].astype("string").str.strip()
        cleaned[col] = normalized.mask(normalized.isin(["", "nan", "None"]), pd.NA)
    return cleaned


def parse_date_columns(df: pd.DataFrame, columns: list[str], fmt: str | None = None) -> pd.DataFrame:
    parsed = df.copy()
    for col in columns:
        parsed[col] = pd.to_datetime(parsed[col], format=fmt or "mixed", errors="coerce")
    return parsed


def parse_boolean_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    parsed = df.copy()
    truthy_values = {"yes", "true", "1", "y", "sim"}
    falsy_values = {"no", "false", "0", "n", "nao", "não"}

    for col in columns:
        normalized = parsed[col].astype("string").str.strip().str.lower()
        parsed[col] = normalized.map(
            lambda value: True
            if value in truthy_values
            else False
            if value in falsy_values
            else pd.NA
        )

    return parsed


def drop_blank_rows(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    filtered = df.copy()
    for col in required_columns:
        filtered = filtered[filtered[col].notna()]
        filtered = filtered[filtered[col].astype("string").str.strip() != ""]
    return filtered


def select_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    return df.loc[:, columns].copy()


def deduplicate_rows(df: pd.DataFrame, subset: list[str] | None = None) -> pd.DataFrame:
    return df.drop_duplicates(subset=subset).reset_index(drop=True)


def build_ingestion_audit_row(
    table: TableConfig,
    csv_path: Path,
    cleaned: pd.DataFrame,
    duplicate_rows: int,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "table_key": table.key,
                "bronze_table": table.bronze_table,
                "source_file": table.csv_name,
                "source_path": str(csv_path),
                "export_type": table.export_type,
                "row_count": len(cleaned),
                "column_count": len(cleaned.columns),
                "duplicate_rows_after_transform": duplicate_rows,
                "contract_owner": table.contract.owner,
                "contract_description": table.contract.description,
                "required_columns": ", ".join(table.contract.required_columns),
                "sensitive_columns": ", ".join(table.contract.sensitive_columns),
                "loaded_at_utc": datetime.now(timezone.utc),
            }
        ]
    )


def transform_connections(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(
        cleaned,
        [
            "first_name",
            "last_name",
            "url",
            "email_address",
            "company",
            "position",
            "connected_on",
        ],
    )
    cleaned = clean_text_columns(
        cleaned,
        ["first_name", "last_name", "url", "email_address", "company", "position"],
    )
    cleaned = parse_date_columns(cleaned, ["connected_on"])
    cleaned = select_columns(
        cleaned,
        [
            "first_name",
            "last_name",
            "url",
            "email_address",
            "company",
            "position",
            "connected_on",
        ],
    )
    return cleaned


def transform_positions(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(
        cleaned,
        ["company_name", "title", "description", "location", "started_on", "finished_on"],
    )
    cleaned = clean_text_columns(cleaned, ["company_name", "title", "description", "location"])
    cleaned = parse_date_columns(cleaned, ["started_on", "finished_on"])
    cleaned["is_current"] = cleaned["finished_on"].isna()
    cleaned = select_columns(
        cleaned,
        [
            "company_name",
            "title",
            "description",
            "location",
            "started_on",
            "finished_on",
            "is_current",
        ],
    )
    return cleaned


def transform_profile(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = [
        "first_name",
        "last_name",
        "maiden_name",
        "address",
        "birth_date",
        "headline",
        "summary",
        "industry",
        "zip_code",
        "geo_location",
        "twitter_handles",
        "websites",
        "instant_messengers",
    ]
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(cleaned, required_columns)
    cleaned = clean_text_columns(
        cleaned,
        [
            "first_name",
            "last_name",
            "maiden_name",
            "address",
            "headline",
            "summary",
            "industry",
            "zip_code",
            "geo_location",
            "twitter_handles",
            "websites",
            "instant_messengers",
        ],
    )
    cleaned = parse_date_columns(cleaned, ["birth_date"])
    return select_columns(cleaned, required_columns)


def transform_languages(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(cleaned, ["name", "proficiency"])
    cleaned = clean_text_columns(cleaned, ["name", "proficiency"])
    return select_columns(cleaned, ["name", "proficiency"])


def transform_education(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = cleaned.rename(
        columns={
            "school": "school_name",
            "degree": "degree_name",
            "description": "notes",
            "started": "started_on",
            "start_date": "started_on",
            "finished": "finished_on",
            "end_date": "finished_on",
        }
    )
    cleaned = ensure_columns(
        cleaned,
        ["school_name", "degree_name", "notes", "started_on", "finished_on"],
    )
    cleaned = clean_text_columns(cleaned, ["school_name", "degree_name", "notes"])
    cleaned = parse_date_columns(cleaned, ["started_on", "finished_on"])
    cleaned["is_current_education"] = cleaned["finished_on"].isna()
    return select_columns(
        cleaned,
        [
            "school_name",
            "degree_name",
            "notes",
            "started_on",
            "finished_on",
            "is_current_education",
        ],
    )


def transform_certifications(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(
        cleaned,
        ["name", "url", "authority", "started_on", "finished_on", "license_number"],
    )
    cleaned = clean_text_columns(cleaned, ["name", "url", "authority", "license_number"])
    cleaned = parse_date_columns(cleaned, ["started_on", "finished_on"])
    return select_columns(
        cleaned,
        ["name", "url", "authority", "started_on", "finished_on", "license_number"],
    )


def transform_endorsement_received_info(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(
        cleaned,
        [
            "endorsement_date",
            "skill_name",
            "endorser_first_name",
            "endorser_last_name",
            "endorser_public_url",
            "endorsement_status",
        ],
    )
    cleaned = clean_text_columns(
        cleaned,
        [
            "skill_name",
            "endorser_first_name",
            "endorser_last_name",
            "endorser_public_url",
            "endorsement_status",
        ],
    )
    cleaned = parse_date_columns(cleaned, ["endorsement_date"])
    cleaned = drop_blank_rows(cleaned, ["skill_name"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(
        cleaned,
        [
            "endorsement_date",
            "skill_name",
            "endorser_first_name",
            "endorser_last_name",
            "endorser_public_url",
            "endorsement_status",
        ],
    )


def transform_company_follows(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(cleaned, ["organization", "followed_on"])
    cleaned = clean_text_columns(cleaned, ["organization"])
    cleaned = parse_date_columns(cleaned, ["followed_on"])
    cleaned = drop_blank_rows(cleaned, ["organization"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(cleaned, ["organization", "followed_on"])


def transform_recommendations_received(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = cleaned.rename(
        columns={
            "job_title": "position",
            "text": "recommendation_text",
            "creation_date": "recommendation_date",
            "status": "visibility",
        }
    )
    cleaned = ensure_columns(
        cleaned,
        [
            "first_name",
            "last_name",
            "company",
            "position",
            "recommendation_text",
            "recommendation_date",
            "visibility",
        ],
    )
    cleaned = clean_text_columns(
        cleaned,
        [
            "first_name",
            "last_name",
            "company",
            "position",
            "recommendation_text",
            "visibility",
        ],
    )
    cleaned["recommendation_date"] = pd.to_datetime(
        cleaned["recommendation_date"],
        format="%m/%d/%y, %I:%M %p",
        errors="coerce",
    )
    cleaned = drop_blank_rows(cleaned, ["recommendation_text"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(
        cleaned,
        [
            "first_name",
            "last_name",
            "company",
            "position",
            "recommendation_text",
            "recommendation_date",
            "visibility",
        ],
    )


def transform_skills(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(cleaned, ["name"])
    cleaned = cleaned.rename(columns={"name": "skill_name"})
    cleaned = clean_text_columns(cleaned, ["skill_name"])
    cleaned = drop_blank_rows(cleaned, ["skill_name"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(cleaned, ["skill_name"])


def transform_phone_numbers(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(cleaned, ["extension", "number", "type"])
    cleaned = cleaned.rename(
        columns={
            "number": "phone_number",
            "type": "phone_type",
        }
    )
    cleaned = clean_text_columns(cleaned, ["extension", "phone_number", "phone_type"])
    cleaned = drop_blank_rows(cleaned, ["phone_number"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(cleaned, ["extension", "phone_number", "phone_type"])


def transform_email_addresses(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = cleaned.rename(
        columns={
            "email_address": "email_address",
            "updated_on": "updated_on",
        }
    )
    cleaned = ensure_columns(cleaned, ["email_address", "confirmed", "primary", "updated_on"])
    cleaned = clean_text_columns(cleaned, ["email_address", "confirmed", "primary", "updated_on"])
    cleaned.loc[cleaned["updated_on"] == "Not Available", "updated_on"] = pd.NA
    cleaned = parse_boolean_columns(cleaned, ["confirmed", "primary"])
    cleaned = drop_blank_rows(cleaned, ["email_address"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(cleaned, ["email_address", "confirmed", "primary", "updated_on"])


def transform_registration(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = cleaned.rename(
        columns={
            "registered_at": "registered_at",
            "registration_ip": "registration_ip",
            "subscription_types": "subscription_types",
        }
    )
    cleaned = ensure_columns(cleaned, ["registered_at", "registration_ip", "subscription_types"])
    cleaned = clean_text_columns(cleaned, ["registration_ip", "subscription_types"])
    cleaned["registered_at"] = pd.to_datetime(
        cleaned["registered_at"],
        format="%m/%d/%y, %I:%M %p",
        errors="coerce",
    )
    return select_columns(cleaned, ["registered_at", "registration_ip", "subscription_types"])


def transform_saved_job_alerts(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(cleaned, ["alert_parameters", "query_context", "saved_search_id"])
    cleaned = clean_text_columns(cleaned, ["alert_parameters", "query_context", "saved_search_id"])
    cleaned = drop_blank_rows(cleaned, ["saved_search_id"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(cleaned, ["alert_parameters", "query_context", "saved_search_id"])


def transform_volunteering(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(
        cleaned,
        ["company_name", "role", "cause", "started_on", "finished_on", "description"],
    )
    cleaned = clean_text_columns(cleaned, ["company_name", "role", "cause", "description"])
    cleaned = parse_date_columns(cleaned, ["started_on", "finished_on"])
    cleaned["is_current_volunteering"] = cleaned["finished_on"].isna()
    cleaned = drop_blank_rows(cleaned, ["company_name", "role"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(
        cleaned,
        [
            "company_name",
            "role",
            "cause",
            "started_on",
            "finished_on",
            "description",
            "is_current_volunteering",
        ],
    )


def transform_invitations(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(
        cleaned,
        [
            "from",
            "to",
            "sent_at",
            "message",
            "direction",
            "inviterprofileurl",
            "inviteeprofileurl",
        ],
    )
    cleaned = cleaned.rename(
        columns={
            "from": "sender_name",
            "to": "recipient_name",
            "inviterprofileurl": "inviter_profile_url",
            "inviteeprofileurl": "invitee_profile_url",
        }
    )
    cleaned = clean_text_columns(
        cleaned,
        [
            "sender_name",
            "recipient_name",
            "message",
            "direction",
            "inviter_profile_url",
            "invitee_profile_url",
        ],
    )
    cleaned["sent_at"] = pd.to_datetime(
        cleaned["sent_at"],
        format="%m/%d/%y, %I:%M %p",
        errors="coerce",
    )
    cleaned = drop_blank_rows(cleaned, ["sender_name", "recipient_name", "direction"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(
        cleaned,
        [
            "sender_name",
            "recipient_name",
            "sent_at",
            "message",
            "direction",
            "inviter_profile_url",
            "invitee_profile_url",
        ],
    )


def transform_events(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = ensure_columns(cleaned, ["event_name", "event_time", "status", "external_url"])
    cleaned = clean_text_columns(cleaned, ["event_name", "event_time", "status", "external_url"])

    event_parts = cleaned["event_time"].astype("string").str.split(" - ", n=1, expand=True)
    cleaned["started_at"] = pd.to_datetime(
        event_parts[0],
        format="%b %d, %Y %I:%M %p",
        errors="coerce",
    )
    cleaned["finished_at"] = pd.to_datetime(
        event_parts[1],
        format="%b %d, %Y %I:%M %p",
        errors="coerce",
    )

    cleaned = drop_blank_rows(cleaned, ["event_name"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(
        cleaned,
        ["event_name", "event_time", "status", "external_url", "started_at", "finished_at"],
    )


def transform_learning(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = cleaned.rename(
        columns={
            "content_last_watched_date_(if_viewed)": "last_watched_at",
            "content_completed_at_(if_completed)": "completed_at",
            "notes_taken_on_videos_(if_taken)": "notes_taken",
            "content_saved": "content_saved",
        }
    )
    cleaned = ensure_columns(
        cleaned,
        [
            "content_title",
            "content_description",
            "content_type",
            "last_watched_at",
            "completed_at",
            "content_saved",
            "notes_taken",
        ],
    )
    cleaned = clean_text_columns(
        cleaned,
        ["content_title", "content_description", "content_type", "notes_taken", "content_saved"],
    )
    cleaned = parse_date_columns(cleaned, ["last_watched_at", "completed_at"])
    cleaned = parse_boolean_columns(cleaned, ["content_saved"])
    cleaned = drop_blank_rows(cleaned, ["content_title"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(
        cleaned,
        [
            "content_title",
            "content_description",
            "content_type",
            "last_watched_at",
            "completed_at",
            "content_saved",
            "notes_taken",
        ],
    )


def transform_job_applications(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = normalize_columns(df)
    cleaned = cleaned.rename(
        columns={
            "application_date": "application_date",
            "contact_email": "contact_email",
            "contact_phone_number": "contact_phone_number",
            "company_name": "company_name",
            "job_title": "job_title",
            "job_url": "job_url",
            "resume_name": "resume_name",
            "question_and_answers": "question_and_answers",
        }
    )
    cleaned = ensure_columns(
        cleaned,
        [
            "application_date",
            "contact_email",
            "contact_phone_number",
            "company_name",
            "job_title",
            "job_url",
            "resume_name",
            "question_and_answers",
        ],
    )
    cleaned = clean_text_columns(
        cleaned,
        [
            "contact_email",
            "contact_phone_number",
            "company_name",
            "job_title",
            "job_url",
            "resume_name",
            "question_and_answers",
        ],
    )
    cleaned["application_date"] = pd.to_datetime(
        cleaned["application_date"],
        format="%m/%d/%y, %I:%M %p",
        errors="coerce",
    )
    cleaned = drop_blank_rows(cleaned, ["company_name", "job_title"])
    cleaned = deduplicate_rows(cleaned)
    return select_columns(
        cleaned,
        [
            "application_date",
            "contact_email",
            "contact_phone_number",
            "company_name",
            "job_title",
            "job_url",
            "resume_name",
            "question_and_answers",
        ],
    )


TABLES: dict[str, TableConfig] = {
    "connections": TableConfig(
        key="connections",
        bronze_table="connections",
        csv_name="Connections.csv",
        export_type="complete",
        transform=transform_connections,
        contract=TableContract(
            required_columns=(
                "first_name",
                "last_name",
                "url",
                "email_address",
                "company",
                "position",
                "connected_on",
            ),
            sensitive_columns=("first_name", "last_name", "email_address", "url"),
            description="Professional network connections exported from LinkedIn.",
        ),
        read_kwargs={"skiprows": 3},
    ),
    "positions": TableConfig(
        key="positions",
        bronze_table="positions",
        csv_name="Positions.csv",
        export_type="basic",
        transform=transform_positions,
        contract=TableContract(
            required_columns=(
                "company_name",
                "title",
                "description",
                "location",
                "started_on",
                "finished_on",
                "is_current",
            ),
            sensitive_columns=("description",),
            description="Professional positions and career history.",
        ),
    ),
    "profile": TableConfig(
        key="profile",
        bronze_table="profile",
        csv_name="Profile.csv",
        export_type="basic",
        transform=transform_profile,
        contract=TableContract(
            required_columns=(
                "first_name",
                "last_name",
                "maiden_name",
                "address",
                "birth_date",
                "headline",
                "summary",
                "industry",
                "zip_code",
                "geo_location",
                "twitter_handles",
                "websites",
                "instant_messengers",
            ),
            sensitive_columns=(
                "first_name",
                "last_name",
                "maiden_name",
                "address",
                "birth_date",
                "zip_code",
                "twitter_handles",
                "websites",
                "instant_messengers",
            ),
            description="Primary LinkedIn member profile and identity attributes.",
        ),
    ),
    "languages": TableConfig(
        key="languages",
        bronze_table="languages",
        csv_name="Languages.csv",
        export_type="basic",
        transform=transform_languages,
        contract=TableContract(
            required_columns=("name", "proficiency"),
            description="Languages and proficiencies listed on the profile.",
        ),
    ),
    "education": TableConfig(
        key="education",
        bronze_table="education",
        csv_name="Education.csv",
        export_type="basic",
        transform=transform_education,
        contract=TableContract(
            required_columns=(
                "school_name",
                "degree_name",
                "notes",
                "started_on",
                "finished_on",
                "is_current_education",
            ),
            sensitive_columns=("notes",),
            description="Academic background and education timeline.",
        ),
    ),
    "certifications": TableConfig(
        key="certifications",
        bronze_table="certifications",
        csv_name="Certifications.csv",
        export_type="basic",
        transform=transform_certifications,
        contract=TableContract(
            required_columns=(
                "name",
                "url",
                "authority",
                "started_on",
                "finished_on",
                "license_number",
            ),
            sensitive_columns=("license_number", "url"),
            description="Certifications and credential metadata.",
        ),
    ),
    "endorsement_received_info": TableConfig(
        key="endorsement_received_info",
        bronze_table="endorsement_received_info",
        csv_name="Endorsement_Received_Info.csv",
        export_type="basic",
        transform=transform_endorsement_received_info,
        contract=TableContract(
            required_columns=(
                "endorsement_date",
                "skill_name",
                "endorser_first_name",
                "endorser_last_name",
                "endorser_public_url",
                "endorsement_status",
            ),
            unique_columns=(
                "endorsement_date",
                "skill_name",
                "endorser_first_name",
                "endorser_last_name",
            ),
            sensitive_columns=(
                "endorser_first_name",
                "endorser_last_name",
                "endorser_public_url",
            ),
            description="Received endorsements grouped by skill and endorser.",
        ),
    ),
    "company_follows": TableConfig(
        key="company_follows",
        bronze_table="company_follows",
        csv_name="Company Follows.csv",
        export_type="complete",
        transform=transform_company_follows,
        contract=TableContract(
            required_columns=("organization", "followed_on"),
            unique_columns=("organization", "followed_on"),
            description="Organizations followed by the member.",
        ),
    ),
    "recommendations_received": TableConfig(
        key="recommendations_received",
        bronze_table="recommendations_received",
        csv_name="Recommendations_Received.csv",
        export_type="complete",
        transform=transform_recommendations_received,
        contract=TableContract(
            required_columns=(
                "first_name",
                "last_name",
                "company",
                "position",
                "recommendation_text",
                "recommendation_date",
                "visibility",
            ),
            unique_columns=("first_name", "last_name", "recommendation_date"),
            sensitive_columns=("first_name", "last_name", "recommendation_text"),
            description="Text recommendations received from professional contacts.",
        ),
    ),
    "skills": TableConfig(
        key="skills",
        bronze_table="skills",
        csv_name="Skills.csv",
        export_type="complete",
        transform=transform_skills,
        contract=TableContract(
            required_columns=("skill_name",),
            unique_columns=("skill_name",),
            description="Unique skills listed by the member.",
        ),
    ),
    "phone_numbers": TableConfig(
        key="phone_numbers",
        bronze_table="phone_numbers",
        csv_name="PhoneNumbers.csv",
        export_type="basic",
        transform=transform_phone_numbers,
        contract=TableContract(
            required_columns=("extension", "phone_number", "phone_type"),
            unique_columns=("phone_number", "phone_type"),
            sensitive_columns=("extension", "phone_number"),
            description="Phone numbers registered in the profile account.",
        ),
        read_kwargs={"dtype": "string"},
    ),
    "email_addresses": TableConfig(
        key="email_addresses",
        bronze_table="email_addresses",
        csv_name="Email Addresses.csv",
        export_type="basic",
        transform=transform_email_addresses,
        contract=TableContract(
            required_columns=("email_address", "confirmed", "primary", "updated_on"),
            unique_columns=("email_address",),
            sensitive_columns=("email_address",),
            description="Account email addresses and confirmation flags.",
        ),
        read_kwargs={"dtype": "string"},
    ),
    "registration": TableConfig(
        key="registration",
        bronze_table="registration",
        csv_name="Registration.csv",
        export_type="basic",
        transform=transform_registration,
        contract=TableContract(
            required_columns=("registered_at", "registration_ip", "subscription_types"),
            sensitive_columns=("registration_ip",),
            description="Registration metadata for the account.",
        ),
        read_kwargs={"dtype": "string"},
    ),
    "saved_job_alerts": TableConfig(
        key="saved_job_alerts",
        bronze_table="saved_job_alerts",
        csv_name="SavedJobAlerts.csv",
        export_type="complete",
        transform=transform_saved_job_alerts,
        contract=TableContract(
            required_columns=("alert_parameters", "query_context", "saved_search_id"),
            unique_columns=("saved_search_id",),
            description="Saved job alerts and search parameters.",
        ),
        read_kwargs={"dtype": "string"},
    ),
    "volunteering": TableConfig(
        key="volunteering",
        bronze_table="volunteering",
        csv_name="Volunteering.csv",
        export_type="basic",
        transform=transform_volunteering,
        contract=TableContract(
            required_columns=(
                "company_name",
                "role",
                "cause",
                "started_on",
                "finished_on",
                "description",
                "is_current_volunteering",
            ),
            unique_columns=("company_name", "role", "started_on"),
            sensitive_columns=("description",),
            description="Volunteering experiences and associated causes.",
        ),
    ),
    "invitations": TableConfig(
        key="invitations",
        bronze_table="invitations",
        csv_name="Invitations.csv",
        export_type="complete",
        transform=transform_invitations,
        contract=TableContract(
            required_columns=(
                "sender_name",
                "recipient_name",
                "sent_at",
                "message",
                "direction",
                "inviter_profile_url",
                "invitee_profile_url",
            ),
            unique_columns=("sender_name", "recipient_name", "sent_at", "direction"),
            sensitive_columns=(
                "sender_name",
                "recipient_name",
                "message",
                "inviter_profile_url",
                "invitee_profile_url",
            ),
            description="Invitations sent and received through LinkedIn.",
        ),
    ),
    "events": TableConfig(
        key="events",
        bronze_table="events",
        csv_name="Events.csv",
        export_type="complete",
        transform=transform_events,
        contract=TableContract(
            required_columns=(
                "event_name",
                "event_time",
                "status",
                "external_url",
                "started_at",
                "finished_at",
            ),
            unique_columns=("event_name", "event_time"),
            sensitive_columns=("external_url",),
            description="Events tracked by LinkedIn, including attendance status.",
        ),
    ),
    "learning": TableConfig(
        key="learning",
        bronze_table="learning",
        csv_name="Learning.csv",
        export_type="complete",
        transform=transform_learning,
        contract=TableContract(
            required_columns=(
                "content_title",
                "content_description",
                "content_type",
                "last_watched_at",
                "completed_at",
                "content_saved",
                "notes_taken",
            ),
            unique_columns=("content_title", "last_watched_at", "completed_at"),
            sensitive_columns=("notes_taken",),
            description="LinkedIn Learning content activity and completion status.",
        ),
    ),
    "job_applications": TableConfig(
        key="job_applications",
        bronze_table="job_applications",
        csv_name="Jobs/Job Applications.csv",
        export_type="complete",
        transform=transform_job_applications,
        contract=TableContract(
            required_columns=(
                "application_date",
                "contact_email",
                "contact_phone_number",
                "company_name",
                "job_title",
                "job_url",
                "resume_name",
                "question_and_answers",
            ),
            unique_columns=("company_name", "job_title", "application_date"),
            sensitive_columns=(
                "contact_email",
                "contact_phone_number",
                "job_url",
                "question_and_answers",
            ),
            description="Job applications and related contact metadata.",
        ),
        read_kwargs={"dtype": "string"},
    ),
}


def load_table(key: str, settings: ProjectSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    table = TABLES[key]
    csv_path = settings.export_dir(table.export_type) / table.csv_name
    df = read_csv_safe(csv_path, **table.read_kwargs)
    cleaned = table.transform(df)
    validation = assert_contract(cleaned, table.contract)
    write_dataframe_to_bronze(cleaned, table.bronze_table, settings=settings)
    audit_row = build_ingestion_audit_row(
        table,
        csv_path,
        cleaned,
        duplicate_rows=validation.duplicate_rows,
    )
    write_dataframe_to_bronze(
        audit_row,
        "ingestion_audit",
        mode="append",
        settings=settings,
    )
    return cleaned


def load_all_tables(settings: ProjectSettings | None = None) -> dict[str, int]:
    settings = settings or get_settings()
    loaded_rows: dict[str, int] = {}
    for key in TABLES:
        cleaned = load_table(key, settings=settings)
        loaded_rows[key] = len(cleaned)
    return loaded_rows
