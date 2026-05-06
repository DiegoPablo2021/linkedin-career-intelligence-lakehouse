from __future__ import annotations

import json
from pathlib import Path
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPORTS_DIR = PROJECT_ROOT / "powerbi" / "exports"

REQUIRED_FILES = {
    "dCalendario": "dim_date.parquet",
    "health_fact": "fact_ingestion_audit_health_timeline.parquet",
    "null_rate_fact": "fact_ingestion_audit_null_rate_timeline.parquet",
}

EXPECTED_COLUMNS = {
    "dim_date.parquet": {
        "date",
        "year_month",
    },
    "fact_ingestion_audit_health_timeline.parquet": {
        "loaded_on",
        "table_key",
        "export_type",
        "source_row_count",
        "row_count_after_transform",
        "rows_removed_during_transform",
        "duplicate_rows_after_transform",
        "row_retention_rate",
        "health_status",
        "successful_load_flag",
    },
    "fact_ingestion_audit_null_rate_timeline.parquet": {
        "loaded_on",
        "table_key",
        "export_type",
        "monitored_column",
        "null_rate_before_transform",
        "null_rate_after_transform",
        "null_rate_delta",
        "null_rate_alert_flag",
    },
}


def load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def main() -> int:
    result: dict[str, object] = {
        "exports_dir": str(EXPORTS_DIR),
        "files_found": {},
        "missing_files": [],
        "column_validation": {},
        "row_counts": {},
        "relationship_checklist": [
            {
                "from_table": "dCalendario",
                "from_column": "date",
                "to_table": "fact_ingestion_audit_health_timeline",
                "to_column": "loaded_on",
                "is_active": True,
                "cross_filter": "OneDirection",
            },
            {
                "from_table": "dCalendario",
                "from_column": "date",
                "to_table": "fact_ingestion_audit_null_rate_timeline",
                "to_column": "loaded_on",
                "is_active": True,
                "cross_filter": "OneDirection",
            },
        ],
    }

    for logical_name, file_name in REQUIRED_FILES.items():
        path = EXPORTS_DIR / file_name
        exists = path.exists()
        result["files_found"] = {
            **result["files_found"],
            logical_name: {"file": file_name, "exists": exists},
        }
        if not exists:
            result["missing_files"].append(file_name)
            continue

        frame = load_frame(path)
        actual_columns = set(frame.columns)
        expected_columns = EXPECTED_COLUMNS[file_name]
        missing_columns = sorted(expected_columns - actual_columns)

        result["row_counts"] = {
            **result["row_counts"],
            file_name: len(frame),
        }
        result["column_validation"] = {
            **result["column_validation"],
            file_name: {
                "missing_columns": missing_columns,
                "all_expected_columns_present": not missing_columns,
            },
        }

    is_valid = not result["missing_files"] and all(
        item["all_expected_columns_present"]
        for item in result["column_validation"].values()
    )
    result["is_valid_for_observability_page"] = is_valid

    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
