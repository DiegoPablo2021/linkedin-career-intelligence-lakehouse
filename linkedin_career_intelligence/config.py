from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_EXPORTS = {
    "basic": "basic_export_2026_04_04",
    "complete": "complete_export_2026_04_05",
}


@dataclass(frozen=True)
class ProjectSettings:
    project_root: Path
    data_dir: Path
    raw_dir: Path
    warehouse_dir: Path
    db_path: Path

    def export_dir(self, export_type: str) -> Path:
        env_key = f"LINKEDIN_{export_type.upper()}_EXPORT_DIR"
        configured = os.getenv(env_key)
        export_name = configured or DEFAULT_EXPORTS[export_type]
        return self.raw_dir / export_name


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def get_settings() -> ProjectSettings:
    project_root = get_project_root()
    data_dir = project_root / "data"
    warehouse_dir = project_root / "warehouse"
    return ProjectSettings(
        project_root=project_root,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        warehouse_dir=warehouse_dir,
        db_path=warehouse_dir / "linkedin_career_intelligence.duckdb",
    )

