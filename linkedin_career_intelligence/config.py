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
    demo_dir: Path
    db_path: Path
    default_db_path: Path
    demo_db_path: Path
    is_demo_db: bool

    def export_dir(self, export_type: str) -> Path:
        env_key = f"LINKEDIN_{export_type.upper()}_EXPORT_DIR"
        configured = os.getenv(env_key)
        export_name = configured or DEFAULT_EXPORTS[export_type]
        return self.raw_dir / export_name


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_db_path(default_db_path: Path, demo_db_path: Path) -> tuple[Path, bool]:
    configured_db_path = os.getenv("LINKEDIN_DB_PATH")
    if configured_db_path:
        resolved_path = Path(configured_db_path).expanduser()
        if not resolved_path.is_absolute():
            resolved_path = get_project_root() / resolved_path
        return resolved_path, resolved_path.resolve() == demo_db_path.resolve()

    if default_db_path.exists():
        return default_db_path, False

    if demo_db_path.exists():
        return demo_db_path, True

    return default_db_path, False


def get_settings() -> ProjectSettings:
    project_root = get_project_root()
    data_dir = project_root / "data"
    warehouse_dir = project_root / "warehouse"
    demo_dir = project_root / "demo"
    default_db_path = warehouse_dir / "linkedin_career_intelligence.duckdb"
    demo_db_path = demo_dir / "linkedin_career_intelligence_demo.duckdb"
    db_path, is_demo_db = resolve_db_path(default_db_path, demo_db_path)
    return ProjectSettings(
        project_root=project_root,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        warehouse_dir=warehouse_dir,
        demo_dir=demo_dir,
        db_path=db_path,
        default_db_path=default_db_path,
        demo_db_path=demo_db_path,
        is_demo_db=is_demo_db,
    )
