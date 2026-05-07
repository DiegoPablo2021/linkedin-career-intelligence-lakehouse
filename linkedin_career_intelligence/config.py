from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path


EXPORT_DIR_PATTERNS = {
    "basic": re.compile(r"^basic_export_(\d{4})_(\d{2})_(\d{2})$", re.IGNORECASE),
    "complete": re.compile(r"^complete_export_(\d{4})_(\d{2})_(\d{2})$", re.IGNORECASE),
}

EXPORT_MARKERS = {
    "basic": ("Profile.csv", "Positions.csv"),
    "complete": ("Connections.csv", "Jobs/Job Applications.csv"),
}


@dataclass(frozen=True)
class ResolvedExportDir:
    export_type: str
    path: Path
    source: str
    label: str
    exists: bool
    warning: str | None = None


@dataclass(frozen=True)
class ProjectSettings:
    project_root: Path
    data_dir: Path
    raw_dir: Path
    manual_exports_dir: Path
    warehouse_dir: Path
    demo_dir: Path
    db_path: Path
    default_db_path: Path
    demo_db_path: Path
    is_demo_db: bool

    def resolve_path(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        if not path.is_absolute():
            path = self.project_root / path
        return path

    def _looks_like_export_dir(self, path: Path, export_type: str) -> bool:
        if not path.is_dir():
            return False
        return any((path / marker).exists() for marker in EXPORT_MARKERS[export_type])

    def _extract_export_date(self, path: Path, export_type: str) -> date | None:
        match = EXPORT_DIR_PATTERNS[export_type].match(path.name)
        if not match:
            return None
        year, month, day = (int(part) for part in match.groups())
        return date(year, month, day)

    def _candidate_sort_key(self, path: Path, export_type: str) -> tuple[date, float, str]:
        parsed_date = self._extract_export_date(path, export_type) or date.min
        modified_at = path.stat().st_mtime if path.exists() else 0.0
        return (parsed_date, modified_at, path.name.lower())

    def _typed_export_candidates(self, export_type: str) -> list[Path]:
        candidates: list[Path] = []

        if self.raw_dir.exists():
            candidates.extend(
                child
                for child in self.raw_dir.iterdir()
                if self._extract_export_date(child, export_type) is not None
                and self._looks_like_export_dir(child, export_type)
            )

        manual_root = self.manual_exports_dir
        if manual_root.exists():
            candidates.extend(
                child
                for child in manual_root.iterdir()
                if self._looks_like_export_dir(child, export_type)
            )
            nested_root = manual_root / export_type
            if nested_root.exists():
                candidates.extend(
                    child
                    for child in nested_root.iterdir()
                    if self._looks_like_export_dir(child, export_type)
                )

        deduped = {candidate.resolve(): candidate for candidate in candidates}
        return sorted(
            deduped.values(),
            key=lambda item: self._candidate_sort_key(item, export_type),
            reverse=True,
        )

    def _cross_type_fallback_candidates(self, export_type: str) -> list[Path]:
        candidates: list[Path] = []
        if self.raw_dir.exists():
            candidates.extend(
                child for child in self.raw_dir.iterdir() if self._looks_like_export_dir(child, export_type)
            )
        deduped = {candidate.resolve(): candidate for candidate in candidates}
        return sorted(
            deduped.values(),
            key=lambda item: (item.stat().st_mtime if item.exists() else 0.0, item.name.lower()),
            reverse=True,
        )

    def _legacy_default_path(self, export_type: str) -> Path:
        return self.raw_dir / f"{export_type}_export_latest"

    def resolve_export_dir(self, export_type: str) -> ResolvedExportDir:
        env_key = f"LINKEDIN_{export_type.upper()}_EXPORT_DIR"
        configured = os.getenv(env_key)
        if configured:
            resolved = self.resolve_path(configured)
            return ResolvedExportDir(
                export_type=export_type,
                path=resolved,
                source="env",
                label=resolved.name,
                exists=resolved.exists(),
                warning=None if resolved.exists() else f"{env_key} points to a missing directory.",
            )

        typed_candidates = self._typed_export_candidates(export_type)
        if typed_candidates:
            selected = typed_candidates[0]
            return ResolvedExportDir(
                export_type=export_type,
                path=selected,
                source="dated_raw_dir",
                label=selected.name,
                exists=True,
            )

        cross_type_candidates = self._cross_type_fallback_candidates(export_type)
        if cross_type_candidates:
            selected = cross_type_candidates[0]
            return ResolvedExportDir(
                export_type=export_type,
                path=selected,
                source="cross_type_fallback",
                label=selected.name,
                exists=True,
                warning=(
                    f"No {export_type}_export_YYYY_MM_DD directory was found. "
                    f"Falling back to {selected.name} because it still contains the required files."
                ),
            )

        legacy = self._legacy_default_path(export_type)
        return ResolvedExportDir(
            export_type=export_type,
            path=legacy,
            source="legacy_default",
            label=legacy.name,
            exists=legacy.exists(),
            warning=(
                f"No valid {export_type} export directory was discovered under {self.raw_dir}."
            ),
        )

    def export_dir(self, export_type: str) -> Path:
        return self.resolve_export_dir(export_type).path


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def resolve_db_path(default_db_path: Path, demo_db_path: Path) -> tuple[Path, bool]:
    configured_db_path = os.getenv("LINKEDIN_DB_PATH") or os.getenv("LINKEDIN_DUCKDB_PATH")
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
        manual_exports_dir=(data_dir / "raw" / "linkedin_exports"),
        warehouse_dir=warehouse_dir,
        demo_dir=demo_dir,
        db_path=db_path,
        default_db_path=default_db_path,
        demo_db_path=demo_db_path,
        is_demo_db=is_demo_db,
    )
