from __future__ import annotations

from pathlib import Path

import pytest

from linkedin_career_intelligence.config import ProjectSettings


def build_settings(tmp_path: Path) -> ProjectSettings:
    project_root = tmp_path
    data_dir = project_root / "data"
    warehouse_dir = project_root / "warehouse"
    demo_dir = project_root / "demo"
    default_db_path = warehouse_dir / "linkedin_career_intelligence.duckdb"
    demo_db_path = demo_dir / "linkedin_career_intelligence_demo.duckdb"
    return ProjectSettings(
        project_root=project_root,
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        manual_exports_dir=data_dir / "raw" / "linkedin_exports",
        warehouse_dir=warehouse_dir,
        demo_dir=demo_dir,
        db_path=default_db_path,
        default_db_path=default_db_path,
        demo_db_path=demo_db_path,
        is_demo_db=False,
    )


def test_resolve_export_dir_prefers_latest_dated_raw_directory(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    older = settings.raw_dir / "complete_export_2026_05_01"
    newer = settings.raw_dir / "complete_export_2026_05_07"
    for candidate in (older, newer):
        (candidate / "Jobs").mkdir(parents=True, exist_ok=True)
        (candidate / "Connections.csv").write_text("header\n", encoding="utf-8")
        (candidate / "Jobs" / "Job Applications.csv").write_text("header\n", encoding="utf-8")

    resolved = settings.resolve_export_dir("complete")

    assert resolved.path == newer
    assert resolved.source == "dated_raw_dir"
    assert resolved.label == "complete_export_2026_05_07"
    assert resolved.warning is None


def test_resolve_export_dir_prefers_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = build_settings(tmp_path)
    explicit = tmp_path / "custom_complete"
    explicit.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("LINKEDIN_COMPLETE_EXPORT_DIR", str(explicit))

    resolved = settings.resolve_export_dir("complete")

    assert resolved.path == explicit
    assert resolved.source == "env"


def test_resolve_export_dir_falls_back_to_cross_type_directory_when_needed(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)
    fallback = settings.raw_dir / "complete_export_2026_05_07"
    (fallback / "Jobs").mkdir(parents=True, exist_ok=True)
    (fallback / "Profile.csv").write_text("header\n", encoding="utf-8")
    (fallback / "Positions.csv").write_text("header\n", encoding="utf-8")
    (fallback / "Connections.csv").write_text("header\n", encoding="utf-8")
    (fallback / "Jobs" / "Job Applications.csv").write_text("header\n", encoding="utf-8")

    resolved = settings.resolve_export_dir("basic")

    assert resolved.path == fallback
    assert resolved.source == "cross_type_fallback"
    assert resolved.exists is True
    assert resolved.warning is not None


def test_resolve_export_dir_reports_missing_directory_when_nothing_is_available(tmp_path: Path) -> None:
    settings = build_settings(tmp_path)

    resolved = settings.resolve_export_dir("basic")

    assert resolved.path == settings.raw_dir / "basic_export_latest"
    assert resolved.source == "legacy_default"
    assert resolved.exists is False
    assert resolved.warning is not None
