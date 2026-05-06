from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

from linkedin_career_intelligence.config import get_settings


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "linkedin_career_intelligence_dbt"
PROFILES_DIR = PROJECT_ROOT / "profiles"


def run_step(command: list[str], workdir: Path) -> None:
    print(f"\n> {' '.join(command)}")
    completed = subprocess.run(command, cwd=workdir, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def cleanup_duckdb_sidecars(db_path: Path) -> None:
    for suffix in (".wal", ".tmp"):
        sidecar = db_path.with_name(f"{db_path.name}{suffix}")
        if sidecar.exists():
            sidecar.unlink()


@contextmanager
def pipeline_database_guard(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path: Path | None = None
    db_existed = db_path.exists()

    if db_existed:
        with tempfile.NamedTemporaryFile(
            prefix=f"{db_path.stem}_backup_",
            suffix=".duckdb",
            delete=False,
            dir=str(db_path.parent),
        ) as backup_file:
            backup_path = Path(backup_file.name)
        shutil.copy2(db_path, backup_path)

    try:
        yield
    except Exception:
        cleanup_duckdb_sidecars(db_path)
        if backup_path is not None:
            shutil.copy2(backup_path, db_path)
        elif db_path.exists():
            db_path.unlink()
        raise
    finally:
        if backup_path is not None and backup_path.exists():
            backup_path.unlink()


if __name__ == "__main__":
    python_executable = sys.executable
    settings = get_settings()

    with pipeline_database_guard(settings.db_path):
        run_step([python_executable, "scripts/run_ingestion.py"], PROJECT_ROOT)
        run_step([python_executable, "scripts/profiling/inventory_linkedin_exports.py"], PROJECT_ROOT)
        run_step(
            ["dbt", "build", "--profiles-dir", str(PROFILES_DIR)],
            DBT_DIR,
        )
    print("\nPipeline end-to-end finalizado com sucesso.")
