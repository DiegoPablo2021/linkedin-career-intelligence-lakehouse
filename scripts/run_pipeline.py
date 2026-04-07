from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "linkedin_career_intelligence_dbt"
PROFILES_DIR = PROJECT_ROOT / "profiles"


def run_step(command: list[str], workdir: Path) -> None:
    print(f"\n> {' '.join(command)}")
    completed = subprocess.run(command, cwd=workdir, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    python_executable = sys.executable

    run_step([python_executable, "scripts/run_ingestion.py"], PROJECT_ROOT)
    run_step([python_executable, "scripts/profiling/inventory_linkedin_exports.py"], PROJECT_ROOT)
    run_step(
        ["dbt", "build", "--profiles-dir", str(PROFILES_DIR)],
        DBT_DIR,
    )
    print("\nPipeline end-to-end finalizado com sucesso.")
