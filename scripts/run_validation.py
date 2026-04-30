from __future__ import annotations

import os
import shutil
import site
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_ROOT / "linkedin_career_intelligence_dbt"
PROFILES_DIR = PROJECT_ROOT / "profiles"


def run_step(command: list[str], workdir: Path, env: dict[str, str] | None = None) -> None:
    print(f"\n> {' '.join(command)}")
    completed = subprocess.run(command, cwd=workdir, check=False, env=env)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def resolve_cli_command(command_name: str, fallback: list[str]) -> list[str]:
    resolved = shutil.which(command_name)
    if resolved:
        return [resolved]

    user_scripts_dir = Path(site.getuserbase()) / ("Scripts" if os.name == "nt" else "bin")
    suffix = ".exe" if os.name == "nt" else ""
    candidate = user_scripts_dir / f"{command_name}{suffix}"
    if candidate.exists():
        return [str(candidate)]

    return fallback


def main() -> None:
    python_executable = sys.executable
    validation_db = PROJECT_ROOT / "warehouse" / "linkedin_career_intelligence_validation.duckdb"
    env = os.environ.copy()
    env["LINKEDIN_DUCKDB_PATH"] = str(validation_db)
    env["LINKEDIN_DB_PATH"] = str(validation_db)
    sqlfluff_command = resolve_cli_command("sqlfluff", [python_executable, "-m", "sqlfluff"])
    dbt_command = resolve_cli_command("dbt", [python_executable, "-m", "dbt.cli.main"])

    run_step([python_executable, "-m", "pytest", "-q"], PROJECT_ROOT, env=env)
    run_step([python_executable, "scripts/ci/bootstrap_validation_warehouse.py"], PROJECT_ROOT, env=env)
    run_step([*sqlfluff_command, "lint"], PROJECT_ROOT, env=env)
    run_step(
        [
            *dbt_command,
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(PROFILES_DIR),
        ],
        PROJECT_ROOT,
        env=env,
    )
    print("\nValidação completa finalizada com sucesso.")


if __name__ == "__main__":
    main()
