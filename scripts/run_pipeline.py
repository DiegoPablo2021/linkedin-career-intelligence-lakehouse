from __future__ import annotations

import argparse
import os
import shutil
import site
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.config import ResolvedExportDir, get_settings


DBT_DIR = PROJECT_ROOT / "linkedin_career_intelligence_dbt"
PROFILES_DIR = PROJECT_ROOT / "profiles"


def run_step(command: list[str], workdir: Path, env: dict[str, str] | None = None) -> None:
    print(f"\n> {' '.join(command)}")
    completed = subprocess.run(command, cwd=workdir, check=False, env=env)
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


def resolve_repo_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Executa o pipeline operacional semi-automatizado. "
            "A ingestao dos exports continua manual; todo o restante e automatizado."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("manual_exports", "validation_fixture"),
        default="manual_exports",
        help=(
            "manual_exports processa os exports resolvidos em data/raw "
            "ou via variaveis LINKEDIN_*_EXPORT_DIR. "
            "validation_fixture usa a fixture sintetica da CI para validar o workflow no GitHub Actions."
        ),
    )
    parser.add_argument(
        "--basic-export-dir",
        help="Diretorio do export basic para esta execucao. Aceita caminho absoluto ou relativo ao repo.",
    )
    parser.add_argument(
        "--complete-export-dir",
        help="Diretorio do export complete para esta execucao. Aceita caminho absoluto ou relativo ao repo.",
    )
    parser.add_argument(
        "--db-path",
        help="Banco DuckDB alvo para a execucao. Aceita caminho absoluto ou relativo ao repo.",
    )
    parser.add_argument(
        "--powerbi-output-dir",
        default="powerbi/exports",
        help="Diretorio de saida dos exports da camada Power BI.",
    )
    parser.add_argument(
        "--powerbi-format",
        choices=("csv", "parquet", "both"),
        default="both",
        help="Formato de saida da camada Power BI operacional.",
    )
    return parser.parse_args()


def build_runtime_env(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    basic_export_dir = resolve_repo_path(args.basic_export_dir)
    complete_export_dir = resolve_repo_path(args.complete_export_dir)
    db_path = resolve_repo_path(args.db_path)

    if basic_export_dir is not None:
        env["LINKEDIN_BASIC_EXPORT_DIR"] = str(basic_export_dir)
    if complete_export_dir is not None:
        env["LINKEDIN_COMPLETE_EXPORT_DIR"] = str(complete_export_dir)
    if db_path is not None:
        env["LINKEDIN_DB_PATH"] = str(db_path)
        env["LINKEDIN_DUCKDB_PATH"] = str(db_path)

    return env


def print_resolution_summary(env: dict[str, str], mode: str) -> None:
    settings = get_settings()
    db_path = Path(env.get("LINKEDIN_DB_PATH", str(settings.db_path)))

    print("\nPipeline configuration")
    print(f"- mode: {mode}")
    print(f"- duckdb: {db_path}")
    if mode == "manual_exports":
        print(f"- raw exports root: {settings.raw_dir}")
        print(f"- compatibility root: {settings.manual_exports_dir}")


def log_resolved_export(resolved: ResolvedExportDir) -> None:
    print(
        f"- {resolved.export_type} export: {resolved.path} "
        f"[source={resolved.source}, exists={resolved.exists}]"
    )
    if resolved.warning:
        print(f"  warning: {resolved.warning}")


def validate_manual_export_resolution(env: dict[str, str]) -> None:
    original_basic = os.getenv("LINKEDIN_BASIC_EXPORT_DIR")
    original_complete = os.getenv("LINKEDIN_COMPLETE_EXPORT_DIR")

    try:
        if "LINKEDIN_BASIC_EXPORT_DIR" in env:
            os.environ["LINKEDIN_BASIC_EXPORT_DIR"] = env["LINKEDIN_BASIC_EXPORT_DIR"]
        else:
            os.environ.pop("LINKEDIN_BASIC_EXPORT_DIR", None)

        if "LINKEDIN_COMPLETE_EXPORT_DIR" in env:
            os.environ["LINKEDIN_COMPLETE_EXPORT_DIR"] = env["LINKEDIN_COMPLETE_EXPORT_DIR"]
        else:
            os.environ.pop("LINKEDIN_COMPLETE_EXPORT_DIR", None)

        settings = get_settings()
        basic = settings.resolve_export_dir("basic")
        complete = settings.resolve_export_dir("complete")

        print("\nResolved exports")
        log_resolved_export(basic)
        log_resolved_export(complete)

        missing = [item for item in (basic, complete) if not item.exists]
        if missing:
            missing_list = ", ".join(item.export_type for item in missing)
            raise SystemExit(
                f"Missing required export directories for: {missing_list}. "
                "Place new folders in data/raw using basic_export_YYYY_MM_DD and complete_export_YYYY_MM_DD."
            )
    finally:
        if original_basic is None:
            os.environ.pop("LINKEDIN_BASIC_EXPORT_DIR", None)
        else:
            os.environ["LINKEDIN_BASIC_EXPORT_DIR"] = original_basic

        if original_complete is None:
            os.environ.pop("LINKEDIN_COMPLETE_EXPORT_DIR", None)
        else:
            os.environ["LINKEDIN_COMPLETE_EXPORT_DIR"] = original_complete


def run_dbt_build(env: dict[str, str]) -> None:
    dbt_command = resolve_cli_command("dbt", [sys.executable, "-m", "dbt.cli.main"])
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


def run_exports(env: dict[str, str], powerbi_output_dir: Path, powerbi_format: str) -> None:
    run_step(
        [
            sys.executable,
            "scripts/utils/export_powerbi_layer.py",
            "--output-dir",
            str(powerbi_output_dir),
            "--format",
            powerbi_format,
        ],
        PROJECT_ROOT,
        env=env,
    )
    run_step(
        [
            sys.executable,
            "scripts/powerbi/export_snapshot_tables.py",
            "--output-dir",
            str(powerbi_output_dir),
        ],
        PROJECT_ROOT,
        env=env,
    )
    run_step(
        [
            sys.executable,
            "scripts/utils/validate_powerbi_observability_layer.py",
            "--exports-dir",
            str(powerbi_output_dir),
        ],
        PROJECT_ROOT,
        env=env,
    )


def run_manual_exports_pipeline(
    env: dict[str, str],
    powerbi_output_dir: Path,
    powerbi_format: str,
) -> None:
    settings = get_settings()
    db_path = Path(env.get("LINKEDIN_DB_PATH", str(settings.db_path)))

    validate_manual_export_resolution(env)
    print(f"\nOutput directory: {powerbi_output_dir}")

    with pipeline_database_guard(db_path):
        run_step([sys.executable, "scripts/run_ingestion.py"], PROJECT_ROOT, env=env)
        run_step([sys.executable, "scripts/profiling/inventory_linkedin_exports.py"], PROJECT_ROOT, env=env)
        run_dbt_build(env)
        run_step([sys.executable, "scripts/snapshots/build_historical_snapshots.py"], PROJECT_ROOT, env=env)
        run_exports(env, powerbi_output_dir, powerbi_format)


def run_validation_fixture_pipeline(
    env: dict[str, str],
    powerbi_output_dir: Path,
    powerbi_format: str,
) -> None:
    validation_db = Path(
        env.get(
            "LINKEDIN_DB_PATH",
            str(PROJECT_ROOT / "warehouse" / "linkedin_career_intelligence_validation.duckdb"),
        )
    )
    env = env.copy()
    env["LINKEDIN_DB_PATH"] = str(validation_db)
    env["LINKEDIN_DUCKDB_PATH"] = str(validation_db)

    print(f"\nOutput directory: {powerbi_output_dir}")
    run_step([sys.executable, "scripts/ci/bootstrap_validation_warehouse.py"], PROJECT_ROOT, env=env)
    run_dbt_build(env)
    run_step([sys.executable, "scripts/snapshots/build_historical_snapshots.py"], PROJECT_ROOT, env=env)
    run_exports(env, powerbi_output_dir, powerbi_format)


def main() -> None:
    args = parse_args()
    env = build_runtime_env(args)
    powerbi_output_dir = resolve_repo_path(args.powerbi_output_dir)
    if powerbi_output_dir is None:
        raise SystemExit("Power BI output dir could not be resolved.")

    print_resolution_summary(env, args.mode)

    if args.mode == "manual_exports":
        run_manual_exports_pipeline(env, powerbi_output_dir, args.powerbi_format)
    else:
        run_validation_fixture_pipeline(env, powerbi_output_dir, args.powerbi_format)

    print("\nPipeline operacional finalizado com sucesso.")


if __name__ == "__main__":
    main()
