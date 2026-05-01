from __future__ import annotations

from datetime import datetime
import json
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.config import get_settings
from linkedin_career_intelligence.duckdb_utils import write_dataframe_to_bronze


def ensure_output_directories(project_root: Path) -> dict[str, Path]:
    inventory_dir = project_root / "data" / "bronze" / "file_inventory"
    inventory_dir.mkdir(parents=True, exist_ok=True)

    return {
        "inventory_dir": inventory_dir
    }


def get_export_sources() -> list[dict[str, Path | str]]:
    settings = get_settings()
    return [
        {
            "export_name": "Basic_LinkedInDataExport_04-04-2026",
            "export_type": "basic",
            "base_path": settings.export_dir("basic"),
        },
        {
            "export_name": "Complete_LinkedInDataExport_04-05-2026",
            "export_type": "complete",
            "base_path": settings.export_dir("complete"),
        },
    ]


def detect_folder_name(csv_path: Path, base_path: Path) -> str:
    relative_path = csv_path.relative_to(base_path)
    parts = relative_path.parts

    if len(parts) == 1:
        return "root"

    return parts[0]


def safe_read_csv_metadata(csv_path: Path) -> dict:
    """
    Tenta ler o CSV com abordagens seguras para capturar:
    - quantidade de linhas
    - quantidade de colunas
    - nomes das colunas
    - encoding aproximado
    - sucesso ou erro de leitura
    """
    file_name = csv_path.name.lower()
    last_error = "Unknown CSV read error"

    if file_name == "connections.csv":
        attempts = [
            {"encoding": "utf-8", "sep": ",", "skiprows": 3},
            {"encoding": "utf-8-sig", "sep": ",", "skiprows": 3},
            {"encoding": "latin-1", "sep": ",", "skiprows": 3},
        ]
    else:
        attempts = [
            {"encoding": "utf-8", "sep": ","},
            {"encoding": "utf-8-sig", "sep": ","},
            {"encoding": "latin-1", "sep": ","},
        ]

    for attempt in attempts:
        try:
            df = pd.read_csv(
                csv_path,
                encoding=attempt["encoding"],
                sep=attempt["sep"],
                skiprows=attempt.get("skiprows", 0),
                low_memory=False,
            )

            return {
                "read_success": True,
                "detected_encoding": attempt["encoding"],
                "row_count": int(df.shape[0]),
                "column_count": int(df.shape[1]),
                "column_names": json.dumps(df.columns.tolist(), ensure_ascii=False),
                "error_message": None,
            }

        except Exception as e:
            last_error = str(e)

    if file_name == "searchqueries.csv":
        for encoding in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                with csv_path.open("r", encoding=encoding, errors="strict") as fh:
                    lines = [line.rstrip("\n\r") for line in fh if line.strip()]

                if not lines:
                    return {
                        "read_success": True,
                        "detected_encoding": encoding,
                        "row_count": 0,
                        "column_count": 0,
                        "column_names": json.dumps([], ensure_ascii=False),
                        "error_message": None,
                    }

                header = lines[0].split(",", 1)
                row_count = max(len(lines) - 1, 0)

                return {
                    "read_success": True,
                    "detected_encoding": encoding,
                    "row_count": row_count,
                    "column_count": len(header),
                    "column_names": json.dumps(header, ensure_ascii=False),
                    "error_message": None,
                }
            except Exception as e:
                last_error = str(e)

    return {
        "read_success": False,
        "detected_encoding": None,
        "row_count": None,
        "column_count": None,
        "column_names": None,
        "error_message": last_error,
    }


def build_inventory(project_root: Path) -> pd.DataFrame:
    export_sources = get_export_sources()
    inventory_rows = []
    inventory_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for source in export_sources:
        export_name = source["export_name"]
        export_type = source["export_type"]
        base_path = source["base_path"]

        print(f"\nIniciando varredura do export: {export_name}")
        print(f"Caminho: {base_path}")

        if not base_path.exists():
            print(f"AVISO: caminho não encontrado: {base_path}")
            inventory_rows.append({
                "export_name": export_name,
                "export_type": export_type,
                "relative_path": None,
                "file_name": None,
                "file_stem": None,
                "folder_name": None,
                "file_size_bytes": None,
                "file_size_kb": None,
                "row_count": None,
                "column_count": None,
                "column_names": None,
                "detected_encoding": None,
                "read_success": False,
                "error_message": f"Caminho não encontrado: {base_path}",
                "inventory_timestamp": inventory_timestamp,
            })
            continue

        csv_files = list(base_path.rglob("*.csv"))

        print(f"Quantidade de CSVs encontrados: {len(csv_files)}")

        for csv_path in csv_files:
            relative_path = csv_path.relative_to(base_path)
            folder_name = detect_folder_name(csv_path, base_path)
            file_size_bytes = csv_path.stat().st_size
            file_size_kb = round(file_size_bytes / 1024, 2)

            metadata = safe_read_csv_metadata(csv_path)

            inventory_rows.append({
                "export_name": export_name,
                "export_type": export_type,
                "relative_path": str(relative_path).replace("\\", "/"),
                "file_name": csv_path.name,
                "file_stem": csv_path.stem,
                "folder_name": folder_name,
                "file_size_bytes": file_size_bytes,
                "file_size_kb": file_size_kb,
                "row_count": metadata["row_count"],
                "column_count": metadata["column_count"],
                "column_names": metadata["column_names"],
                "detected_encoding": metadata["detected_encoding"],
                "read_success": metadata["read_success"],
                "error_message": metadata["error_message"],
                "inventory_timestamp": inventory_timestamp,
            })

    df_inventory = pd.DataFrame(inventory_rows)

    if not df_inventory.empty:
        df_inventory = df_inventory.sort_values(
            by=["export_type", "folder_name", "file_name"],
            ascending=[True, True, True]
        ).reset_index(drop=True)

    return df_inventory


def save_inventory_files(df_inventory: pd.DataFrame, output_paths: dict[str, Path]) -> dict[str, Path]:
    inventory_dir = output_paths["inventory_dir"]
    timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_output = inventory_dir / f"linkedin_file_inventory_{timestamp_suffix}.csv"
    parquet_output = inventory_dir / f"linkedin_file_inventory_{timestamp_suffix}.parquet"
    latest_csv_output = inventory_dir / "linkedin_file_inventory_latest.csv"
    latest_parquet_output = inventory_dir / "linkedin_file_inventory_latest.parquet"

    df_inventory.to_csv(csv_output, index=False, encoding="utf-8-sig")
    df_inventory.to_csv(latest_csv_output, index=False, encoding="utf-8-sig")

    df_inventory.to_parquet(parquet_output, index=False)
    df_inventory.to_parquet(latest_parquet_output, index=False)

    return {
        "csv_output": csv_output,
        "parquet_output": parquet_output,
        "latest_csv_output": latest_csv_output,
        "latest_parquet_output": latest_parquet_output,
    }


def save_inventory_to_duckdb(df_inventory: pd.DataFrame) -> Path:
    db_path = get_settings().db_path
    write_dataframe_to_bronze(df_inventory, "file_inventory")
    return db_path


def print_summary(df_inventory: pd.DataFrame, db_path: Path, saved_files: dict[str, Path]) -> None:
    print("\n" + "=" * 80)
    print("RESUMO DO INVENTÁRIO")
    print("=" * 80)

    print(f"Total de registros no inventário: {len(df_inventory)}")

    if not df_inventory.empty:
        successful_reads = int(df_inventory["read_success"].fillna(False).sum())
        failed_reads = int((~df_inventory["read_success"].fillna(False)).sum())

        print(f"Leituras com sucesso: {successful_reads}")
        print(f"Leituras com erro: {failed_reads}")

        print("\nArquivos por tipo de export:")
        print(df_inventory.groupby("export_type")["file_name"].count())

        print("\nArquivos por pasta lógica:")
        print(df_inventory.groupby("folder_name")["file_name"].count().sort_values(ascending=False))

    print("\nArquivos salvos:")
    for label, path in saved_files.items():
        print(f"- {label}: {path}")

    print(f"\nTabela carregada no DuckDB: bronze.file_inventory")
    print(f"Banco: {db_path}")
    print("=" * 80)


def main() -> None:
    project_root = get_settings().project_root
    output_paths = ensure_output_directories(project_root)

    df_inventory = build_inventory(project_root)

    if df_inventory.empty:
        print("Nenhum dado foi inventariado. Verifique os caminhos dos exports.")
        return

    saved_files = save_inventory_files(df_inventory, output_paths)
    db_path = save_inventory_to_duckdb(df_inventory)
    print_summary(df_inventory, db_path, saved_files)


if __name__ == "__main__":
    main()
