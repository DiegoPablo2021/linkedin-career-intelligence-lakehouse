from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.ingestion import load_table


def run_table_loader(table_key: str) -> None:
    df = load_table(table_key)
    print(f"bronze.{table_key} carregada com {len(df)} linhas.")
