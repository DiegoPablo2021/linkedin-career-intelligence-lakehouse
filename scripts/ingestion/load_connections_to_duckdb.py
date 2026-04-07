from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.ingestion import load_table


if __name__ == "__main__":
    df = load_table("connections")
    print(f"bronze.connections carregada com {len(df)} linhas.")

