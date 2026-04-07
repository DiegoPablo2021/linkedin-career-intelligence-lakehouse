from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linkedin_career_intelligence.ingestion import load_all_tables


if __name__ == "__main__":
    loaded_rows = load_all_tables()
    print("Carga consolidada finalizada:")
    for table_name, row_count in loaded_rows.items():
        print(f"- {table_name}: {row_count} linhas")
