from pathlib import Path
import sys
import duckdb

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from linkedin_career_intelligence.config import get_settings

db_path = get_settings().db_path

conn = duckdb.connect(str(db_path))

df = conn.execute("""
    SELECT *
    FROM bronze.languages
    LIMIT 20
""").fetchdf()

print(df)
print("\nTotal de registros:")
print(conn.execute("SELECT COUNT(*) AS total FROM bronze.languages").fetchdf())

conn.close()
