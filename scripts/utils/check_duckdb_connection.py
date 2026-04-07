from pathlib import Path
import sys

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from linkedin_career_intelligence.config import get_settings
from linkedin_career_intelligence.duckdb_utils import connect_duckdb, ensure_core_schemas


conn = connect_duckdb()
ensure_core_schemas(conn)
conn.close()

print(f"Banco criado/conectado com sucesso em: {get_settings().db_path}")
