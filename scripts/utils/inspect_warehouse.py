from __future__ import annotations

from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "warehouse" / "linkedin_career_intelligence.duckdb"


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        print(f"Database: {DB_PATH}")
        print()
        rows = con.execute(
            """
            select table_schema, table_name
            from information_schema.tables
            where table_schema in ('raw', 'bronze', 'silver', 'gold', 'main')
            order by table_schema, table_name
            """
        ).fetchall()

        current_schema = None
        for schema_name, table_name in rows:
            if schema_name != current_schema:
                current_schema = schema_name
                print(f"[{schema_name}]")
            count = con.execute(f"select count(*) from {schema_name}.{table_name}").fetchone()[0]
            print(f"- {table_name}: {count} linhas")
            columns = con.execute(f"describe {schema_name}.{table_name}").fetchall()
            column_names = ", ".join(column[0] for column in columns)
            print(f"  colunas: {column_names}")
        if not rows:
            print("Nenhuma tabela encontrada.")
    finally:
        con.close()


if __name__ == "__main__":
    main()
