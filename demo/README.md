## Public Demo Database

This folder stores the sanitized DuckDB file used for public portfolio and Streamlit deployments.

Security model:
- the private source database stays in `warehouse/` and remains ignored by git
- the public database is regenerated into this folder from the local private warehouse
- names, contact channels, profile URLs and free-text narrative fields are anonymized or replaced

Regenerate the demo file locally with:

```powershell
$env:PYTHONPATH='.'
.\.venv\Scripts\python.exe scripts\utils\create_public_demo_db.py
```

On public deploys, the app automatically falls back to `demo/linkedin_career_intelligence_demo.duckdb` when the private warehouse database is not available.
