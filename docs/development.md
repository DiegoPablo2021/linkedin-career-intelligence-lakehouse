# Development Guide

## Recommended Workflow

1. Run ingestion with `python scripts/run_ingestion.py`
2. Profile exports when needed with `python scripts/profiling/inventory_linkedin_exports.py`
3. Build analytics models with `dbt build --profiles-dir ..\profiles`
4. Explore or validate hypotheses in `notebooks/`
5. Expose approved metrics and narratives in `apps/`

## Project Conventions

- Use notebooks for exploration, not for production ETL
- Promote stable logic from notebooks into `linkedin_career_intelligence/`
- Keep business transformations in dbt when they are analytical and SQL-friendly
- Keep ingestion, parsing, normalization and file-level concerns in Python
- Prefer tests for parsing and cleaning rules before expanding marts

## Quality Checks

### Python tests

```powershell
python -m pytest -q
```

### dbt validation

```powershell
dbt build --profiles-dir ..\profiles
```

Run the dbt command from the `linkedin_career_intelligence_dbt/` folder.

## Suggested Future Standards

- Add `ruff` for linting and import hygiene
- Add notebook execution checks in CI when notebooks become part of regular delivery
- Add semantic metrics layer for cross-page KPI reuse

