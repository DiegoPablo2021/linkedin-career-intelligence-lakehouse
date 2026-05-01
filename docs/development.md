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
- Keep ingestion metadata in code contracts instead of espalhar regras implicitamente entre scripts
- Prefer tests for parsing and cleaning rules before expanding marts

## Ingestion Standards

- New ingestion tables must be registered in `linkedin_career_intelligence/ingestion.py`
- Every table config must declare a `TableContract` with required columns and governance metadata
- Reuse `write_dataframe` or `write_dataframe_to_bronze` for DuckDB writes; avoid criar conexoes ad hoc
- Single-table loaders should reuse `scripts/ingestion/_cli.py`
- Audit evidence is written to `bronze.ingestion_audit` on every successful load

## Quality Checks

### Python tests

```powershell
python -m pytest -q
```

### dbt validation

```powershell
python -m dbt.cli.main build --project-dir linkedin_career_intelligence_dbt --profiles-dir profiles
```

### Full validation

```powershell
python scripts\run_validation.py
```

O comando completo usa uma base sintética gerada por `scripts/ci/bootstrap_validation_warehouse.py`, o que permite validar Python, SQL e dbt sem depender dos exports privados.

## CI/CD

- workflow central em `.github/workflows/ci.yml`
- runner Linux com cache de dependências Python
- validação unificada via `scripts/run_validation.py`
- warehouse sintético para assegurar reprodutibilidade e governança

## Suggested Future Standards

- Add `ruff` for linting and import hygiene
- Add notebook execution checks in CI when notebooks become part of regular delivery
- Add semantic metrics layer for cross-page KPI reuse
