# GitHub Actions Workflows

## Overview

The repository uses two distinct GitHub Actions workflows:

- `CI`: code quality and build validation on `push` and `pull_request`
- `Operational Pipeline`: semi-automated pipeline execution triggered manually with `workflow_dispatch`

This split keeps software delivery checks separate from data operations.

## CI workflow

File: `.github/workflows/ci.yml`

Trigger model:

- runs on every `push`
- runs on every `pull_request`
- does not run on `workflow_dispatch`

Responsibilities:

- install Python dependencies
- run `pytest`
- compile Python packages as a packaging smoke test
- bootstrap a synthetic DuckDB warehouse
- run `sqlfluff lint`
- run `dbt build`

Purpose:

- validate code quality
- validate the dbt project
- validate repository health without needing private LinkedIn exports

## Operational pipeline workflow

File: `.github/workflows/operational-pipeline.yml`

Trigger model:

- runs only through `workflow_dispatch`

Responsibilities in `manual_exports` mode:

1. read manually placed LinkedIn exports from `data/raw`
2. ingest source files into DuckDB
3. refresh `bronze.file_inventory`
4. rebuild dbt staging, intermediate, and marts
5. rebuild historical snapshots
6. regenerate Power BI exports
7. validate observability outputs
8. upload the generated warehouse and Power BI artifacts

Responsibilities in `validation_fixture` mode:

1. bootstrap a synthetic validation warehouse
2. run `dbt build`
3. rebuild snapshots
4. export Power BI artifacts
5. validate observability outputs
6. upload validation artifacts

Purpose:

- keep export ingestion manual
- automate every downstream processing stage
- support GitHub-hosted validation without exposing private personal exports

## Manual export layout

Recommended structure:

```text
data/raw/
├─ basic_export_2026_05_07/
│  ├─ Profile.csv
│  └─ ...
└─ complete_export_2026_05_07/
   ├─ Connections.csv
   ├─ Jobs/Job Applications.csv
   └─ ...
```

Resolution rules:

- explicit `LINKEDIN_BASIC_EXPORT_DIR` or `LINKEDIN_COMPLETE_EXPORT_DIR` wins
- otherwise the pipeline scans `data/raw`
- the latest valid folder matching `basic_export_YYYY_MM_DD` or `complete_export_YYYY_MM_DD` is selected automatically
- if one type is absent, the resolver tries a compatible fallback directory that still contains the required files
- if no compatible folder exists, the pipeline fails before ingestion starts

## Operational UX

The operational runner is designed to be explicit:

- it prints the resolved export directories before starting ingestion
- it shows whether resolution came from env override, dated discovery, or fallback
- it prints warnings when a fallback path is being used
- it keeps local Power BI outputs in a consistent directory and uploads artifacts in GitHub Actions

## GitHub Actions operational risks

The main operational risk in GitHub-hosted Actions is input availability:

- `manual_exports` requires the export folders to exist in the runner workspace
- GitHub-hosted runners are ephemeral, so monthly real-data operation usually needs a self-hosted runner or a controlled artifact staging step
- `validation_fixture` is safe for workflow validation because it does not require private exports

The workflow is therefore ready for monthly continuous operation as long as the exports are staged intentionally before dispatching `manual_exports`.

## Recommended usage

Local execution with real exports:

```powershell
python scripts\run_pipeline.py --mode manual_exports
```

Local validation without private exports:

```powershell
python scripts\run_pipeline.py `
  --mode validation_fixture `
  --db-path tmp/local-validation.duckdb `
  --powerbi-output-dir tmp/powerbi_exports `
  --powerbi-format both
```

GitHub Actions validation:

- dispatch `Operational Pipeline`
- choose `execution_mode=validation_fixture`
- inspect the uploaded Power BI and DuckDB artifacts

GitHub Actions manual run with real exports:

- use a runner that has the export folders available in the workspace
- dispatch `Operational Pipeline`
- set `execution_mode=manual_exports`
- optionally pass explicit `basic_export_dir` and `complete_export_dir`
