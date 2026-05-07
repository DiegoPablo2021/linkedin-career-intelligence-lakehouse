# LinkedIn Career Intelligence Lakehouse

![Python](https://img.shields.io/badge/Python-ETL%20%26%20contracts-3776AB?style=flat-square&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-local%20analytics%20warehouse-FECD45?style=flat-square&logo=duckdb&logoColor=black)
![dbt](https://img.shields.io/badge/dbt-semantic%20marts-FF694B?style=flat-square&logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-executive%20dashboard-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![Snapshots](https://img.shields.io/badge/Historical%20Snapshots-governed%20layer-0F766E?style=flat-square)
![Observability](https://img.shields.io/badge/Observability-pipeline%20%26%20data%20quality-111827?style=flat-square)

An enterprise-style analytics case study that transforms LinkedIn personal exports into a governed decision-support platform. The repository combines ingestion engineering, semantic modeling, historical snapshots, observability metrics, and a premium Power BI experience built with DAX-driven HTML/CSS layouts.

<p align="center">
  <img src="docs/images/linkedin-lakehouse-end-to-end.svg" alt="LinkedIn Career Intelligence Lakehouse architecture" width="100%" />
</p>

## Executive Project Overview

Career data is usually fragmented, static, and difficult to analyze over time. LinkedIn exports contain valuable signals about professional trajectory, network growth, applications, invitations, events, recommendations, and platform activity, but they are not structured for executive analytics.

This project solves that problem by turning raw LinkedIn exports into a reproducible analytics platform with:

- governed ingestion contracts
- a local analytical warehouse
- dbt-modeled marts
- historical snapshot facts
- data quality and observability telemetry
- an executive Power BI layer with custom HTML-based pages

The result is not a generic dashboard. It is a portfolio-grade analytics platform focused on:

- career intelligence
- professional presence and reputation
- historical trend analysis
- pipeline reliability
- governed semantic modeling

## Índice Da Documentação

Use estes documentos como referência canônica para operação, arquitetura e apresentação:

- [Workflows do GitHub Actions](docs/GITHUB_ACTIONS_WORKFLOWS.md)
- [Camada Power BI](docs/POWERBI_LAYER.md)

Notas operacionais e de apoio permanecem apenas locais em `private_docs/`.

### Why historical snapshots matter

Current-state metrics are useful but incomplete. Executive analytics needs historical context. Snapshot facts add that context by preserving monthly evolution across networking, applications, presence, career progression, and data quality, without relying on external APIs or fabricated history.

### Why governance and observability matter

The project treats personal analytics as a production-grade data problem. That means the platform must explain:

- what was loaded
- what changed during transformation
- how quality behaved over time
- whether the pipeline is operationally trustworthy

## Enterprise Architecture Overview

The stack is intentionally layered to separate responsibilities and keep the Power BI layer focused on semantic logic and UX instead of raw data wrangling.

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Python, Pandas | Reads LinkedIn exports, applies contracts, validates schemas, writes audited bronze tables |
| Storage | DuckDB | Lightweight analytical warehouse for local development and reproducible execution |
| Transformation | dbt, SQL | Standardizes staging, enriches intermediate models, materializes business marts |
| Historical Layer | Python, DuckDB | Builds governed snapshot facts from real event history and pipeline audit timelines |
| Semantic Layer | Power BI, DAX | Defines business measures, relationships, folders, and executive KPIs |
| Presentation Layer | Power BI HTML Content, HTML/CSS | Renders premium executive pages with narrative blocks, KPI systems, and compact layouts |

### Core technologies

- `Python`: ingestion orchestration, warehouse utilities, snapshot generation, exports
- `Pandas`: ingestion shaping and file-level validation logic
- `DuckDB`: local warehouse for bronze, staging-friendly sources, marts, and snapshots
- `dbt`: transformation, tests, reusable analytical marts
- `SQL`: staging, enrichment, summary modeling, observability timelines
- `Power BI`: semantic model, DAX measures, executive reporting
- `DAX`: KPIs, observability logic, historical measures, HTML page rendering
- `HTML/CSS inside Power BI`: executive UX layer for premium portfolio-grade pages

## Solution Architecture Flow

```text
LinkedIn Export Files
-> Python Ingestion + Contracts
-> bronze.* in DuckDB
-> dbt staging / intermediate / marts
-> Historical Snapshot Builder
-> Power BI export layer
-> Semantic model + DAX
-> HTML/CSS executive dashboards
```

### Operational flow

1. Python ingests LinkedIn export files into `bronze`, enforcing explicit contracts.
2. Each load records audit metadata in `bronze.ingestion_audit`.
3. dbt materializes staging, intermediate, and mart models for analytics consumption.
4. Snapshot scripts build governed historical facts from real monthly aggregates and real observability records.
5. Export scripts generate Power BI-ready datasets.
6. Power BI consumes the semantic layer and renders executive pages, including premium HTML/CSS layouts.

## Main Dashboard Pages

### Executive Overview

Purpose:
- provide a high-level view of professional trajectory, network, applications, presence, and pipeline health

Core KPIs:
- connections
- applications
- recommendations
- historical snapshot counts
- refresh metadata
- pipeline reliability signals

Analytical value:
- combines current-state and historical indicators into a single executive landing page

Executive insight:
- answers whether the professional profile is growing, visible, and operationally well governed

### Networking Intelligence

Purpose:
- analyze professional network scale, reach, growth, and relationship quality

Core KPIs:
- total connections
- connections with email
- coverage rates
- invitations
- recommendations
- network growth signals

Analytical value:
- measures both volume and relationship traceability

Executive insight:
- turns network data into a maturity signal instead of a simple contact count

### Career & Education Intelligence

Purpose:
- evaluate professional evolution, learning path, certifications, and trajectory depth

Core KPIs:
- positions started
- current positions
- average position duration
- education started
- certifications
- trajectory maturity indicators

Analytical value:
- combines career progression with structured learning signals

Executive insight:
- shows whether the profile reflects continuity, depth, and technical credibility

### Applications & Presence Intelligence

Purpose:
- monitor applications, invitations, recommendations, events, and visibility signals

Core KPIs:
- total applications
- applications momentum
- events
- invitations
- recommendations
- engagement and presence scores

Analytical value:
- merges opportunity pipeline with professional exposure and reputation evidence

Executive insight:
- reveals whether professional activity is visible, consistent, and opportunity-oriented

### Pipeline & Governance Intelligence

Purpose:
- assess operational reliability, inventory footprint, governance, and analytical readiness

Core KPIs:
- successful reads
- failed reads
- read success and failure rates
- data quality score
- inventory files, rows, and size
- freshness

Analytical value:
- shifts the narrative from dashboard output to data platform trust

Executive insight:
- demonstrates whether the reporting layer is supported by a reliable and auditable pipeline

### Pipeline & Data Quality Observability

Purpose:
- monitor the operational health of ingestion, retention, null rate behavior, and alert conditions

Core KPIs:
- health score
- monitored loads
- row retention
- source vs transformed rows
- average and max null rate
- duplicate and removal alerts

Analytical value:
- provides observability-grade visibility into data quality and pipeline behavior over time

Executive insight:
- makes it possible to explain trust, degradation, and readiness with evidence

## Historical Snapshot Strategy

The snapshot layer exists to enrich executive analytics with governed historical behavior without introducing synthetic noise.

### Design principles

- use real monthly aggregates whenever available
- derive cumulative series only from real events
- preserve actual totals as the authoritative endpoint
- mark every snapshot row with methodology and provenance

### Phase 1 scope

Implemented snapshot facts:

- `fact_snapshot_network_growth`
- `fact_snapshot_applications`
- `fact_snapshot_presence`
- `fact_snapshot_career_education`
- `fact_snapshot_data_quality`
- `dim_snapshot_method`
- `dim_snapshot_run`

Phase 1 intentionally excludes synthetic backfill. All current rows are actual or cumulatively derived from real historical events.

### Governance fields

Snapshot facts include:

- `snapshot_date`
- `snapshot_method`
- `snapshot_source`
- `snapshot_run_id`
- `snapshot_created_at`
- `is_simulated_snapshot`

## Observability & Governance

This repository treats observability as a first-class analytical product.

### Pipeline health

The pipeline records:

- successful and failed reads
- execution freshness
- inventory footprint
- data quality score

### Data quality monitoring

The ingestion audit captures:

- source row count
- source column count
- transformed row count
- transformed column count
- rows removed during transform
- duplicate rows after transform
- non-empty columns
- unique columns
- null rate by column before and after transformation

### Operational readiness

dbt marts and Power BI measures turn these logs into readiness signals, alert counts, retention metrics, and historical trends.

### Governance value

The governance layer improves:

- auditability
- troubleshooting
- semantic trust
- pipeline explainability
- executive confidence in reported metrics

## Power BI HTML/CSS Layer

The Power BI report is not built only with native cards and charts. It also includes a custom HTML/CSS rendering layer driven by DAX measures.

This approach is used to:

- create premium executive headers
- build visually consistent KPI systems
- render narrative insight blocks
- control responsive compaction inside fixed Power BI canvases
- improve storytelling beyond standard Power BI visual defaults

For the technical implementation details, see [docs/POWERBI_LAYER.md](docs/POWERBI_LAYER.md).

## GitHub Actions Operating Model

The repository now uses a split workflow model:

- `CI` runs only on `push` and `pull_request`
- `Operational Pipeline` runs only by `workflow_dispatch`
- raw LinkedIn export ingestion remains manual
- all downstream processing stays automated after the exports are placed in the repository workspace

### Manual export drop zone

Place new LinkedIn exports directly under `data/raw` using this naming convention:

```text
data/raw/
├─ basic_export_2026_05_07/
│  ├─ Profile.csv
│  ├─ Positions.csv
│  └─ ...
└─ complete_export_2026_05_07/
   ├─ Connections.csv
   ├─ Jobs/Job Applications.csv
   └─ ...
```

The pipeline scans `data/raw`, detects the latest valid directory for each export type automatically, and uses fallback discovery if one type is temporarily absent. You can still override both paths with `LINKEDIN_BASIC_EXPORT_DIR` and `LINKEDIN_COMPLETE_EXPORT_DIR`.

### Workflow responsibilities

- `CI`: tests, Python compile checks, SQL lint, and `dbt build` against a synthetic validation warehouse
- `Operational Pipeline`: ingestion, inventory refresh, DuckDB refresh, dbt marts, Power BI exports, historical snapshots, and observability validation

Detailed workflow documentation lives in [docs/GITHUB_ACTIONS_WORKFLOWS.md](docs/GITHUB_ACTIONS_WORKFLOWS.md).

## Fluxo Mensal De Refresh Do LinkedIn

Este projeto foi desenhado para operação mensal semi-automatizada. O export do LinkedIn continua manual e tudo depois disso é automatizado.

### Fluxo operacional passo a passo

1. Exporte os dados do LinkedIn manualmente.
2. Extraia os arquivos exportados para `data/raw/basic_export_YYYY_MM_DD` e `data/raw/complete_export_YYYY_MM_DD`.
3. Dispare o GitHub Actions `Operational Pipeline` com `workflow_dispatch` ou execute `python scripts\run_pipeline.py --mode manual_exports` localmente.
4. O pipeline resolve automaticamente as pastas válidas mais recentes.
5. A ingestion bronze atualiza o warehouse DuckDB.
6. O dbt reconstrói staging, intermediate models e marts.
7. Os exports do Power BI são regenerados em um diretório de saída consistente.
8. Os historical snapshots são reconstruídos e validados.
9. A validação final de observability e de outputs confirma que o refresh foi concluído com sucesso.

### Notas operacionais

- Se um tipo de export estiver ausente, o resolver tenta uma pasta de fallback compatível que ainda contenha os arquivos necessários.
- Se nenhuma pasta compatível existir, o pipeline falha cedo com uma mensagem clara.
- Os exports operacionais locais usam `powerbi/exports` por padrão.
- O GitHub Actions pode publicar artifacts em um diretório separado, como `artifacts/powerbi_exports`.

## Key Technical Highlights

- governed Python ingestion with explicit schema contracts
- DuckDB warehouse optimized for local reproducibility
- dbt-driven marts for analytics and observability
- historical snapshot architecture without API dependency
- Power BI semantic model with dedicated historical snapshot measures
- observability facts built from ingestion audit timelines
- premium HTML/CSS pages rendered from DAX
- compact UX optimization for Power BI HTML Content
- executive narrative cards combined with operational metrics

## Repository Structure

```text
linkedin-career-intelligence-lakehouse/
├─ apps/                              # Streamlit app layer and supporting pages
├─ data/
│  ├─ raw/                            # Private LinkedIn exports
│  └─ bronze/file_inventory/          # File inventory snapshots
├─ demo/                              # Sanitized public demo database
├─ docs/
│  ├─ images/                         # Architecture and dashboard visuals
│  ├─ GITHUB_ACTIONS_WORKFLOWS.md     # CI and operational workflow documentation
│  └─ POWERBI_LAYER.md                # Technical Power BI documentation
├─ linkedin_career_intelligence/      # Python package: config, ingestion, contracts, DuckDB helpers
├─ linkedin_career_intelligence_dbt/
│  └─ models/
│     ├─ staging/                     # Standardized SQL layer
│     ├─ intermediate/                # Reusable enriched models
│     └─ marts/core/                  # Final analytics and observability marts
├─ powerbi/
│  ├─ exports/                        # Power BI-ready CSV/Parquet exports
│  ├─ *.pbix                          # Power BI Desktop model/report
│  ├─ *.html                          # HTML layout references
│  └─ observability_measures.dax      # DAX package for observability
├─ profiles/                          # Local dbt profile
├─ scripts/
│  ├─ ingestion/                      # Table-level ingestion utilities
│  ├─ profiling/                      # Export inventory and profiling
│  ├─ snapshots/                      # Historical snapshot generation
│  ├─ powerbi/                        # Semantic model + HTML measure automation
│  ├─ utils/                          # Exports, warehouse inspection, validation
│  ├─ run_ingestion.py
│  ├─ run_pipeline.py
│  └─ run_validation.py
├─ tests/                             # Python validation layer
├─ warehouse/                         # Local DuckDB warehouse location
└─ README.md
```

## Screenshots

Architecture:

<p align="center">
  <img src="docs/images/linkedin-lakehouse-end-to-end.svg" alt="End-to-end architecture" width="100%" />
</p>

Application and analytical storytelling:

<p align="center">
  <img src="docs/images/app-home-highlight.svg" alt="Executive application home" width="100%" />
</p>

<p align="center">
  <img src="docs/images/app-profile-highlight.svg" alt="Profile and executive narrative layer" width="100%" />
</p>

<p align="center">
  <img src="docs/images/app-health-highlight.svg" alt="Health and observability view" width="100%" />
</p>

## Technical Learnings

### Architectural decisions

- separating ingestion, transformation, semantic modeling, and presentation reduces technical debt
- keeping Power BI focused on business logic and UX produces a cleaner semantic layer
- using DuckDB and dbt together creates a strong local analytics platform without unnecessary infrastructure overhead

### Performance considerations

- heavy shaping stays outside Power BI
- HTML measures are compacted to fit fixed Power BI containers
- snapshot facts are intentionally narrow and governed to avoid unnecessary bloat

### Snapshot challenges

- historical depth is only as rich as the available event history
- cumulative snapshots must remain traceable and deterministic
- simulated backfill must be explicitly separated from actual history

### Power BI limitations and workarounds

- HTML Content does not support JavaScript
- interactive behavior must come from slicers, relationships, and DAX context
- responsive layout inside DAX-rendered HTML requires strict control over padding, min-height, gap, and overflow

### HTML/CSS inside DAX

- premium design in Power BI is possible, but maintainability depends on reusable visual patterns
- compact executive layouts require repeated fit adjustments to avoid internal scrollbars
- narrative cards are useful, but they must be dimensioned carefully to coexist with KPIs in a fixed viewport

## Future Improvements

- orchestrate the full pipeline with a scheduler or workflow engine
- add CI/CD for semantic layer validation and documentation linting
- extend snapshot strategy with governed simulated backfill when justified
- strengthen semantic model automation and deployment workflows
- add optional API-based enrichment for near-real-time profile signals
- explore cloud deployment for warehouse, dbt execution, and dashboard distribution

## Local Execution

Install and run locally:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts\run_validation.py
python scripts\run_pipeline.py --mode manual_exports
```

Validate the operational workflow locally without private exports:

```powershell
python scripts\run_pipeline.py `
  --mode validation_fixture `
  --db-path tmp/local-validation.duckdb `
  --powerbi-output-dir tmp/powerbi_exports `
  --powerbi-format both
```

## Author

**Diego Pablo**

- GitHub: <https://github.com/DiegoPablo2021/>
- Portfolio: <https://diego-pablo.vercel.app/>
- LinkedIn: <https://www.linkedin.com/in/diego-pablo/>
- Email: <diegopmenezes@hotmail.com>
