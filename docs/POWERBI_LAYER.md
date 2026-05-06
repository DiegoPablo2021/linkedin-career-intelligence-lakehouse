# Power BI Layer

## Purpose

This document explains the technical design of the Power BI layer that sits on top of the LinkedIn Career Intelligence Lakehouse. It focuses on semantic modeling, DAX strategy, HTML/CSS rendering, observability integration, responsive layout decisions, and the tradeoffs required to build an executive-grade report inside Power BI Desktop.

The goal of this layer is not to compensate for weak upstream data modeling. The goal is to expose a governed, performant, and recruiter-grade analytics experience on top of a properly engineered warehouse and dbt mart structure.

## 1. Power BI Layer Architecture

The Power BI layer consumes curated exports derived from DuckDB and dbt, not raw LinkedIn files.

```text
DuckDB + dbt marts
-> Power BI export layer
-> semantic model
-> DAX measures
-> HTML/CSS rendering layer
-> executive pages
```

### Architectural principles

- keep ingestion and transformation outside Power BI
- keep the semantic model readable and intentionally scoped
- centralize business logic in `_Measures`
- isolate historical behavior in snapshot facts
- isolate observability and governance in dedicated facts and folders
- use HTML Content for layout control, not for business logic

### Why this architecture was chosen

Power BI performs best when it consumes stable analytical entities. Using curated marts and snapshot facts reduces:

- model fragility
- duplicated transformation logic
- debugging complexity
- report maintenance overhead

It also improves:

- semantic clarity
- measure reusability
- portability of the report
- confidence in KPI definitions

## 2. Measure Folder Organization

The project uses a centralized `_Measures` table and separates concerns through display folders.

### Current relevant folders

| Folder | Purpose |
|---|---|
| `02 | Networking` | networking and relationship logic |
| `14 | HTML Content` | page-rendering measures that output HTML/CSS |
| `15 | Observability` | pipeline health and data quality measures |
| `16 | Historical Snapshots` | historical and snapshot-driven measures |

### Why folders matter

Folder organization is not cosmetic. It helps:

- keep semantic intent visible
- separate UX measures from analytical measures
- reduce ambiguity during maintenance
- support enterprise-style review and onboarding

## 3. HTML Measure Rendering Strategy

The report uses DAX measures that return HTML strings for the Power BI `HTML Content` visual.

### Rendering model

Each HTML page is a single measure that:

1. resolves analytical measures into formatted variables
2. composes an HTML layout string
3. embeds CSS inline for portability
4. rerenders under Power BI filter context

### Why HTML measures were used

Native visuals are strong for standard analysis, but they limit high-end layout control. The HTML layer was introduced to provide:

- premium executive headers
- consistent page-level design systems
- narrative intelligence cards
- richer visual hierarchy
- tighter control of spacing and composition

### What HTML measures do not do

- they do not replace semantic modeling
- they do not introduce independent interaction logic
- they do not perform computation that should live in analytical measures
- they do not use JavaScript

The correct pattern is:

```text
DAX measures compute metrics
-> HTML measure formats and arranges them
-> slicers and relationships drive the context
```

## 4. CSS Layout Decisions

All HTML pages were designed for a constrained Power BI viewport rather than an unconstrained web page.

### Core wrapper strategy

The root wrapper pattern is standardized around:

```css
width: 100%;
height: 100%;
max-height: 100%;
overflow: hidden;
box-sizing: border-box;
```

### Why this matters

The Power BI HTML visual has a fixed container. If the HTML behaves like a normal web page, the result is:

- vertical scrollbars
- clipped headers
- inconsistent visual density
- broken alignment between pages

The wrapper strategy reduces those risks and keeps each page aligned with the report canvas.

### Design system

The approved Power BI design system is:

- background: `#0b1220`
- primary cards: `#111827`
- borders: `#1f2937`
- primary text: `#f9fafb`
- secondary text: `#94a3b8`
- blue accent: `#2563eb`
- green success: `#10b981`
- amber warning: `#f59e0b`
- red critical: `#ef4444`

### Why inline CSS was kept

Inline CSS makes the HTML measures:

- self-contained
- portable inside the semantic model
- easier to deploy through TOM/XMLA measure updates

The tradeoff is maintainability. To offset that, visual patterns were repeated consistently across measures.

## 5. Responsive Compaction Strategy

The most difficult UX problem was not styling. It was fit.

Power BI HTML pages need to remain readable inside a fixed visual height while avoiding internal scrollbars.

### Compaction approach

The final strategy relies on controlled micro-adjustments rather than redesign:

- reduce wrapper padding by a few pixels
- reduce hero height only when necessary
- reduce vertical gaps selectively
- compact bottom narrative cards before touching KPI typography
- avoid exaggerated `min-height`
- keep `overflow:hidden` in all main wrappers

### Why this approach was chosen

The pages serve an executive audience. Aggressive scaling would damage visual credibility. The compaction strategy therefore favors:

- preserving hierarchy
- preserving visual identity
- preserving KPI prominence
- trimming only the residual non-essential whitespace

### Tradeoff

This approach is slower than redesigning the pages, but it preserves the approved visual system and avoids rework across the report.

## 6. Executive UX Decisions

The Power BI layer was designed as an enterprise analytics product, not as a default report.

### UX patterns used

- hero sections with strong visual context
- compact KPI grids
- analytical blocks with explicit purpose
- narrative cards for interpretation
- governance and observability badges
- executive microcopy

### Why these choices matter

Executive dashboards are not only about showing numbers. They must:

- establish context immediately
- reduce interpretation time
- communicate trust
- differentiate insight from raw output

### Portfolio implications

This matters for technical audiences because it demonstrates:

- semantic discipline
- product thinking
- front-end judgment inside Power BI constraints
- senior-level control over analytics UX

## 7. Snapshot Measures

Historical measures were added under `16 | Historical Snapshots` to enrich the semantic model with trend-aware signals.

### Examples

- `Snapshot Connections`
- `Snapshot Applications`
- `Snapshot Events`
- `Snapshot Invitations`
- `Snapshot Recommendations`
- `Snapshot Career Maturity Score %`
- `Snapshot Presence Score %`
- `Snapshot Engagement Score %`
- `Snapshot Data Quality Rows`
- `Snapshot Avg Null Rate %`
- `Snapshot Max Null Rate %`
- `Snapshot Data Quality Alerts`
- `Snapshot Read Success Rate %`
- `Snapshot Row Retention Rate %`
- `Historical Snapshot Count`
- `Actual Snapshot Count`
- `Simulated Snapshot Count`

### Design philosophy

Historical measures were created to:

- compare current and historical behavior
- enrich page narratives without depending on external APIs
- preserve governed provenance through snapshot metadata

### Tradeoff

Snapshot measures improve insight density but increase semantic complexity. That is why they were isolated into their own folder and kept separate from operational observability measures.

## 8. Observability Measures

Observability measures live under `15 | Observability` and focus on the health of ingestion and data quality.

### Core families

- load counts
- success and failure rates
- retention rates
- row removal indicators
- duplicate alert indicators
- null rate indicators
- freshness and latest load indicators
- composite health scores

### Why this layer exists

Without observability, the dashboard can display polished KPIs while hiding operational fragility. This layer turns the report into a platform that can defend its own reliability.

### Scalability concern

As observability density grows, the risk is turning a semantic model into an operations console. The separation between governance and observability pages keeps that scope manageable.

## 9. Governance Measures

Governance measures are designed to express analytical trust, not only technical status.

### Typical governance topics

- read success and failure
- inventory footprint
- asset freshness
- quality readiness
- traceability signals
- auditability indicators

### Why governance is separate from observability

Observability answers:

- what happened in the pipeline
- where did quality degrade

Governance answers:

- can this data be trusted for executive consumption
- is the analytical layer operationally ready

That separation improves both storytelling and maintainability.

## 10. Performance Optimization Decisions

The Power BI layer deliberately avoids using HTML as a computation engine.

### Performance rules followed

- compute metrics in dedicated DAX measures first
- format only the final values inside HTML measures
- keep relationship design simple and one-directional
- avoid fact-to-fact relationships
- consume curated exports instead of raw or excessively wide sources

### Why this matters

HTML measures are expensive to maintain if they also become logic containers. Keeping logic upstream or in analytical measures reduces:

- debugging time
- recomputation ambiguity
- performance regressions under slicer changes

### Related semantic optimizations

- dedicated date dimension
- historical snapshot facts separated by subject area
- helper dimensions for observability filters
- measures centralized in `_Measures`

## 11. DAX Design Philosophy

The DAX layer follows a production-minded philosophy.

### Principles

- keep measures explicit
- use `COALESCE` to avoid unnecessary blanks in executive views
- separate formatting from arithmetic when practical
- create synthetic scores only when the logic is explainable
- prefer semantic clarity over cleverness

### Why these principles were chosen

Executive reporting requires trust more than novelty. Measures should be:

- inspectable
- testable
- maintainable
- reusable across pages

### Tradeoff

Some visual formulas inside HTML measures compute lightweight display scores inline. This is acceptable for UX signals, but the stronger long-term pattern is to progressively externalize reusable scoring logic into analytical measures when reuse increases.

## 12. Reusable HTML Block Patterns

Although measures are page-specific, they reuse stable UI patterns.

### Common patterns

- hero header with contextual badges
- KPI grid card
- analytical block card
- narrative / insight card
- governance status chip
- compact metric row

### Why reuse matters

Reusable structure keeps the report:

- visually coherent
- easier to compact
- easier to review
- easier to extend page by page

### Maintainability implication

The more these patterns stay consistent, the easier it becomes to automate updates through measure scripts without visual drift.

## 13. Limitations and Workarounds

### HTML Content limitations

- no JavaScript
- no native internal drill behavior
- no arbitrary DOM interaction
- viewport is constrained by the Power BI visual container

### Workarounds used

- external slicers drive context
- DAX variables drive dynamic text and KPI rendering
- compact layouts prevent container scroll
- responsive compaction is done measure-by-measure

### Semantic limitations

- snapshots are only as complete as the underlying event history
- observability timelines depend on pipeline audit richness
- the report favors governed executive storytelling over free-form exploration

## 14. Future Evolution Ideas

- extract common HTML block templates into safer generation utilities
- add automated semantic validation for HTML measure fit
- formalize measure linting for folder, description, and formatting compliance
- extend historical pages to consume more snapshot trends directly
- improve deployment automation for TOM/XMLA updates
- add release governance for report UX regression checks

## Final Assessment

The Power BI layer is intentionally positioned as the final semantic and UX layer of an enterprise-style analytics system.

Its value comes from the combination of:

- governed upstream data
- a controlled semantic model
- premium executive presentation
- historical intelligence
- operational observability

That combination is what makes the report stronger than a standard dashboard and closer to a production-grade analytics product.
