# Camada Exportável para Power BI

## Objetivo

Criar uma camada intermediária, estável e portátil para consumo no Power BI sem depender diretamente do DuckDB em todos os cenários.

## Quando vale a pena

Vale criar essa camada quando você quiser:

- facilitar a distribuição do `.pbix`
- congelar um schema de consumo
- reduzir acoplamento do Power BI ao banco local
- simplificar refresh e troubleshooting

## Estrutura recomendada

```text
powerbi/
├─ exports/
│  ├─ dim_profile.csv
│  ├─ dim_date.csv
│  ├─ fact_connections_timeline.csv
│  ├─ fact_career_progression.csv
│  ├─ fact_education_timeline.csv
│  ├─ fact_certifications_timeline.csv
│  ├─ fact_skills_summary.csv
│  ├─ fact_learning_summary.csv
│  ├─ fact_job_applications_timeline.csv
│  ├─ fact_recommendations_timeline.csv
│  ├─ fact_endorsements_summary.csv
│  ├─ fact_company_follows.csv
│  ├─ fact_events_summary.csv
│  ├─ fact_invitations_summary.csv
│  ├─ fact_file_inventory.csv
│  └─ fact_pipeline_health.csv
```

## Regra de desenho

Cada arquivo exportado deve:

- ter nome de negócio claro
- conter apenas colunas úteis para o Power BI
- evitar duplicar colunas irrelevantes
- sair já padronizado e pronto para modelagem

## Mapeamento sugerido

## dim_profile.csv

### Origem

- `main.mart_profile_summary`

### Colunas

- `first_name`
- `last_name`
- `headline`
- `industry`
- `geo_location`
- `profile_track`
- `summary_length`
- `summary_size_category`
- `portfolio_website`

## fact_connections_timeline.csv

### Origem

- `main.mart_connections_summary`

### Colunas

- `connection_year`
- `connection_month`
- `connection_year_month`
- `total_connections`
- `connections_with_email`
- `unique_companies`
- `unique_positions`

## fact_career_progression.csv

### Origem

- `main.mart_career_progression`

### Colunas

- `start_year`
- `start_month`
- `start_year_month`
- `total_positions_started`
- `current_positions_started`
- `unique_companies`
- `unique_titles`
- `avg_duration_months`

## fact_education_timeline.csv

### Origem

- `main.mart_education_summary`

## fact_certifications_timeline.csv

### Origem

- `main.mart_certifications_summary`

## fact_skills_summary.csv

### Origem

- `main.mart_skills_summary`

## fact_learning_summary.csv

### Origem

- `main.mart_learning_summary`

## fact_job_applications_timeline.csv

### Origem

- `main.mart_job_applications_summary`

## fact_recommendations_timeline.csv

### Origem

- `main.mart_recommendations_received_timeline`

## fact_recommendations_summary.csv

### Origem

- `main.mart_recommendations_received_summary`

## fact_endorsements_summary.csv

### Origem

- `main.mart_endorsement_received_info_summary`

## fact_company_follows.csv

### Origem

- `main.mart_company_follows_summary`

## fact_events_summary.csv

### Origem

- `main.mart_events_summary`

## fact_invitations_summary.csv

### Origem

- `main.mart_invitations_summary`

## fact_file_inventory.csv

### Origem

- `main.mart_file_inventory_summary`

## fact_pipeline_health.csv

### Origem

- `main.mart_pipeline_health_summary`

## Formato recomendado

### Opção 1: CSV

Vantagens:

- fácil de abrir
- fácil de versionar
- simples para Power BI

### Opção 2: Parquet

Vantagens:

- mais eficiente
- tipagem melhor
- melhor performance

Se for uma camada exportável mais madura, prefira Parquet.

## Frequência de geração

Recomendo gerar após:

1. `run_ingestion.py`
2. `inventory_linkedin_exports.py`
3. `dbt build`

Ou seja, no final do pipeline.

## Fluxo sugerido

```text
CSV bruto
-> DuckDB bronze
-> dbt marts
-> export powerbi/
-> Power BI
```

## Script futuro recomendado

Você pode criar depois um script como:

```text
scripts/utils/export_powerbi_layer.py
```

Esse script faria:

- conexão no DuckDB
- leitura dos marts
- export para `powerbi/exports/`
- opcionalmente CSV e Parquet

## Benefícios técnicos

- deixa o Power BI mais limpo
- separa camada analítica da camada de apresentação
- melhora portabilidade
- reduz risco de quebrar o `.pbix` por mudança interna do banco

## Recomendação final

Se você quiser transformar o dashboard em ativo de portfólio forte:

- crie a pasta `powerbi/`
- adicione uma camada exportável
- conecte o `.pbix` nessa camada

Isso deixa o case com cara de produto analítico profissional.
