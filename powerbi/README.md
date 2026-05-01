# Power BI

Esta pasta concentra a camada de consumo do dashboard Power BI deste projeto.

## Nome recomendado do arquivo `.pbix`

Use:

```text
linkedin_career_intelligence.pbix
```

Esse nome já conversa com o nome do repositório, com o app Streamlit e com a documentação existente.

## Estrutura sugerida

```text
powerbi/
├─ linkedin_career_intelligence.pbix
├─ README.md
├─ codex_prompt.md
└─ exports/
```

## Recomendação de conexão

O caminho mais profissional e portátil para este projeto é:

```text
LinkedIn CSV Export
-> Python ingestion
-> DuckDB + dbt marts
-> powerbi/exports
-> Power BI Desktop
```

Em vez de conectar o `.pbix` direto ao arquivo `warehouse/linkedin_career_intelligence.duckdb`, a recomendação é consumir a camada exportada. Isso reduz acoplamento, facilita troubleshooting e deixa o refresh do Power BI mais previsível.

## Como gerar a camada exportável

Depois de rodar ingestão + inventário + dbt:

```powershell
python scripts\run_ingestion.py
python scripts\profiling\inventory_linkedin_exports.py
cd linkedin_career_intelligence_dbt
dbt build --profiles-dir ..\profiles
cd ..
python scripts\utils\export_powerbi_layer.py
```

O script exporta por padrão `CSV` e `Parquet` para `powerbi/exports/`.

Se quiser escolher o formato:

```powershell
python scripts\utils\export_powerbi_layer.py --format csv
python scripts\utils\export_powerbi_layer.py --format parquet
python scripts\utils\export_powerbi_layer.py --format both
```

## Arquivos exportados

Os principais arquivos gerados hoje são:

- `dim_date`
- `dim_profile`
- `dim_language_proficiency`
- `fact_connections_timeline`
- `fact_career_progression`
- `fact_education_timeline`
- `fact_certifications_timeline`
- `fact_skills_summary`
- `fact_learning_summary`
- `fact_job_applications_timeline`
- `fact_recommendations_timeline`
- `fact_recommendations_summary`
- `fact_endorsements_summary`
- `fact_company_follows`
- `fact_events_summary`
- `fact_invitations_summary`
- `fact_saved_job_alerts`
- `fact_file_inventory`
- `fact_pipeline_health`
- `fact_contact_account`

Também é gerado `_export_manifest`, com metadados do lote exportado.

## Como usar no Power BI Desktop

### Opção recomendada

1. Abra o Power BI Desktop.
2. Use `Obter Dados`.
3. Importe os arquivos de `powerbi/exports/`, preferencialmente os `parquet`.
4. Marque `dim_date[date]` como tabela de datas.
5. Crie os relacionamentos usando as colunas `year_month` ou datas equivalentes em cada fato temporal.

### Opção direta no DuckDB

É possível, mas menos recomendada.

Na prática, a conexão direta costuma depender de conector/driver intermediário e tende a ser menos portátil do que a camada exportada. Como a intenção aqui é ter um dashboard profissional, executivo e fácil de publicar, a camada `exports/` é o melhor default.

## Relacionamentos recomendados

- `fact_connections_timeline[connection_year_month]` -> `dim_date[year_month]`
- `fact_career_progression[start_year_month]` -> `dim_date[year_month]`
- `fact_education_timeline[start_year_month]` -> `dim_date[year_month]`
- `fact_certifications_timeline[start_year_month]` -> `dim_date[year_month]`
- `fact_job_applications_timeline[application_year_month]` -> `dim_date[year_month]`
- `fact_recommendations_timeline[recommendation_year_month]` -> `dim_date[year_month]`
- `fact_events_summary[event_year_month]` -> `dim_date[year_month]`
- `fact_invitations_summary[invitation_year_month]` -> `dim_date[year_month]`

## `dCalendario`

Você mencionou uma `dCalendario` robusta. O script já exporta `dim_date`, contendo:

- data
- chave numérica
- dia
- dia da semana
- mês
- nome do mês
- ano
- trimestre
- bimestre
- semestre
- semana do ano
- rótulos anuais e mensais

No Power BI, você pode renomear a tabela importada para `dCalendario` sem problema.

## Medidas DAX

As medidas sugeridas continuam documentadas em:

- [docs/powerbi_dax_measures.md](../docs/powerbi_dax_measures.md)

O ideal é criar uma tabela `_Measures` no Power BI e organizar as medidas em display folders como:

- `Executive`
- `Network`
- `Career`
- `Education`
- `Skills`
- `Learning`
- `Jobs`
- `Reputation`
- `Engagement`
- `Governance`
- `Narrative`
- `Time Intelligence`

## Observação importante sobre automação

Neste ambiente, o melhor uso do Codex é:

- preparar a camada exportável
- sugerir o modelo semântico
- escrever as medidas DAX
- definir os relacionamentos e o blueprint do dashboard

Controlar o Power BI Desktop já aberto, clicar na interface e persistir o `.pbix` automaticamente não é algo confiável aqui. Por isso, a abordagem recomendada é o Codex preparar toda a base e você fazer a ligação final dentro do Power BI em poucos minutos.
