# Medidas DAX Sugeridas

## Objetivo

Centralizar medidas que deem cara profissional ao dashboard Power BI.

A recomendação é criar uma tabela chamada:

- `_Measures`

e colocar nela as principais medidas DAX do projeto.

## Convenção de nomes

Prefira nomes simples e legíveis:

- `Total Connections`
- `Connections With Email`
- `Email Coverage %`
- `Total Skills`
- `Current Positions`

## Medidas Executivas

```DAX
Total Connections =
SUM ( fact_connections_timeline[total_connections] )
```

```DAX
Total Skills =
SUM ( fact_skills_summary[total_skills] )
```

```DAX
Total Applications =
SUM ( fact_job_applications_timeline[total_applications] )
```

```DAX
Total Learning Records =
SUM ( fact_learning_summary[total_contents] )
```

```DAX
Successful Reads =
SUM ( fact_pipeline_health[successful_reads] )
```

```DAX
Failed Reads =
SUM ( fact_pipeline_health[failed_reads] )
```

```DAX
Pipeline Success % =
DIVIDE ( [Successful Reads], [Successful Reads] + [Failed Reads] )
```

## Rede e Networking

```DAX
Connections With Email =
SUM ( fact_connections_timeline[connections_with_email] )
```

```DAX
Email Coverage % =
DIVIDE ( [Connections With Email], [Total Connections] )
```

```DAX
Unique Companies In Network =
MAX ( fact_connections_timeline[unique_companies] )
```

```DAX
Unique Positions In Network =
MAX ( fact_connections_timeline[unique_positions] )
```

## Carreira

```DAX
Total Positions Started =
SUM ( fact_career_progression[total_positions_started] )
```

```DAX
Current Positions =
SUM ( fact_career_progression[current_positions_started] )
```

```DAX
Unique Companies In Career =
MAX ( fact_career_progression[unique_companies] )
```

```DAX
Unique Titles =
MAX ( fact_career_progression[unique_titles] )
```

```DAX
Average Position Duration =
AVERAGE ( fact_career_progression[avg_duration_months] )
```

## Educação e Certificações

```DAX
Total Education Records =
SUM ( fact_education_timeline[total_education_started] )
```

```DAX
Current Education Records =
SUM ( fact_education_timeline[current_education_started] )
```

```DAX
Unique Schools =
MAX ( fact_education_timeline[unique_schools] )
```

```DAX
Unique Degrees =
MAX ( fact_education_timeline[unique_degrees] )
```

```DAX
Total Certifications =
SUM ( fact_certifications_timeline[total_certifications] )
```

```DAX
Unique Authorities =
MAX ( fact_certifications_timeline[unique_authorities] )
```

## Skills

```DAX
Total Skills Loaded =
SUM ( fact_skills_summary[total_skills] )
```

```DAX
Unique Skills =
SUM ( fact_skills_summary[unique_skills] )
```

```DAX
Top Skill Category Volume =
MAX ( fact_skills_summary[total_skills] )
```

```DAX
Top Skill Category Share % =
DIVIDE ( [Top Skill Category Volume], [Total Skills Loaded] )
```

## Learning

```DAX
Total Contents =
SUM ( fact_learning_summary[total_contents] )
```

```DAX
Completed Contents =
SUM ( fact_learning_summary[completed_contents] )
```

```DAX
Saved Contents =
SUM ( fact_learning_summary[saved_contents] )
```

```DAX
Contents With Notes =
SUM ( fact_learning_summary[contents_with_notes] )
```

```DAX
Completion Rate % =
DIVIDE ( [Completed Contents], [Total Contents] )
```

## Jobs

```DAX
Total Job Applications =
SUM ( fact_job_applications_timeline[total_applications] )
```

```DAX
Applications With Resume =
SUM ( fact_job_applications_timeline[applications_with_resume] )
```

```DAX
Applications With Questionnaire =
SUM ( fact_job_applications_timeline[applications_with_questionnaire] )
```

```DAX
Resume Usage % =
DIVIDE ( [Applications With Resume], [Total Job Applications] )
```

## Recomendações e Reputação

```DAX
Total Recommendations =
SUM ( fact_recommendations_timeline[total_recommendations] )
```

```DAX
Average Recommendation Length =
AVERAGE ( fact_recommendations_timeline[avg_text_length] )
```

```DAX
Data Mentions =
SUM ( fact_recommendations_timeline[mentions_data_count] )
```

```DAX
Teamwork Mentions =
SUM ( fact_recommendations_timeline[mentions_teamwork_count] )
```

```DAX
Data Mentions Share % =
DIVIDE ( [Data Mentions], [Data Mentions] + [Teamwork Mentions] )
```

## Endorsements

```DAX
Total Endorsements =
SUM ( fact_endorsements_summary[endorsement_count] )
```

```DAX
Unique Endorsers =
SUM ( fact_endorsements_summary[unique_endorsers] )
```

## Engajamento

```DAX
Total Invitations =
SUM ( fact_invitations_summary[total_invitations] )
```

```DAX
Invitations With Message =
SUM ( fact_invitations_summary[invitations_with_message] )
```

```DAX
Invitation Message Rate % =
DIVIDE ( [Invitations With Message], [Total Invitations] )
```

```DAX
Total Events =
SUM ( fact_events_summary[total_events] )
```

```DAX
Events With URL =
SUM ( fact_events_summary[events_with_url] )
```

```DAX
Events With URL % =
DIVIDE ( [Events With URL], [Total Events] )
```

## Qualidade e Governança

```DAX
Total Inventory Files =
SUM ( fact_file_inventory[total_arquivos] )
```

```DAX
Total Inventory Rows =
SUM ( fact_file_inventory[total_linhas] )
```

```DAX
Total Inventory Columns =
SUM ( fact_file_inventory[total_colunas] )
```

```DAX
Total Inventory Size KB =
SUM ( fact_file_inventory[total_tamanho_kb] )
```

## Medidas de apoio narrativo

```DAX
Profile Full Name =
SELECTEDVALUE ( dim_profile[first_name] ) & " " & SELECTEDVALUE ( dim_profile[last_name] )
```

```DAX
Primary Headline =
SELECTEDVALUE ( dim_profile[headline] )
```

```DAX
Profile Track =
SELECTEDVALUE ( dim_profile[profile_track] )
```

```DAX
Summary Category =
SELECTEDVALUE ( dim_profile[summary_size_category] )
```

## Indicadores de variação temporal

```DAX
Connections Previous Period =
CALCULATE (
    [Total Connections],
    DATEADD ( dim_date[Date], -1, MONTH )
)
```

```DAX
Connections Delta =
[Total Connections] - [Connections Previous Period]
```

```DAX
Connections Delta % =
DIVIDE ( [Connections Delta], [Connections Previous Period] )
```

O mesmo padrão pode ser replicado para:

- candidaturas
- recomendações
- learning
- certificações

## Recomendação final

O Power BI vai parecer mais especialista se você:

- centralizar as medidas
- usar nomes consistentes
- separar medidas de negócio e medidas técnicas
- evitar cálculo espalhado em visuais

Medida boa em tabela central transmite maturidade de modelagem.
