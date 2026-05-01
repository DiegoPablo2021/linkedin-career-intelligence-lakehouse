# Modelo Semântico Power BI

## Objetivo

Definir uma modelagem semântica enxuta, profissional e sustentável para o dashboard Power BI deste projeto.

A ideia não é importar tudo como veio do DuckDB, mas organizar o consumo em um desenho próximo de fatos e dimensões.

## Princípio de modelagem

Usar os `marts` como fonte principal e transformá-los em um modelo semântico com:

- dimensões de contexto
- fatos de eventos, trajetória e agregações temporais
- medidas DAX centralizadas

## Estratégia recomendada

### Dimensões

- `dim_profile`
- `dim_date`
- `dim_skill_category`
- `dim_job_family`
- `dim_language_proficiency`
- `dim_invitation_direction`
- `dim_event_status`
- `dim_inventory_category`

### Fatos

- `fact_connections_timeline`
- `fact_career_progression`
- `fact_education_timeline`
- `fact_certifications_timeline`
- `fact_recommendations_timeline`
- `fact_job_applications_timeline`
- `fact_learning_summary`
- `fact_skills_summary`
- `fact_events_summary`
- `fact_invitations_summary`
- `fact_company_follows`
- `fact_endorsements_summary`
- `fact_file_inventory`
- `fact_pipeline_health`

## Tabelas sugeridas

## 1. dim_profile

### Origem

- `mart_profile_summary`

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

### Papel

Tabela institucional do dashboard. Serve para identidade, contexto e cards principais.

## 2. dim_date

### Origem

Derivada no Power BI.

### Colunas sugeridas

- `Date`
- `Year`
- `Month Number`
- `Month Name`
- `YearMonth`
- `Quarter`

### Papel

Padronizar e conectar todos os fatos temporais.

## 3. fact_connections_timeline

### Origem

- `mart_connections_summary`

### Chave temporal

- `connection_year_month`

### Métricas

- `total_connections`
- `connections_with_email`
- `unique_companies`
- `unique_positions`

## 4. fact_career_progression

### Origem

- `mart_career_progression`

### Chave temporal

- `start_year_month`

### Métricas

- `total_positions_started`
- `current_positions_started`
- `unique_companies`
- `unique_titles`
- `avg_duration_months`

## 5. fact_education_timeline

### Origem

- `mart_education_summary`

### Chave temporal

- `start_year_month`

### Métricas

- `total_education_started`
- `current_education_started`
- `unique_schools`
- `unique_degrees`
- `avg_education_duration_months`

## 6. fact_certifications_timeline

### Origem

- `mart_certifications_summary`

### Chave temporal

- `start_year_month`

### Métricas

- `total_certifications`
- `unique_certifications`
- `unique_authorities`
- `avg_duration_months`

## 7. fact_recommendations_timeline

### Origem

- `mart_recommendations_received_timeline`
- `mart_recommendations_received_summary`

### Chave temporal

- `recommendation_year_month`

### Métricas

- `total_recommendations`
- `avg_text_length`
- `mentions_data_count`
- `mentions_teamwork_count`

## 8. fact_job_applications_timeline

### Origem

- `mart_job_applications_summary`

### Chave temporal

- `application_year_month`

### Métricas

- `total_applications`
- `unique_companies`
- `applications_with_resume`
- `applications_with_questionnaire`

### Dimensão associada

- `dim_job_family`

## 9. fact_learning_summary

### Origem

- `mart_learning_summary`

### Métricas

- `total_contents`
- `completed_contents`
- `saved_contents`
- `contents_with_notes`

### Dimensão associada

- `dim_content_type`

## 10. fact_skills_summary

### Origem

- `mart_skills_summary`

### Métricas

- `total_skills`
- `unique_skills`
- `avg_skill_name_length`

### Dimensão associada

- `dim_skill_category`

## 11. fact_events_summary

### Origem

- `mart_events_summary`

### Métricas

- `total_events`
- `events_with_url`

### Dimensão associada

- `dim_event_status`

## 12. fact_invitations_summary

### Origem

- `mart_invitations_summary`

### Métricas

- `total_invitations`
- `invitations_with_message`

### Dimensão associada

- `dim_invitation_direction`

## 13. fact_company_follows

### Origem

- `mart_company_follows_summary`

### Métricas

- `follow_count`

## 14. fact_endorsements_summary

### Origem

- `mart_endorsement_received_info_summary`

### Métricas

- `endorsement_count`
- `unique_endorsers`

## 15. fact_file_inventory

### Origem

- `mart_file_inventory_summary`

### Métricas

- `total_arquivos`
- `total_linhas`
- `total_colunas`
- `total_tamanho_kb`

### Dimensões associadas

- `export_type`
- `categoria_dado`
- `status_leitura`
- `volume_categoria`

## 16. fact_pipeline_health

### Origem

- `mart_pipeline_health_summary`

### Métricas

- todos os totais de domínio
- sucesso/falha de leitura
- timestamps de execução

## Relacionamentos recomendados

### Relacionamentos temporais

Todas as tabelas com `YearMonth` ou data equivalente devem se conectar à `dim_date`.

### Relacionamentos categóricos

- `fact_skills_summary` -> `dim_skill_category`
- `fact_job_applications_timeline` -> `dim_job_family`
- `fact_events_summary` -> `dim_event_status`
- `fact_invitations_summary` -> `dim_invitation_direction`

## Boas práticas no `.pbix`

- esconder colunas técnicas
- manter medidas em tabela própria, por exemplo `measures`
- padronizar nomes de medidas
- usar `dim_date` única no modelo
- preferir estrela simples e legível

## Estrutura sugerida no painel de campos

- `_Measures`
- `dim_profile`
- `dim_date`
- `fact_connections_timeline`
- `fact_career_progression`
- `fact_skills_summary`
- `fact_learning_summary`
- `fact_job_applications_timeline`
- `fact_pipeline_health`

## Recomendação final

Não tente usar todas as tabelas em todas as páginas.

Monte um modelo:

- pequeno
- legível
- semanticamente claro
- orientado às perguntas do dashboard

Esse desenho passa mais maturidade do que um `.pbix` com dezenas de tabelas soltas.
