# Plano de Dashboard Power BI

Este documento responde três decisões importantes:

1. se vale mais a pena usar dados brutos no Power Query ou dados já tratados
2. como organizar os arquivos do projeto para o Power BI
3. como desenhar um dashboard profissional, página por página

## Recomendação principal

Para este projeto, a melhor abordagem é:

- tratar os dados antes
- expor uma camada analítica estável
- conectar o Power BI nessa camada final

Em outras palavras:

```text
LinkedIn CSV Export
-> Python ingestion
-> DuckDB bronze
-> dbt staging / intermediate / marts
-> Power BI
```

## O que é melhor para nível especialista em Power BI

### Opção A: usar dados brutos e tratar no Power Query

Isso demonstra:

- domínio de Power Query
- limpeza e transformação dentro do Power BI
- capacidade de estruturar ETL leve no próprio relatório

Mas tem limitações:

- o `.pbix` fica mais pesado
- a manutenção fica mais difícil
- você duplica lógica que já existe no projeto
- o modelo fica mais frágil para evolução
- você mistura engenharia de dados com camada semântica/visual

### Opção B: tratar antes e subir pronto para o Power BI

Isso demonstra:

- visão de arquitetura
- separação de responsabilidades
- maturidade de modelagem
- desenho de camada analítica reutilizável
- preocupação com escalabilidade e governança

Para um case especialista, esta opção é mais forte.

O Power BI passa a mostrar o que ele faz melhor:

- modelagem semântica
- medidas DAX
- design de dashboard
- storytelling executivo
- navegação analítica

## Conclusão recomendada

Se a meta é parecer mais sênior e mais especialista, o melhor é:

- usar os marts do DuckDB como fonte principal
- usar Power Query apenas para ajustes leves de consumo
- deixar as regras pesadas no pipeline Python + dbt

Isso posiciona melhor o trabalho como produto de dados completo.

## Quando vale usar o Power Query com dados brutos

Só faz sentido como exercício paralelo, por exemplo:

- criar uma versão alternativa do case só em Power BI
- demonstrar domínio de M/Power Query em entrevistas
- comparar uma arquitetura self-service vs uma arquitetura profissional em camadas

Se fizer isso, trate como um segundo case, não como substituto do pipeline atual.

## Organização recomendada de pastas

### Melhor opção

Manter o Power BI conectado à camada analítica já pronta deste projeto.

Sugestão:

```text
linkedIn_career_intelligence_lakehouse/
├─ powerbi/
│  ├─ linkedin_career_intelligence.pbix
│  ├─ exports/
│  └─ README.md
```

### Por que esta opção é melhor

- mantém tudo no mesmo contexto do case
- preserva a narrativa de arquitetura ponta a ponta
- facilita versionamento do material de apoio
- evita duplicar CSVs em outro lugar

### O que evitar

- criar outra pasta totalmente separada só para repetir os CSVs
- conectar o `.pbix` direto em múltiplos arquivos crus se você já tem DuckDB + dbt

## Fonte ideal para o Power BI

### Melhor fonte

Usar os `marts` do DuckDB.

Tabelas especialmente úteis:

- `mart_profile_summary`
- `mart_pipeline_health_summary`
- `mart_connections_summary`
- `mart_career_progression`
- `mart_education_summary`
- `mart_certifications_summary`
- `mart_languages_summary`
- `mart_skills_summary`
- `mart_learning_summary`
- `mart_job_applications_summary`
- `mart_recommendations_received_summary`
- `mart_recommendations_received_timeline`
- `mart_company_follows_summary`
- `mart_endorsement_received_info_summary`
- `mart_invitations_summary`
- `mart_events_summary`
- `mart_saved_job_alerts_summary`
- `mart_file_inventory_summary`

### Recomendação prática

1. mantenha DuckDB + dbt como camada produtora
2. crie uma camada exportável para Power BI, se quiser simplificar ainda mais
3. conecte o Power BI nessa camada final

## Camada exportável para Power BI

Se quiser elevar ainda mais o projeto, vale criar uma pasta de exportação com tabelas prontas para consumo do Power BI.

Exemplo:

```text
powerbi/exports/
├─ dim_profile.csv
├─ fact_connections_timeline.csv
├─ fact_career_progression.csv
├─ fact_skills_summary.csv
├─ fact_learning_summary.csv
├─ fact_jobs_summary.csv
├─ fact_pipeline_health.csv
```

Isso ajuda quando:

- você quer diminuir dependência direta do DuckDB dentro do Power BI
- você quer publicar uma versão portátil do dashboard
- você quer controlar melhor o schema consumido pelo `.pbix`

## Recomendação final de arquitetura

A melhor resposta para o seu caso é:

- não usar os dados brutos direto no Power BI como estratégia principal
- não criar uma pasta separada só para jogar CSVs novamente
- usar este mesmo projeto
- criar uma pasta `powerbi/`
- conectar o `.pbix` nos `marts` do DuckDB ou numa camada exportável derivada deles

Essa é a solução mais profissional.

## Blueprint do dashboard

## Página 1: Visão Executiva

### Objetivo

Dar uma leitura de alto nível da sua trajetória, do posicionamento profissional e da saúde do pipeline.

### KPIs

- nome do perfil
- headline principal
- trilha inferida
- total de conexões
- total de skills
- total de candidaturas
- total de registros de learning
- taxa de sucesso do pipeline

### Visuais

- cards executivos no topo
- linha do tempo resumida da carreira
- barra com categorias principais de skills
- bloco narrativo com resumo do perfil
- indicador de saúde do pipeline

### Tabelas sugeridas

- `mart_profile_summary`
- `mart_pipeline_health_summary`
- `mart_connections_summary`
- `mart_skills_summary`
- `mart_job_applications_summary`
- `mart_learning_summary`

## Página 2: Trajetória Profissional

### Objetivo

Mostrar evolução de carreira, empresas, cargos e movimentação ao longo do tempo.

### KPIs

- total de posições
- empresas distintas
- cargos distintos
- posições atuais
- duração média por experiência

### Visuais

- linha do tempo da carreira
- barras por empresa
- barras por cargo
- gráfico por ano/mês de início
- tabela detalhada de posições

### Tabelas sugeridas

- `mart_career_progression`

## Página 3: Networking e Conexões

### Objetivo

Mostrar escala, qualidade e diversidade da rede.

### KPIs

- total de conexões
- conexões com e-mail
- percentual com e-mail
- empresas únicas
- posições únicas

### Visuais

- evolução temporal de conexões
- top empresas da rede
- top cargos da rede
- distribuição por ano
- tabela de conexões resumidas

### Tabelas sugeridas

- `mart_connections_summary`

## Página 4: Formação e Credenciais

### Objetivo

Consolidar educação, certificações e idiomas.

### KPIs

- total de formações
- escolas únicas
- graus únicos
- total de certificações
- authorities únicas
- total de idiomas

### Visuais

- barras por trilha educacional
- barras por autoridade certificadora
- barras por proficiência de idioma
- timeline de educação
- tabela consolidada

### Tabelas sugeridas

- `mart_education_summary`
- `mart_certifications_summary`
- `mart_languages_summary`

## Página 5: Skills e Posicionamento Técnico

### Objetivo

Mostrar densidade, foco e amplitude das habilidades.

### KPIs

- total de skills
- skills únicas
- categoria dominante
- percentual de concentração da categoria principal

### Visuais

- barras por categoria de skill
- treemap de habilidades
- ranking de skills
- bloco interpretativo sobre concentração vs diversidade

### Tabelas sugeridas

- `mart_skills_summary`

## Página 6: Learning e Evolução Contínua

### Objetivo

Mostrar sinais de aprendizado e atualização profissional.

### KPIs

- total de conteúdos
- conteúdos concluídos
- conteúdos salvos
- tipos de conteúdo

### Visuais

- barras por tipo de conteúdo
- timeline de learning
- status de conclusão
- tabela de conteúdos

### Tabelas sugeridas

- `mart_learning_summary`

## Página 7: Oportunidades e Mercado

### Objetivo

Mostrar movimentação em vagas, alertas e aplicações.

### KPIs

- total de candidaturas
- job families
- total de alertas
- frequência de alertas

### Visuais

- barras por job family
- timeline de candidaturas
- cards de foco de mercado
- tabela de oportunidades

### Tabelas sugeridas

- `mart_job_applications_summary`
- `mart_saved_job_alerts_summary`

## Página 8: Prova Social e Reputação

### Objetivo

Mostrar endorsements e recomendações como evidência reputacional.

### KPIs

- total de recomendações
- média de tamanho das recomendações
- menções temáticas
- endorsers únicos
- skills endossadas

### Visuais

- timeline de recomendações
- barras por skill endossada
- cards de visibilidade
- tabela de prova social

### Tabelas sugeridas

- `mart_recommendations_received_summary`
- `mart_recommendations_received_timeline`
- `mart_endorsement_received_info_summary`

## Página 9: Engajamento e Presença

### Objetivo

Consolidar convites, eventos, follows e sinais de atividade.

### KPIs

- total de convites
- direções de convite
- total de eventos
- total de empresas seguidas

### Visuais

- barras por direção de convite
- barras por status de evento
- ranking de empresas seguidas
- cards de atividade na plataforma

### Tabelas sugeridas

- `mart_invitations_summary`
- `mart_events_summary`
- `mart_company_follows_summary`

## Página 10: Qualidade e Governança

### Objetivo

Mostrar robustez do pipeline e maturidade técnica.

### KPIs

- total de arquivos inventariados
- leituras com sucesso
- leituras com falha
- total de colunas
- última execução

### Visuais

- barras por categoria de dado
- barras por volume
- tabela de inventário
- cards de saúde operacional

### Tabelas sugeridas

- `mart_pipeline_health_summary`
- `mart_file_inventory_summary`

## Medidas e modelagem no Power BI

### O que deixar no DuckDB/dbt

- regras de limpeza
- categorização
- deduplicação
- derivação temporal
- enriquecimentos de negócio
- agregações estáveis

### O que deixar no Power BI

- medidas DAX
- percentuais
- ranking
- variações
- navegação por filtros
- tooltips
- bookmarks
- storytelling visual

## Nível especialista no Power BI

Para o dashboard parecer mais sênior:

- use tema visual consistente
- defina uma camada de medidas central
- crie tooltips customizados
- use drill-through
- use páginas com papéis claros
- aplique bookmarks para leitura executiva vs analítica
- organize as tabelas em fato/dimensão mesmo que a origem venha dos marts

## Próximo passo recomendado

O caminho mais forte é:

1. criar a pasta `powerbi/`
2. definir quais marts entram no dashboard
3. decidir se a conexão será direta no DuckDB ou via exportação CSV
4. montar um modelo semântico enxuto
5. construir o dashboard em 8 a 10 páginas

Se quiser, o próximo passo natural é eu preparar:

- uma especificação funcional do dashboard no estilo PRD
- uma sugestão de modelo semântico para o Power BI
- uma camada exportável para o Power BI a partir dos marts
