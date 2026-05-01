# Data Dictionary

## Objetivo

Este documento resume as principais entidades analíticas do projeto, sua origem e como elas são usadas no app.

## Camadas

### `data/raw`

Arquivos CSV originais exportados do LinkedIn. São a fonte bruta do projeto.

### `bronze.*`

Tabelas carregadas pelo Python no DuckDB com limpeza inicial, mas ainda muito próximas da fonte.

### `main.stg_*`

Modelos dbt de padronização leve. Ajustam nomes, tipos e campos mínimos para consumo analítico.

### `main.int_*`

Modelos dbt enriquecidos com classificações, flags, timelines, categorias e campos prontos para agrupamentos.

### `main.mart_*`

Modelos finais consumidos pelo app Streamlit.

## Entidades principais

### `bronze.profile`

- Origem: `Profile.csv` e `Profile Summary.csv`
- Papel: consolidar os dados centrais do perfil principal
- Uso final: headline, summary, contexto profissional e identidade do app

### `bronze.connections`

- Origem: `Connections.csv`
- Papel: base relacional da rede profissional
- Uso final: evolução mensal, e-mails disponíveis, empresas e cargos

### `bronze.positions`

- Origem: `Positions.csv`
- Papel: histórico de experiências profissionais
- Uso final: leitura de trajetória, empresas e cargos recorrentes

### `bronze.education`

- Origem: `Education.csv`
- Papel: histórico acadêmico e formativo
- Uso final: formações, instituições, trilhas educacionais

### `bronze.certifications`

- Origem: `Certifications.csv`
- Papel: certificações obtidas e suas authorities
- Uso final: trilhas de certificação e recorrência temporal

### `bronze.languages`

- Origem: `Languages.csv`
- Papel: idiomas e proficiência registrada
- Uso final: leitura executiva de idioma principal e faixa de proficiência

### `bronze.endorsement_received_info`

- Origem: `Endorsement_Received_Info.csv`
- Papel: endorsements recebidos
- Uso final: sinais de reconhecimento e competências validadas

### `bronze.company_follows`

- Origem: `Company Follows.csv`
- Papel: empresas acompanhadas na plataforma
- Uso final: interesse setorial e rastros de afinidade profissional

### `bronze.recommendations_received`

- Origem: `Recommendations_Received.csv`
- Papel: recomendações recebidas por texto
- Uso final: prova social, temas recorrentes e linha do tempo

### `bronze.skills`

- Origem: `Skills.csv`
- Papel: inventário de habilidades registradas
- Uso final: distribuição por categoria e leitura de concentração técnica

### `bronze.invitations`

- Origem: `Invitations.csv`
- Papel: convites enviados e recebidos
- Uso final: sinais de presença profissional e alcance de relacionamento

### `bronze.events`

- Origem: `Events.csv`
- Papel: eventos registrados na plataforma
- Uso final: atividade e engajamento por status

### `bronze.learning`

- Origem: `Learning.csv`
- Papel: histórico de cursos e conteúdos
- Uso final: trilha de aprendizado, itens salvos e leitura executiva de evolução

### `bronze.job_applications`

- Origem: `Job Applicant Saved Screening Question Responses.csv` e relacionados
- Papel: candidaturas registradas
- Uso final: famílias de vaga, volume de aplicações e foco de movimentação

### `bronze.saved_job_alerts`

- Origem: `SavedJobAlerts.csv`
- Papel: alertas de vagas salvos
- Uso final: intensidade de monitoramento de oportunidades

### `bronze.volunteering`

- Origem: `Volunteering.csv`
- Papel: experiências de voluntariado
- Uso final: presença de atuação complementar fora do eixo formal de carreira

### `bronze.file_inventory`

- Origem: inventário técnico montado por script
- Papel: registrar cobertura, volume e status de leitura dos arquivos
- Uso final: página `Health` e monitoramento operacional

### `bronze.ingestion_audit`

- Origem: auditoria gerada automaticamente a cada `load_table`
- Papel: registrar arquivo de origem, tabela bronze, volume carregado e contrato aplicado
- Uso final: governança operacional, troubleshooting e rastreabilidade de ingestão

## Marts principais

### `main.mart_pipeline_health_summary`

- Resumo executivo de saúde do pipeline
- Métricas-chave:
  - total de arquivos inventariados
  - leituras com sucesso
  - leituras com erro
  - totais por domínio

### `main.mart_file_inventory_summary`

- Agregação técnica por export, categoria e status de leitura
- Suporta visões operacionais e auditoria do inventário

### `main.mart_profile_summary`

- Resumo final do perfil principal
- Métricas-chave:
  - headline
  - trilha inferida
  - tamanho e categoria do summary
  - website e contato principal

### `main.mart_connections_summary`

- Série mensal da rede
- Métricas-chave:
  - total de conexões
  - conexões com e-mail
  - empresas únicas
  - cargos únicos

### `main.mart_career_progression`

- Série temporal de posições iniciadas
- Métricas-chave:
  - posições iniciadas
  - cargos únicos
  - empresas únicas
  - duração média

### `main.mart_education_summary`

- Série temporal da formação
- Métricas-chave:
  - formações iniciadas
  - duração média
  - instituições únicas
  - graus únicos

### `main.mart_certifications_summary`

- Série temporal e agregações de certificações
- Métricas-chave:
  - total de certificações
  - authorities únicas
  - duração média

### `main.mart_languages_summary`

- Resumo por trilha de proficiência
- Métricas-chave:
  - total de idiomas
  - idiomas únicos
  - trilha de proficiência dominante

### `main.mart_skills_summary`

- Resumo por categoria de skills
- Métricas-chave:
  - total por categoria
  - concentração da categoria dominante

### `main.mart_learning_summary`

- Resumo por tipo de conteúdo de learning
- Métricas-chave:
  - total de conteúdos
  - conteúdos concluídos
  - conteúdos salvos
  - conteúdos com notas

### `main.mart_job_applications_summary`

- Resumo por família de vaga
- Métricas-chave:
  - total de candidaturas
  - agrupamento por job family

### `main.mart_saved_job_alerts_summary`

- Resumo por frequência de alerta salvo
- Métricas-chave:
  - total de alertas
  - frequência dominante

### `main.mart_recommendations_received_summary`

- Resumo textual das recomendações
- Métricas-chave:
  - total de recomendações
  - tamanho médio do texto
  - menções a temas relevantes

## Convenções de leitura

- `*_clean`: campo textual padronizado para agrupamento
- `*_track`: classificação derivada para leitura analítica
- `year_month`: recorte mensal para séries temporais
- `mart_*`: objeto final orientado ao app
- `int_*`: objeto intermediário orientado a regra reaproveitável
