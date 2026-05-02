# Power BI Overview: Microcopy e Dicionario Semantico

## Microcopy Executivo

### Titulo da pagina
- `Overview`
- `LinkedIn career intelligence at a glance`

### KPIs do topo
- `Total Connections`
  Valor principal: volume de conexoes no periodo selecionado.
  Apoio opcional: `Network growth across the selected time window`

- `Connections MoM %`
  Valor principal: variacao mensal das conexoes.
  Apoio opcional: `Month-over-month change in network growth`

- `Total Applications`
  Valor principal: volume de candidaturas no periodo.
  Apoio opcional: `Applications submitted in the selected period`

- `Total Recommendations`
  Valor principal: volume de recomendacoes registradas.
  Apoio opcional: `Recommendations captured in the selected period`

- `Read Success Rate %`
  Valor principal: taxa de sucesso das leituras do pipeline.
  Apoio opcional: `Operational reliability of the data pipeline`

- `Last Refresh Date`
  Valor principal: data da ultima atualizacao do modelo.
  Apoio opcional: `Last model refresh reference date`

### Titulos dos blocos
- `Networking Growth`
- `Applications Momentum`
- `Career and Education Progress`
- `Professional Presence`
- `Pipeline Health`
- `Inventory Footprint`

### Subtitulos curtos sugeridos
- Networking Growth: `How the network evolves over time`
- Applications Momentum: `Application volume and completeness`
- Career and Education Progress: `Professional and learning milestones`
- Professional Presence: `Signals of engagement and credibility`
- Pipeline Health: `Reliability of ingestion and refresh operations`
- Inventory Footprint: `Scale and freshness of the analytical asset base`

## Dicionario Semantico

### _Measures
Descricao da tabela:
Tabela dedicada exclusivamente ao armazenamento e organizacao de medidas DAX do modelo.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo para identificacao da linha fisica.
- `Placeholder`: coluna tecnica usada apenas para materializar a tabela de medidas no modelo.

### dCalendario
Descricao da tabela:
Dimensao de calendario oficial do modelo, usada como eixo temporal central para as tabelas fato.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `date`: data base do calendario no nivel diario.
- `date_key`: chave numerica da data para integracao e ordenacao.
- `year`: ano calendario da data.
- `quarter_number`: numero do trimestre no ano.
- `month_number`: numero do mes no ano.
- `month_name`: nome completo do mes.
- `month_name_short`: abreviacao do nome do mes.
- `year_month`: representacao ano-mes da data.
- `year_month_label`: rotulo amigavel de ano e mes para exibicao.
- `semester_number`: numero do semestre no ano.
- `semester_label`: rotulo amigavel do semestre.
- `bimester_number`: numero do bimestre no ano.
- `bimester_label`: rotulo amigavel do bimestre.
- `week_of_year`: numero da semana no ano.
- `day`: dia do mes.
- `day_of_week_number`: numero do dia da semana.
- `day_of_week_name`: nome do dia da semana.
- `is_weekend`: indicador de fim de semana.
- `quarter_label`: rotulo amigavel do trimestre.
- `half_year_label`: rotulo amigavel do semestre.
- `bimester_year_label`: rotulo amigavel do bimestre no contexto do ano.
- `year_label`: rotulo amigavel do ano.

### dim_language_proficiency
Descricao da tabela:
Dimensao resumida com indicadores sobre idiomas e trilha de proficiencia observados no perfil.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `proficiency_track`: classificacao resumida da trilha de proficiencia em idiomas.
- `total_languages`: quantidade total de idiomas registrados.
- `unique_languages`: quantidade distinta de idiomas registrados.

### dim_profile
Descricao da tabela:
Dimensao de perfil com atributos descritivos do titular do perfil do LinkedIn.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `first_name`: primeiro nome do perfil.
- `last_name`: sobrenome do perfil.
- `headline`: headline profissional exibida no LinkedIn.
- `industry`: industria ou setor informado no perfil.
- `geo_location`: localizacao geografica principal do perfil.
- `profile_track`: classificacao resumida da trilha do perfil.
- `summary_length`: tamanho do resumo do perfil.
- `summary_size_category`: faixa categorizada do tamanho do resumo.
- `primary_contact_url`: URL principal de contato do perfil.
- `primary_contact_label`: rotulo da URL principal de contato.
- `portfolio_website`: site ou portfolio associado ao perfil.

### fact_connections_timeline
Descricao da tabela:
Fato mensal com a evolucao das conexoes e atributos agregados da rede ao longo do tempo.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `connection_year`: ano da agregacao mensal de conexoes.
- `connection_month`: numero do mes da agregacao de conexoes.
- `connection_year_month`: data representando o grao mensal original da tabela.
- `total_connections`: total de conexoes registradas no periodo.
- `connections_with_email`: total de conexoes com e-mail disponivel.
- `unique_companies`: quantidade distinta de empresas associadas as conexoes do periodo.
- `unique_positions`: quantidade distinta de cargos associados as conexoes do periodo.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_career_progression
Descricao da tabela:
Fato mensal com iniciacao de experiencias profissionais e indicadores agregados de progressao de carreira.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `start_year`: ano da agregacao mensal das experiencias iniciadas.
- `start_month`: numero do mes da agregacao das experiencias iniciadas.
- `start_year_month`: data representando o grao mensal original da tabela.
- `total_positions_started`: total de posicoes iniciadas no periodo.
- `current_positions_started`: total de posicoes iniciadas que permanecem atuais.
- `unique_companies`: quantidade distinta de empresas nas posicoes iniciadas.
- `unique_titles`: quantidade distinta de titulos/cargos iniciados.
- `avg_duration_months`: duracao media em meses das experiencias consideradas.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_education_timeline
Descricao da tabela:
Fato mensal com iniciacao de experiencias educacionais e indicadores agregados de formacao.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `start_year`: ano da agregacao mensal das formacoes iniciadas.
- `start_month`: numero do mes da agregacao das formacoes iniciadas.
- `start_year_month`: data representando o grao mensal original da tabela.
- `total_education_started`: total de registros educacionais iniciados no periodo.
- `current_education_started`: total de registros educacionais iniciados que seguem atuais.
- `unique_schools`: quantidade distinta de instituicoes de ensino.
- `unique_degrees`: quantidade distinta de titulacoes ou cursos.
- `avg_education_duration_months`: duracao media em meses das experiencias educacionais.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_certifications_timeline
Descricao da tabela:
Fato mensal com iniciacao de certificacoes e indicadores agregados do portifolio de credenciais.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `start_year`: ano da agregacao mensal das certificacoes iniciadas.
- `start_month`: numero do mes da agregacao das certificacoes iniciadas.
- `start_year_month`: data representando o grao mensal original da tabela.
- `total_certifications`: total de certificacoes registradas no periodo.
- `unique_certifications`: quantidade distinta de certificacoes.
- `unique_authorities`: quantidade distinta de autoridades emissoras.
- `avg_duration_months`: duracao media em meses das certificacoes com janela temporal.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_job_applications_timeline
Descricao da tabela:
Fato mensal com o volume de candidaturas e indicadores de completude do processo de aplicacao.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `application_year`: ano da agregacao mensal de candidaturas.
- `application_month`: numero do mes da agregacao de candidaturas.
- `application_year_month`: data representando o grao mensal original da tabela.
- `job_family`: familia de vagas associada as candidaturas.
- `total_applications`: total de candidaturas registradas no periodo.
- `unique_companies`: quantidade distinta de empresas nas candidaturas.
- `applications_with_resume`: total de candidaturas com curriculo associado.
- `applications_with_questionnaire`: total de candidaturas com questionario associado.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_recommendations_timeline
Descricao da tabela:
Fato mensal com a evolucao das recomendacoes e indicadores de conteudo textual.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `recommendation_year`: ano da agregacao mensal das recomendacoes.
- `recommendation_month`: numero do mes da agregacao das recomendacoes.
- `recommendation_year_month`: data representando o grao mensal original da tabela.
- `total_recommendations`: total de recomendacoes registradas no periodo.
- `avg_text_length`: tamanho medio do texto das recomendacoes.
- `mentions_data_count`: total de recomendacoes com mencao ao tema data.
- `mentions_teamwork_count`: total de recomendacoes com mencao a trabalho em equipe.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_events_summary
Descricao da tabela:
Fato mensal com eventos agregados e indicadores de cobertura de links.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `event_year`: ano da agregacao mensal dos eventos.
- `event_month`: numero do mes da agregacao dos eventos.
- `event_year_month`: data representando o grao mensal original da tabela.
- `status`: status agregado do evento.
- `total_events`: total de eventos registrados no periodo.
- `events_with_url`: total de eventos com URL associada.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_invitations_summary
Descricao da tabela:
Fato mensal com convites agregados e indicadores de personalizacao da abordagem.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `invitation_year`: ano da agregacao mensal dos convites.
- `invitation_month`: numero do mes da agregacao dos convites.
- `invitation_year_month`: data representando o grao mensal original da tabela.
- `direction`: direcao do convite, como enviado ou recebido.
- `total_invitations`: total de convites registrados no periodo.
- `invitations_with_message`: total de convites acompanhados de mensagem.
- `month_date`: data oficial do primeiro dia do mes usada no relacionamento com `dCalendario`.

### fact_company_follows
Descricao da tabela:
Fato resumido com empresas seguidas e janela temporal da relacao de acompanhamento.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `organization_clean`: nome padronizado da organizacao seguida.
- `follow_count`: quantidade de registros de follow associados a organizacao.
- `first_follow_date`: primeira data conhecida de follow da organizacao.
- `last_follow_date`: data mais recente conhecida de follow da organizacao.

### fact_contact_account
Descricao da tabela:
Fato resumido com indicadores da conta de contato e janelas temporais de registro.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `total_email_addresses`: total de enderecos de e-mail registrados.
- `confirmed_email_addresses`: total de enderecos de e-mail confirmados.
- `primary_email_addresses`: total de e-mails marcados como principais.
- `total_phone_numbers`: total de numeros de telefone registrados.
- `first_registered_at`: primeira data/hora de registro conhecida da conta.
- `latest_registered_at`: data/hora mais recente de registro conhecida da conta.
- `subscription_types`: tipos de assinatura associados a conta.
- `account_age_years`: idade estimada da conta em anos.
- `first_registered_at_key`: versao somente data da primeira data de registro para relacionamento com `dCalendario`.
- `latest_registered_at_key`: versao somente data da ultima data de registro para relacionamento com `dCalendario`.

### fact_file_inventory
Descricao da tabela:
Fato resumido com o inventario dos arquivos processados e metricas de volume da camada analitica.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `export_type`: tipo de exportacao do arquivo inventariado.
- `categoria_dado`: categoria de dado do arquivo inventariado.
- `status_leitura`: status da leitura do arquivo no pipeline.
- `volume_categoria`: classificacao de volume do arquivo ou categoria.
- `total_arquivos`: total de arquivos no agrupamento.
- `total_linhas`: total de linhas no agrupamento.
- `total_colunas`: total de colunas no agrupamento.
- `total_tamanho_kb`: tamanho total em kilobytes do agrupamento.
- `primeira_execucao_inventario`: primeira data/hora de execucao do inventario.
- `ultima_execucao_inventario`: data/hora mais recente de execucao do inventario.
- `primeira_execucao_inventario_key`: versao somente data da primeira execucao para relacionamento com `dCalendario`.
- `ultima_execucao_inventario_key`: versao somente data da ultima execucao para relacionamento com `dCalendario`.

### fact_endorsements_summary
Descricao da tabela:
Fato resumido com endossos por habilidade e janela temporal de ocorrencia.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `skill_name_clean`: nome padronizado da habilidade endossada.
- `endorsement_count`: quantidade total de endossos da habilidade.
- `unique_endorsers`: quantidade distinta de pessoas que endossaram a habilidade.
- `first_endorsement_date`: primeira data conhecida de endosso da habilidade.
- `last_endorsement_date`: data mais recente conhecida de endosso da habilidade.

### fact_recommendations_summary
Descricao da tabela:
Fato resumido com recomendacoes e indicadores agregados de qualidade e conteudo textual.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `total_recommendations`: total de recomendacoes consideradas no agrupamento.
- `avg_text_length`: tamanho medio do texto das recomendacoes.
- `mentions_data_count`: quantidade de recomendacoes com mencao a data.
- `mentions_teamwork_count`: quantidade de recomendacoes com mencao a trabalho em equipe.
- `first_recommendation_date`: primeira data conhecida de recomendacao.
- `last_recommendation_date`: data mais recente conhecida de recomendacao.

### fact_learning_summary
Descricao da tabela:
Fato resumido com conteudos de aprendizado e indicadores de conclusao, salvamento e anotacoes.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `content_type_clean`: tipo padronizado do conteudo de aprendizado.
- `total_contents`: total de conteudos registrados.
- `completed_contents`: total de conteudos concluidos.
- `saved_contents`: total de conteudos salvos.
- `contents_with_notes`: total de conteudos com anotacoes.

### fact_saved_job_alerts
Descricao da tabela:
Fato resumido com alertas de vaga salvos e seus principais atributos de configuracao.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `alert_frequency`: frequencia configurada para o alerta de vagas.
- `total_alerts`: total de alertas registrados.
- `unique_keyword_sets`: quantidade distinta de combinacoes de palavras-chave.
- `remote_alerts`: total de alertas com foco em trabalho remoto.
- `company_scoped_alerts`: total de alertas limitados a empresas especificas.

### fact_skills_summary
Descricao da tabela:
Fato resumido com habilidades agregadas por categoria e caracteristicas textuais.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `skill_category`: categoria da habilidade.
- `total_skills`: total de habilidades no agrupamento.
- `unique_skills`: quantidade distinta de habilidades no agrupamento.
- `avg_skill_name_length`: tamanho medio do nome das habilidades.

### fact_pipeline_health
Descricao da tabela:
Fato resumido de monitoramento operacional do pipeline e consolidacao de volumes principais do ecossistema analitico.

Colunas:
- `RowNumber-*`: coluna tecnica interna gerada pelo modelo.
- `total_inventory_files`: total de arquivos contabilizados no inventario.
- `successful_reads`: total de leituras bem-sucedidas no pipeline.
- `failed_reads`: total de leituras com falha no pipeline.
- `latest_inventory_timestamp`: data/hora mais recente registrada para o inventario.
- `total_connections`: total consolidado de conexoes.
- `total_positions`: total consolidado de posicoes profissionais.
- `total_education_records`: total consolidado de registros educacionais.
- `total_certifications`: total consolidado de certificacoes.
- `total_languages`: total consolidado de idiomas.
- `total_endorsements`: total consolidado de endossos.
- `total_company_follows`: total consolidado de empresas seguidas.
- `total_recommendations`: total consolidado de recomendacoes.
- `total_skills`: total consolidado de habilidades.
- `total_invitations`: total consolidado de convites.
- `total_events`: total consolidado de eventos.
- `total_learning_records`: total consolidado de registros de aprendizado.
- `total_job_applications`: total consolidado de candidaturas.
- `total_saved_job_alerts`: total consolidado de alertas de vagas salvos.
- `total_volunteering`: total consolidado de registros de voluntariado.
- `total_email_addresses`: total consolidado de enderecos de e-mail.
- `total_phone_numbers`: total consolidado de numeros de telefone.
- `latest_inventory_timestamp_key`: versao somente data do timestamp de inventario para relacionamento com `dCalendario`.
