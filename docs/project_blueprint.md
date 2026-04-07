# Project Blueprint

## Visão executiva

Este projeto foi desenhado como um case de analytics engineering com foco em transformar exportações do LinkedIn em um produto analítico navegável, com boa separação entre ingestão, modelagem, consumo e documentação.

## Princípios de desenho

- pipeline reproduzível antes de visual bonito
- modelagem por camadas e responsabilidade clara
- uso de notebooks apenas para exploração e validação
- marts pensados para consumo direto do app
- domínio de negócio acima de “arquivo por arquivo” no front-end

## Arquitetura lógica

### Camada 1. Fonte

Os arquivos CSV exportados pelo LinkedIn entram em `data/raw/` separados por export `basic` e `complete`.

### Camada 2. Ingestão Python

O módulo [ingestion.py](/c:/Projetos/linkedIn_career_intelligence_lakehouse/linkedin_career_intelligence/ingestion.py) faz:

- padronização de nomes de coluna
- parsing de datas
- limpeza textual
- remoção de linhas inválidas
- escrita em `bronze.*` no DuckDB

### Camada 3. Staging dbt

Os modelos `stg_*` fazem cast, trim e padronização leve para preparar os dados para consumo analítico.

### Camada 4. Intermediate dbt

Os modelos `int_*` agregam valor analítico:

- categorias derivadas
- flags booleanas
- timelines mensais
- trilhas e classificações
- campos limpos para agrupamento

### Camada 5. Marts dbt

Os modelos `mart_*` são tabelas finais orientadas ao app:

- resumo de saúde operacional
- resumo de conexões
- progressão de carreira
- indicadores de skills
- indicadores de learning
- candidaturas e job alerts
- contato e conta

### Camada 6. Aplicação

O Streamlit em `apps/` consome `main.mart_*` e, quando necessário, `main.int_*` para drilldown.

## Estratégia de crescimento

O repositório já saiu da fase “starter” e hoje suporta expansão por blocos funcionais. A estratégia recomendada é:

1. priorizar novos domínios com valor analítico claro
2. criar `stg_*` para cada nova source suportada
3. criar `int_*` apenas quando houver enriquecimento reaproveitável
4. criar `mart_*` quando o app precisar de consumo direto
5. evitar uma página por arquivo quando fizer mais sentido agrupar por domínio de negócio

## Organização recomendada para continuidade

### Manter

- `scripts/run_ingestion.py` como porta de entrada principal
- `scripts/run_pipeline.py` como orquestração local
- `staging / intermediate / marts` no dbt
- `apps/pages` por área de análise

### Evitar

- criar `int_*` sem transformação real
- duplicar regra de negócio entre Python, dbt e app
- usar notebooks como pipeline oficial
- criar páginas minúsculas quando uma área consolidada comunica melhor o caso

## Postura de projeto em contexto empresarial

Se este projeto estivesse em ambiente corporativo, o próximo nível natural seria:

- CI com `pytest`, `dbt build` e lint
- deploy do app automatizado
- versionamento de dados de entrada
- catálogo mais robusto dos modelos
- controle de qualidade por domínio
- estratégia formal de incidentes para leitura/parse de fontes

## Critérios de pronto para publicação

- app navegável com narrativa clara
- README objetivo e vendável
- arquitetura ilustrada
- estrutura limpa de artefatos
- documentação que explique o porquê das camadas
- screenshots ou GIFs do produto
