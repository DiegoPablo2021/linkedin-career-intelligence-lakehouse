# Explicação Técnica do Projeto

## Arquitetura técnica

O projeto foi estruturado como um pipeline analítico em camadas:

```text
LinkedIn CSV Export
-> Python ingestion
-> DuckDB bronze
-> dbt staging
-> dbt intermediate
-> dbt marts
-> Streamlit app
```

Essa separação foi escolhida para manter responsabilidades claras e permitir evolução com baixo acoplamento.

## Stack utilizada

- **Python** para ingestão, configuração, saneamento e automação
- **Pandas** para transformação tabular na carga inicial
- **DuckDB** como warehouse analítico local
- **dbt** para modelagem e testes de dados
- **Streamlit** para visualização e consumo
- **sqlfluff** para qualidade de SQL
- **pytest** para testes Python
- **GitHub Actions** para CI

## Organização por camadas

### 1. Camada raw

Contém os arquivos originais exportados do LinkedIn em `data/raw/`.

### 2. Camada bronze

É a primeira persistência estruturada no DuckDB.

Responsabilidades:

- leitura dos CSVs
- normalização de colunas
- coerção de tipos
- limpeza textual
- deduplicação
- persistência no schema `bronze`

Também há duas tabelas importantes de governança:

- `bronze.file_inventory`
- `bronze.ingestion_audit`

### 3. Camada staging

Implementada em dbt.

Responsabilidades:

- padronização leve
- casting consistente
- pequenas normalizações SQL-friendly
- exposição de fontes limpas para consumo interno

### 4. Camada intermediate

Implementada em dbt.

Responsabilidades:

- enriquecimento analítico
- criação de classificações
- chaves derivadas
- agrupamentos reutilizáveis
- campos auxiliares para marts e app

Exemplos:

- `career_track`
- `education_track`
- `certification_track`
- `proficiency_track`

### 5. Camada marts

Implementada em dbt.

Responsabilidades:

- agregar dados para leitura final
- entregar tabelas orientadas a consumo
- reduzir complexidade para o app

Exemplos:

- `mart_pipeline_health_summary`
- `mart_connections_summary`
- `mart_career_progression`
- `mart_profile_summary`

### 6. Camada app

A aplicação Streamlit consome os marts e intermediates para renderizar páginas analíticas por domínio.

## Padrões de engenharia adotados

### Contratos de dados

Cada tabela de ingestão possui um contrato formal em Python com:

- colunas obrigatórias
- colunas sensíveis
- descrição
- owner lógico

Isso reduz fragilidade e melhora governança.

### Persistência centralizada

As escritas em DuckDB foram centralizadas em utilitários únicos, evitando duplicação de conexão, schema setup e lógica de escrita.

### Deduplicação e limpeza reutilizáveis

A camada de ingestão possui funções auxiliares reutilizáveis para:

- normalização de colunas
- garantia de colunas esperadas
- limpeza textual
- parse de datas
- parse de booleanos
- deduplicação

### App com helpers compartilhados

O app Streamlit passou a usar utilitários compartilhados para:

- configuração de página
- carregamento cacheado de queries
- formatação de dados
- renderização de métricas
- renderização de dataframes
- renderização de gráficos

Isso reduz repetição e melhora consistência visual e de código.

## Governança e segurança

### Auditoria de ingestão

Cada carga gera evidência em `bronze.ingestion_audit`, incluindo:

- tabela carregada
- arquivo de origem
- volume carregado
- colunas requeridas
- colunas sensíveis
- horário da carga

### Demo pública sanitizada

O projeto separa:

- base real local
- base demo pública sanitizada

Assim, o app pode ser mostrado sem expor dados sensíveis.

## Qualidade e testes

## Testes Python

Cobrem:

- transformações de ingestão
- deduplicação
- persistência em DuckDB
- auditoria de carga

## Testes dbt

Cobrem:

- `not_null`
- `accepted_values`
- consistência entre marts
- limites lógicos de agregações

## Lint SQL

`sqlfluff` valida estilo, consistência e legibilidade dos modelos SQL.

## Validação completa

O comando:

```powershell
python scripts\run_validation.py
```

executa:

- `pytest`
- bootstrap de warehouse sintético
- `sqlfluff lint`
- `dbt build`

## CI/CD

O workflow em GitHub Actions:

- instala dependências
- executa a validação completa
- não depende dos dados privados do projeto
- usa base sintética para garantir reprodutibilidade

Isso permite validar a arquitetura em qualquer ambiente.

## Decisões arquiteturais importantes

### Por que DuckDB

- simples para ambiente local
- excelente para analytics
- integração muito boa com Python e dbt
- reduz complexidade operacional

### Por que dbt

- separa transformação analítica da ingestão
- melhora documentação e testes de dados
- facilita manutenção e evolução dos modelos

### Por que Streamlit

- entrega rápida de interface
- ótimo para storytelling analítico
- baixo custo de manutenção para um case técnico

## O que esse projeto demonstra tecnicamente

Esse projeto demonstra:

- engenharia de dados ponta a ponta
- modelagem analítica
- design de pipeline
- governança de dados
- testes e qualidade
- organização arquitetural
- preocupação com segurança e publicação

## Como resumir tecnicamente em uma fala curta

> O projeto implementa um mini lakehouse analítico sobre dados exportados do LinkedIn, com ingestão Python, warehouse local em DuckDB, modelagem em dbt, testes automatizados, validação contínua e consumo final via Streamlit.
