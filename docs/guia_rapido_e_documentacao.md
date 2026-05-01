# Guia Rápido e Documentação do Projeto

Este documento foi pensado para servir em duas frentes:

1. visão explicativa do projeto como um todo
2. visão técnica para desenvolvimento, operação e validação

Também inclui uma lista prática de comandos para usar no terminal.

## Comandos úteis no terminal

### Abrir o projeto no VS Code

```powershell
cd C:\Projetos\linkedIn_career_intelligence_lakehouse
code .
```

### Criar e ativar o ambiente virtual

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Rodar a ingestão

```powershell
python scripts\run_ingestion.py
```

### Rodar o inventário técnico dos exports

```powershell
python scripts\profiling\inventory_linkedin_exports.py
```

### Rodar o pipeline ponta a ponta

```powershell
python scripts\run_pipeline.py
```

### Rodar os testes Python

```powershell
python -m pytest -q
```

### Rodar apenas o teste de ingestão

```powershell
python -m pytest tests\test_ingestion.py -q
```

### Rodar a validação completa

```powershell
python scripts\run_validation.py
```

### Rodar o dbt

```powershell
cd .\linkedin_career_intelligence_dbt
dbt build --profiles-dir ..\profiles
```

### Abrir o app Streamlit

```powershell
streamlit run apps\Home.py
```

### Abrir o app usando a base demo pública

```powershell
$env:LINKEDIN_DB_PATH='demo/linkedin_career_intelligence_demo.duckdb'
streamlit run apps\Home.py
```

### Gerar a base demo pública novamente

```powershell
$env:PYTHONPATH='.'
.\.venv\Scripts\python.exe scripts\utils\create_public_demo_db.py
```

## Frente 1: explicação do projeto como um todo

### O que é este projeto

O `LinkedIn Career Intelligence Lakehouse` transforma arquivos exportados do LinkedIn em uma plataforma analítica local. Em vez de analisar CSVs soltos, o projeto organiza esses dados em um fluxo de engenharia com ingestão, padronização, modelagem, testes e visualização.

### Qual problema ele resolve

Exports do LinkedIn normalmente vêm fragmentados, pouco padronizados e difíceis de explorar de forma confiável. Este projeto resolve isso ao:

- consolidar múltiplos arquivos em uma base analítica única
- limpar e padronizar estruturas diferentes
- aplicar governança e rastreabilidade desde a ingestão
- entregar uma camada final pronta para consumo no app

### O que ele entrega

- pipeline de ingestão em Python
- warehouse analítico local em DuckDB
- modelagem em camadas com dbt
- testes de qualidade em Python e SQL
- dashboard Streamlit com leitura executiva e exploratória
- base demo sanitizada para apresentação pública

### Quais dados entram

O projeto cobre domínios como:

- perfil principal
- conexões
- carreira e posições
- educação
- certificações
- idiomas
- skills
- endorsements
- recomendações recebidas
- companies seguidas
- learning
- candidaturas a vagas
- convites, eventos e voluntariado

### Qual é a ideia central do produto

A proposta não é só mostrar gráficos. A ideia é demonstrar um produto de dados completo, com cara de projeto profissional, usando dados pessoais como matéria-prima para analytics, governança e storytelling.

### Como explicar em uma frase

> É uma plataforma analítica de carreira construída sobre exports do LinkedIn, com pipeline reprodutível, modelagem em camadas, testes e consumo final em Streamlit.

## Frente 2: visão técnica

### Arquitetura geral

```text
LinkedIn CSV Export
-> Python ingestion
-> DuckDB bronze
-> dbt staging
-> dbt intermediate
-> dbt marts
-> Streamlit app
```

### Stack principal

- Python
- Pandas
- DuckDB
- dbt
- Streamlit
- pytest
- sqlfluff
- GitHub Actions

### Estrutura do repositório

- `linkedin_career_intelligence/`: pacote principal com configuração, contratos, ingestão e utilitários
- `tests/`: testes Python de transformação e persistência
- `scripts/`: runners operacionais do pipeline e utilitários
- `linkedin_career_intelligence_dbt/`: modelos, testes e configuração dbt
- `apps/`: aplicação Streamlit
- `data/raw/`: exports originais
- `warehouse/`: base DuckDB local
- `demo/`: base demo sanitizada
- `docs/`: documentação funcional e técnica

### Responsabilidade de cada camada

#### Raw

Guarda os arquivos originais exportados do LinkedIn.

#### Bronze

É a primeira persistência tratada no DuckDB. Nessa etapa o projeto:

- lê CSVs
- normaliza nomes de colunas
- garante colunas esperadas
- converte datas e booleanos
- remove linhas em branco
- deduplica quando necessário
- registra auditoria da carga

#### Staging

Faz padronização leve em dbt para deixar os dados mais previsíveis para transformações seguintes.

#### Intermediate

Concentra enriquecimentos e campos derivados reaproveitáveis.

#### Marts

Entrega tabelas finais orientadas a consumo analítico e ao app.

#### App

Usa os marts para renderizar páginas por domínio com visão executiva e exploração detalhada.

### Ingestão e governança

Cada tabela cadastrada em `linkedin_career_intelligence/ingestion.py` possui:

- nome do arquivo esperado
- tipo de export
- função de transformação
- contrato de dados

Os contratos ajudam a declarar:

- colunas obrigatórias
- colunas sensíveis
- unicidade esperada
- descrição do domínio

Além disso, cada carga bem-sucedida escreve evidências em `bronze.ingestion_audit`.

### Testes e validação

O projeto possui duas frentes principais de qualidade:

- `pytest` para regras Python
- `dbt build` e testes SQL para a camada analítica

O script `scripts\run_validation.py` orquestra a validação completa e usa uma base sintética para não depender dos dados privados.

### App e modo demo

O app resolve automaticamente qual base usar:

- `LINKEDIN_DB_PATH` quando definido
- base privada em `warehouse/`
- base demo em `demo/` quando a privada não existir

Isso permite desenvolvimento com dados reais e apresentação pública com dados sanitizados.

### Ponto forte técnico do projeto

O maior diferencial é a combinação de:

- arquitetura por camadas
- contratos de dados na ingestão
- auditoria de carga
- testes automatizados
- separação entre dado privado e demo pública

## Leitura complementar

- `README.md`
- `docs/project_overview_explanation.md`
- `docs/technical_explanation.md`
- `docs/architecture.md`
- `docs/runbook.md`
- `docs/development.md`
