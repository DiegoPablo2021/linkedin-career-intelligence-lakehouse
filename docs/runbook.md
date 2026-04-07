# Runbook

## Objetivo

Este runbook descreve o fluxo operacional do projeto: como rodar, validar, depurar e publicar.

## Pré-requisitos

- Python com ambiente virtual
- dependências instaladas via `requirements.txt`
- dbt configurado com `profiles/profiles.yml`
- exports do LinkedIn em `data/raw/`

## Setup inicial

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Execução padrão

### 1. Ingestão

```powershell
python scripts\run_ingestion.py
```

Resultado esperado:

- tabelas `bronze.*` atualizadas no DuckDB
- sem erro de leitura nos domínios suportados

### 2. Inventário técnico

```powershell
python scripts\profiling\inventory_linkedin_exports.py
```

Resultado esperado:

- `bronze.file_inventory` atualizado
- contagem de arquivos e leitura refletida na página `Health`

### 3. Build analítico com dbt

Execute a partir da pasta `linkedin_career_intelligence_dbt/`.

```powershell
dbt build --profiles-dir ..\profiles
```

Resultado esperado:

- `stg_*`, `int_*` e `mart_*` atualizados
- testes dbt concluídos com sucesso

### 4. Aplicação Streamlit

```powershell
streamlit run apps\Home.py
```

Resultado esperado:

- menu lateral navegável
- páginas carregando sem erro
- indicadores e tabelas refletindo o warehouse local

## Execução ponta a ponta

```powershell
python scripts\run_pipeline.py
```

Use quando quiser rodar ingestão, inventário e modelagem numa mesma sequência.

## Checagens rápidas

### Validar sintaxe Python

```powershell
python -m py_compile apps\Home.py
```

### Inspecionar o warehouse

```powershell
python scripts\utils\inspect_warehouse.py
```

### Rodar testes Python

```powershell
pytest
```

## Troubleshooting

### Problema: arquivo `.duckdb` não abre no editor

Explicação:

- `.duckdb` é binário, não é arquivo texto

Ação:

- usar `inspect_warehouse.py`
- usar consultas via `run_query`

### Problema: aparecem arquivos `pyc`, `target`, `logs` ou `dbt_packages`

Explicação:

- são artefatos temporários de execução

Ação:

- manter ignorados pelo Git
- usar as configurações do editor para reduzir ruído visual

### Problema: a página `Health` acusa falhas de leitura

Ação:

1. rodar `inventory_linkedin_exports.py`
2. identificar arquivo problemático pelo inventário
3. ajustar parsing na ingestão ou no próprio inventário
4. rerodar ingestão e build

### Problema: gráfico fica ruim com apenas uma categoria

Explicação:

- alguns domínios têm cardinalidade muito baixa no dataset atual

Ação:

- preferir cards, progressos, comparativos e leitura textual
- evitar “barras únicas” quando não agregarem valor

## Publicação no GitHub

### Nome sugerido do repositório

`linkedin-career-intelligence-lakehouse`

### Fluxo recomendado

1. criar repositório vazio no GitHub
2. autenticar o `gh`
3. versionar e subir o projeto

### Login no GitHub CLI

```powershell
& 'C:\Program Files\GitHub CLI\gh.exe' auth login
```

### Comandos de push

```powershell
git add .
git commit -m "feat: publish LinkedIn Career Intelligence Lakehouse"
git remote add origin https://github.com/DiegoPablo2021/linkedin-career-intelligence-lakehouse.git
git push -u origin main
```

## Checklist antes do push

- README revisado
- LICENSE presente
- docs complementares criadas
- app navegável
- health sem falhas de leitura
- sem dados sensíveis desnecessários no repositório
