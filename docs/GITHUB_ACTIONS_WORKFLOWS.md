# Workflows Do GitHub Actions

## Visão Geral

O repositório usa dois workflows distintos no GitHub Actions:

- `CI`: validação de qualidade e build em `push` e `pull_request`
- `Operational Pipeline`: execução semi-automatizada do pipeline disparada manualmente com `workflow_dispatch`

Essa separação mantém os checks de software isolados das operações de dados.

## Workflow De CI

Arquivo: `.github/workflows/ci.yml`

Modelo de trigger:

- roda em todo `push`
- roda em todo `pull_request`
- não roda em `workflow_dispatch`

Responsabilidades:

- instalar dependências Python
- executar `pytest`
- compilar os pacotes Python como smoke test de packaging
- bootstrapar uma DuckDB synthetic warehouse
- executar `sqlfluff lint`
- executar `dbt build`

Objetivo:

- validar qualidade de código
- validar o projeto dbt
- validar a saúde do repositório sem depender de exports privados do LinkedIn

## Workflow Operacional

Arquivo: `.github/workflows/operational-pipeline.yml`

Modelo de trigger:

- roda apenas via `workflow_dispatch`

Responsabilidades no modo `manual_exports`:

1. ler os exports manuais do LinkedIn colocados em `data/raw`
2. ingerir os source files em DuckDB
3. atualizar `bronze.file_inventory`
4. reconstruir dbt staging, intermediate e marts
5. reconstruir snapshots históricos
6. regenerar exports do Power BI
7. validar os outputs de observability
8. publicar a warehouse e os artifacts do Power BI gerados

Responsabilidades no modo `validation_fixture`:

1. bootstrapar uma synthetic validation warehouse
2. executar `dbt build`
3. reconstruir snapshots
4. exportar artifacts do Power BI
5. validar outputs de observability
6. publicar os validation artifacts

Objetivo:

- manter a ingestion dos exports manual
- automatizar toda a camada downstream
- suportar validação no GitHub sem expor exports pessoais privados

## Layout Dos Exports Manuais

Estrutura recomendada:

```text
data/raw/
├─ basic_export_2026_05_07/
│  ├─ Profile.csv
│  └─ ...
└─ complete_export_2026_05_07/
   ├─ Connections.csv
   ├─ Jobs/Job Applications.csv
   └─ ...
```

Regras de resolução:

- `LINKEDIN_BASIC_EXPORT_DIR` ou `LINKEDIN_COMPLETE_EXPORT_DIR` têm prioridade
- caso contrário, o pipeline varre `data/raw`
- a pasta válida mais recente que siga `basic_export_YYYY_MM_DD` ou `complete_export_YYYY_MM_DD` é selecionada automaticamente
- se um tipo estiver ausente, o resolver tenta um fallback compatível que ainda contenha os arquivos necessários
- se nenhuma pasta compatível existir, o pipeline falha antes de iniciar a ingestion

## UX Operacional

O runner operacional foi desenhado para ser explícito:

- imprime os diretórios de export resolvidos antes de iniciar a ingestion
- mostra se a resolução veio de env override, dated discovery ou fallback
- exibe warnings quando um fallback está sendo usado
- mantém os outputs locais do Power BI em um diretório consistente e publica artifacts no GitHub Actions

## Riscos Operacionais No GitHub Actions

O principal risco operacional em GitHub-hosted Actions é disponibilidade de entrada:

- `manual_exports` exige que as pastas de export existam no workspace do runner
- runners hospedados pelo GitHub são efêmeros, então a operação mensal com dados reais normalmente exige um self-hosted runner ou uma etapa controlada de staging de artifacts
- `validation_fixture` é seguro para validação de workflow porque não requer exports privados

O workflow está pronto para operação mensal contínua desde que os exports sejam staged de forma intencional antes de disparar `manual_exports`.

## Uso Recomendado

Execução local com exports reais:

```powershell
python scripts\run_pipeline.py --mode manual_exports
```

Validação local sem exports privados:

```powershell
python scripts\run_pipeline.py `
  --mode validation_fixture `
  --db-path tmp/local-validation.duckdb `
  --powerbi-output-dir tmp/powerbi_exports `
  --powerbi-format both
```

Validação no GitHub Actions:

- dispare `Operational Pipeline`
- escolha `execution_mode=validation_fixture`
- inspecione os artifacts publicados do Power BI e do DuckDB

Execução manual no GitHub Actions com exports reais:

- use um runner que tenha as pastas de export disponíveis no workspace
- dispare `Operational Pipeline`
- defina `execution_mode=manual_exports`
- opcionalmente passe `basic_export_dir` e `complete_export_dir`
