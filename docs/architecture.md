# Arquitetura

## Camadas

- `data/raw`: arquivos exportados do LinkedIn
- `warehouse`: banco local DuckDB do projeto, persistido no arquivo `warehouse/linkedin_career_intelligence.duckdb`
- `bronze`: persistencia padronizada dos arquivos ingeridos
- `staging`: limpeza e padronizacao em SQL
- `intermediate`: enriquecimento analitico
- `marts`: tabelas finais para consumo
- `apps`: camada de visualizacao e narrativa

## Governanca da Ingestao

- cada tabela suportada possui um contrato explicito na camada Python
- o contrato registra colunas obrigatorias, colunas sensiveis, chave logica e descricao do dominio
- cada execucao de `load_table` grava uma linha em `bronze.ingestion_audit`
- o inventario tecnico de arquivos e a ingestao agora usam o mesmo utilitario de persistencia DuckDB, reduzindo duplicidade operacional

### Objetos de governanca adicionados

- `linkedin_career_intelligence/contracts.py`: contrato e validacao de schema
- `bronze.ingestion_audit`: trilha de auditoria da carga bronze
- `scripts/ingestion/_cli.py`: entrada reutilizavel para scripts unitarios de carga
- `scripts/ci/bootstrap_validation_warehouse.py`: warehouse sintetico para validacao automatizada

## Validacao Arquitetural

- testes Python cobrem transformacoes criticas, append no DuckDB e auditoria de ingestao
- dbt possui testes de schema e testes singulares de consistencia em marts
- `sqlfluff` valida padrao e legibilidade do SQL
- o CI executa a trilha completa sem depender do dataset privado

## Decisao Sobre Notebooks

Notebooks fazem parte do projeto, mas com papel especifico:

- exploracao inicial
- profiling visual
- testes de hipotese
- validacao de regra antes de promover para pipeline

O pipeline oficial continua sendo Python + dbt.
