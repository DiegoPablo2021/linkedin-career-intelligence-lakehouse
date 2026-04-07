# Arquitetura

## Camadas

- `data/raw`: arquivos exportados do LinkedIn
- `warehouse`: banco local DuckDB do projeto, persistido no arquivo `warehouse/linkedin_career_intelligence.duckdb`
- `bronze`: persistencia padronizada dos arquivos ingeridos
- `staging`: limpeza e padronizacao em SQL
- `intermediate`: enriquecimento analitico
- `marts`: tabelas finais para consumo
- `apps`: camada de visualizacao e narrativa

## Decisao Sobre Notebooks

Notebooks fazem parte do projeto, mas com papel especifico:

- exploracao inicial
- profiling visual
- testes de hipotese
- validacao de regra antes de promover para pipeline

O pipeline oficial continua sendo Python + dbt.
