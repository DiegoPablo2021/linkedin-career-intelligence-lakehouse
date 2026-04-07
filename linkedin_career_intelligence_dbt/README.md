# dbt Layer

Esta pasta concentra a modelagem analitica do projeto.

## Camadas

- `staging`: padronizacao das tabelas bronze
- `intermediate`: enriquecimentos e classificacoes
- `marts/core`: tabelas finais para consumo analitico

## Como Rodar

Na raiz desta pasta:

```powershell
dbt build --profiles-dir ..\profiles
```

## Responsabilidades

- aplicar regras de limpeza adicionais em SQL
- gerar agregacoes para o Streamlit
- manter testes de qualidade de dados

## Observacao

O profile local do projeto foi movido para `profiles/profiles.yml` para reduzir dependencia de configuracao no ambiente do usuario.
