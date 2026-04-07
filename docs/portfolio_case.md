# Portfolio Case

## Título sugerido

LinkedIn Career Intelligence Lakehouse

## Subtítulo

Transformando exportações pessoais do LinkedIn em um case profissional de engenharia de dados, analytics engineering e data storytelling.

## Resumo curto

Desenvolvi um pipeline end-to-end que recebe os arquivos exportados do LinkedIn, processa os dados com Python, armazena tudo em DuckDB, organiza a camada analítica com dbt e publica os resultados em um app Streamlit com leitura executiva da rede, carreira, perfil, aprendizado e candidaturas.

## Problema

Os exports do LinkedIn trazem muitos arquivos separados, com estruturas heterogêneas e pouco valor analítico imediato. O desafio foi transformar esse material bruto em um produto analítico navegável, confiável e apresentável.

## Solução

- ingestão padronizada em Python para múltiplos CSVs
- inventário técnico para mapear cobertura e falhas de leitura
- modelagem analítica em dbt com `staging`, `intermediate` e `marts`
- aplicação Streamlit com KPIs, gráficos e perguntas de negócio
- documentação e ativos visuais preparados para GitHub e portfólio

## Stack

- Python
- DuckDB
- dbt
- Streamlit
- Matplotlib
- Pytest
- SQLFluff

## Resultados destacados

- 91 arquivos inventariados e 100% de leitura bem-sucedida no cenário atual
- 9.697 conexões analisadas
- 14+ domínios analíticos entregues
- app orientado a narrativa executiva, não apenas a exploração técnica

## Aprendizados

- desenho de pipeline reprodutível para fontes semi-estruturadas
- separação clara entre ingestão, transformação e consumo
- uso de dbt para tornar regras analíticas mais sustentáveis
- importância de UX e storytelling para transformar análise em produto
