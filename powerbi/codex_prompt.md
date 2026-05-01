Olá! Quero montar a camada de Power BI deste projeto com base no que já existe no pipeline.

Contexto:

- Este projeto já está pronto em termos de engenharia de dados.
- Ele recebe exportações do LinkedIn, carrega no DuckDB com Python, modela com dbt e já expõe uma camada final em `main.mart_*`.
- A base principal está em `warehouse/linkedin_career_intelligence.duckdb`.
- Já existem estes documentos no projeto:
  - `docs/powerbi_semantic_model.md`
  - `docs/powerbi_export_layer.md`
  - `docs/powerbi_dax_measures.md`
- Quero construir um dashboard profissional, executivo e escalável no Power BI Desktop e futuramente publicar no meu workspace.

O que eu preciso que você faça:

1. Analise o projeto como um todo para entender a arquitetura e as tabelas finais disponíveis.
2. Use os documentos de Power BI já existentes como fonte principal de desenho.
3. Não use os CSVs brutos do LinkedIn como fonte principal do dashboard.
4. Trabalhe em cima da camada final analítica do projeto.
5. Se necessário, crie ou refine a pasta `powerbi/` e a camada `powerbi/exports/`.
6. Se necessário, crie ou refine um script de exportação a partir dos `main.mart_*` para gerar arquivos estáveis para o Power BI.
7. Estruture uma proposta de modelo semântico enxuto com fatos, dimensões e relacionamentos.
8. Considere `dim_date` como tabela calendário robusta, com:
   - data
   - dia
   - dia da semana
   - semana do ano
   - mês
   - trimestre
   - bimestre
   - semestre
   - ano
   - chaves e rótulos de apoio
9. Sugira os relacionamentos necessários entre fatos e dimensões.
10. Organize as medidas DAX em uma tabela `_Measures` e em display folders coerentes.
11. Reaproveite e refine as medidas de `docs/powerbi_dax_measures.md`.
12. Me entregue:
   - a estrutura de arquivos criada ou ajustada
   - as tabelas que devem entrar na V1 do dashboard
   - as medidas DAX recomendadas por tema
   - os relacionamentos recomendados
   - o fluxo exato que eu devo seguir no Power BI Desktop

Restrições e preferências:

- Não altere a lógica validada do pipeline sem necessidade.
- Preserve a arquitetura atual do projeto.
- Se não for viável automatizar a interface do Power BI Desktop, não finja que fez isso.
- Nesse caso, deixe tudo preparado no repositório e me entregue o passo a passo final para eu conectar no Power BI manualmente.

Nome recomendado do arquivo `.pbix`:

- `linkedin_career_intelligence.pbix`

Entregue tudo de forma prática, objetiva e pronta para execução.
