# Publishing Checklist

## GitHub

- confirmar que o `README.md` apresenta proposta, stack e arquitetura
- revisar `.gitignore` para não subir artefatos binários e temporários
- garantir que o app abre localmente com `streamlit run apps\Home.py`
- validar `python scripts\run_pipeline.py`
- manter um commit limpo com documentação atualizada

## Portfólio

- usar o diagrama end-to-end em `docs/images/`
- capturar screenshots das páginas principais
- destacar:
  - problema resolvido
  - stack utilizada
  - camadas do pipeline
  - insights que o app responde
  - diferenciais de engenharia e analytics

## Storytelling sugerido

- entrada: export bruto do LinkedIn com dezenas de arquivos heterogêneos
- processamento: ingestão em Python com limpeza e padronização
- modelagem: dbt com staging, intermediate e marts
- consumo: aplicativo Streamlit com visão analítica por domínio
- operação: health page, inventário técnico e validações

## Deploy

- publicar o código primeiro no GitHub
- decidir se o app ficará em Streamlit Community Cloud ou outra plataforma
- caso use Streamlit Cloud:
  - apontar o repositório
  - definir comando de inicialização para `apps/Home.py`
  - confirmar dependências em `requirements.txt`

## Materiais visuais recomendados

- diagrama da arquitetura
- screenshot da Health
- screenshot de Connections
- screenshot de Jobs ou Learning
- screenshot de Skills ou Profile
