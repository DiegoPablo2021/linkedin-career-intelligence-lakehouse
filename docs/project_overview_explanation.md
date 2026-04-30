# Explicação do Projeto

## Visão geral

O **LinkedIn Career Intelligence Lakehouse** é um projeto de engenharia de dados, analytics e visualização que transforma os arquivos exportados do LinkedIn em uma plataforma analítica completa.

Em vez de tratar o export do LinkedIn apenas como um conjunto de CSVs isolados, o projeto organiza esses dados em um fluxo profissional com ingestão, modelagem, governança, testes e consumo em aplicação.

Na prática, ele pega dados pessoais de carreira, networking, educação, certificações, candidaturas, learning e atividade dentro da plataforma e converte isso em uma camada analítica reutilizável.

## O que o projeto entrega

O projeto entrega:

- um pipeline reprodutível de ingestão em Python
- armazenamento analítico local em DuckDB
- modelagem de dados em camadas com dbt
- testes de qualidade para dados e transformações
- uma aplicação Streamlit com leitura executiva e exploratória
- trilha de governança e auditoria da ingestão
- base pública sanitizada para demonstração sem expor dados privados

## Qual problema ele resolve

Esse projeto resolve vários problemas comuns quando alguém trabalha com dados exportados de plataformas:

- os dados vêm fragmentados em muitos arquivos
- não existe modelagem analítica pronta
- os dados brutos não estão limpos para análise
- não há governança, rastreabilidade nem padrão de qualidade
- o consumo final costuma ficar preso em planilhas ou notebooks dispersos

Aqui, tudo isso é organizado como um produto de dados.

## Por que esse projeto é valioso

Ele é valioso porque demonstra capacidade real de trabalhar em um ciclo completo de dados:

- coleta e ingestão
- padronização e limpeza
- modelagem analítica
- governança
- testes
- visualização
- publicação segura

Isso faz o projeto ir muito além de um dashboard. Ele mostra maturidade de engenharia, arquitetura e produto de dados.

## Vantagens de construir esse projeto

As principais vantagens são:

- transformar dados pessoais dispersos em ativos analíticos
- criar um case forte de portfólio em engenharia de dados
- praticar arquitetura por camadas
- exercitar governança e qualidade desde a ingestão
- separar claramente exploração, transformação e consumo
- permitir evolução futura sem retrabalho estrutural

## Domínios analisados

O projeto cobre diferentes dimensões da vida profissional:

- perfil principal
- conexões
- trajetória de carreira
- educação
- certificações
- idiomas
- endorsements
- empresas seguidas
- recomendações recebidas
- skills
- convites
- eventos
- voluntariado
- learning
- candidaturas a vagas
- alertas de vaga
- inventário técnico e saúde do pipeline

## O que a aplicação mostra

O app Streamlit permite navegar pelos dados em páginas por domínio e responder perguntas como:

- como o perfil se posiciona profissionalmente
- como a rede evoluiu ao longo do tempo
- quais empresas e cargos aparecem com mais frequência
- como a trajetória educacional e de certificações se distribui
- quais sinais de aprendizado contínuo existem
- como está a saúde operacional do pipeline

Na Home, o projeto também apresenta um resumo executivo do case, um bloco de autoria com links do profissional e uma sinalização clara quando a execução está usando a demo pública sanitizada.

## Diferenciais do projeto

Os diferenciais mais fortes são:

- arquitetura `raw -> bronze -> staging -> intermediate -> marts -> app`
- contratos de dados na ingestão
- auditoria de carga em `bronze.ingestion_audit`
- app com narrativa executiva, identidade autoral e navegação orientada por domínio
- testes Python, testes dbt e lint SQL
- validação reprodutível em CI com base sintética
- estratégia de demo pública com sanitização dos dados

## Resultado final

O resultado final é um projeto que pode ser explicado como:

> uma plataforma analítica de carreira construída sobre dados exportados do LinkedIn, com padrão profissional de engenharia, governança, qualidade e visualização.

## Quando usar essa explicação

Esse documento é ideal para:

- apresentação em entrevistas
- portfólio
- explicação para recrutadores
- introdução executiva para líderes ou gestores
- abertura de uma demonstração técnica do projeto
