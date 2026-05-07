# Camada Power BI

## Propósito

Este documento descreve o design técnico da camada Power BI que fica sobre o LinkedIn Career Intelligence Lakehouse. O foco está em semantic modeling, estratégia de DAX, rendering com HTML/CSS, integração com observability, decisões de layout responsivo e tradeoffs necessários para sustentar um report de nível executivo dentro do Power BI Desktop.

A intenção desta camada não é compensar uma modelagem fraca na origem. O objetivo é expor uma experiência analítica governada, performática e adequada para recrutadores sobre um warehouse e uma estrutura de marts bem desenhados.

## 1. Arquitetura Da Camada Power BI

A camada Power BI consome exports curados derivados de DuckDB e dbt, e não arquivos brutos do LinkedIn.

```text
DuckDB + dbt marts
-> Power BI export layer
-> semantic model
-> DAX measures
-> HTML/CSS rendering layer
-> executive pages
```

### Princípios Arquiteturais

- manter ingestion e transformation fora do Power BI
- manter o semantic model legível e com escopo intencional
- centralizar a lógica de negócio em `_Measures`
- isolar o comportamento histórico em snapshot facts
- isolar observability e governance em facts e folders dedicados
- usar HTML Content para controle de layout, não para lógica de negócio

### Por Que Esta Arquitetura Foi Escolhida

O Power BI funciona melhor quando consome entidades analíticas estáveis. O uso de marts curados e snapshot facts reduz:

- fragilidade do modelo
- duplicação de lógica de transformação
- complexidade de troubleshooting
- esforço de manutenção do report

Além disso, melhora:

- clareza semântica
- reutilização de measures
- portabilidade do report
- confiança nas definições de KPI

## 2. Organização De Pastas De Measures

O projeto usa uma tabela centralizada `_Measures` e separa as responsabilidades por meio de display folders.

### Pastas Relevantes

| Folder | Finalidade |
|---|---|
| `02 | Networking` | lógica de networking e relacionamento |
| `14 | HTML Content` | measures de renderização de páginas em HTML/CSS |
| `15 | Observability` | measures de saúde do pipeline e data quality |
| `16 | Historical Snapshots` | measures históricas e orientadas a snapshot |

### Por Que As Pastas Importam

Organização de folders não é estética. Ela ajuda a:

- tornar a intenção semântica visível
- separar measures de UX de measures analíticas
- reduzir ambiguidade em manutenção
- apoiar onboarding e review em contexto corporativo

## 3. Estratégia De Rendering Das HTML Measures

O report usa DAX measures que retornam strings HTML para o visual `HTML Content` do Power BI.

### Modelo De Rendering

Cada página HTML é uma measure única que:

1. resolve measures analíticas em variáveis formatadas
2. compõe a string de layout HTML
3. embute CSS inline para portabilidade
4. rerenderiza sob o filtro do Power BI

### Por Que HTML Measures Foram Usadas

Os visuais nativos são fortes para análise padrão, mas limitam o controle de layout em nível executivo. A camada HTML foi introduzida para fornecer:

- cabeçalhos executivos mais premium
- design system consistente por página
- narrative cards
- hierarquia visual mais rica
- controle fino de espaçamento e composição

### O Que HTML Measures Não Fazem

- não substituem semantic modeling
- não introduzem lógica de interação independente
- não devem executar computação que deveria ficar em measures analíticas
- não usam JavaScript

O padrão correto é:

```text
DAX measures compute metrics
-> HTML measure formats and arranges them
-> slicers and relationships drive the context
```

## 4. Decisões De CSS E Layout

Todas as páginas HTML foram desenhadas para um viewport restrito do Power BI, e não para uma página web sem limites.

### Estrutura Base Do Wrapper

O padrão do wrapper raiz foi padronizado em torno de:

```css
width: 100%;
height: 100%;
max-height: 100%;
overflow: hidden;
box-sizing: border-box;
```

### Por Que Isso Importa

O visual HTML do Power BI tem um container fixo. Se o HTML se comporta como uma página web comum, o resultado tende a ser:

- barras de rolagem verticais
- cabeçalhos cortados
- densidade visual inconsistente
- quebra de alinhamento entre páginas

O wrapper reduz esses riscos e mantém cada página alinhada ao canvas do report.

### Design System

O design system aprovado para Power BI é:

- background: `#0b1220`
- primary cards: `#111827`
- borders: `#1f2937`
- primary text: `#f9fafb`
- secondary text: `#94a3b8`
- blue accent: `#2563eb`
- green success: `#10b981`
- amber warning: `#f59e0b`
- red critical: `#ef4444`

### Por Que O CSS Inline Foi Mantido

O CSS inline torna as HTML measures:

- autocontidas
- portáveis dentro do semantic model
- mais fáceis de publicar via updates de measure em TOM/XMLA

O tradeoff é manutenção. Para compensar, os padrões visuais foram repetidos com consistência entre as measures.

## 5. Estratégia De Compaction Responsiva

O maior problema de UX não foi styling. Foi fit.

As páginas HTML do Power BI precisam continuar legíveis dentro de uma altura fixa, sem scroll interno.

### Abordagem De Compaction

A estratégia final usa microajustes controlados, e não redesenho estrutural:

- reduzir padding do wrapper alguns pixels
- reduzir hero height somente quando necessário
- reduzir gaps verticais de forma seletiva
- compactar os bottom narrative cards antes de mexer na tipografia dos KPIs
- evitar `min-height` exagerado
- manter `overflow:hidden` nos wrappers principais

### Por Que Essa Abordagem Foi Escolhida

As páginas atendem um público executivo. Escalonamento agressivo prejudicaria credibilidade visual. A estratégia de compaction, portanto, privilegia:

- preservação da hierarquia
- preservação da identidade visual
- preservação da proeminência dos KPIs
- redução apenas do whitespace residual não essencial

### Tradeoff

Essa abordagem é mais lenta do que redesenhar as páginas, mas preserva o visual system aprovado e evita retrabalho no report inteiro.

## 6. Decisões De UX Executiva

A camada Power BI foi desenhada como um produto de analytics corporativo, e não como um report padrão.

### Padrões De UX Utilizados

- hero sections com contexto visual forte
- grids de KPI compactos
- blocos analíticos com propósito explícito
- narrative cards para interpretação
- badges de governance e observability
- microcopy executivo

### Por Que Essas Escolhas Importam

Dashboards executivos não servem apenas para mostrar números. Eles precisam:

- estabelecer contexto imediatamente
- reduzir tempo de interpretação
- comunicar confiança
- diferenciar insight de output bruto

### Implicações Para Portfolio

Isso demonstra:

- disciplina semântica
- product thinking
- julgamento de front-end dentro das restrições do Power BI
- controle sênior da UX de analytics

## 7. Measures De Snapshot

Measures históricas foram adicionadas em `16 | Historical Snapshots` para enriquecer o semantic model com sinais orientados a tendência.

### Exemplos

- `Snapshot Connections`
- `Snapshot Applications`
- `Snapshot Events`
- `Snapshot Invitations`
- `Snapshot Recommendations`
- `Snapshot Career Maturity Score %`
- `Snapshot Presence Score %`
- `Snapshot Engagement Score %`
- `Snapshot Data Quality Rows`
- `Snapshot Avg Null Rate %`
- `Snapshot Max Null Rate %`
- `Snapshot Data Quality Alerts`
- `Snapshot Read Success Rate %`
- `Snapshot Row Retention Rate %`
- `Historical Snapshot Count`
- `Actual Snapshot Count`
- `Simulated Snapshot Count`

### Filosofia De Design

As measures históricas foram criadas para:

- comparar comportamento atual e histórico
- enriquecer a narrativa das páginas sem depender de APIs externas
- preservar provenance governada por meio de metadata de snapshot

### Tradeoff

Snapshot measures aumentam a densidade de insight, mas também aumentam a complexidade semântica. Por isso, ficaram isoladas em sua própria pasta e separadas das measures de observability operacional.

## 8. Measures De Observability

As measures de observability ficam em `15 | Observability` e focam na saúde da ingestion e da data quality.

### Famílias Principais

- load counts
- taxas de sucesso e falha
- retention rates
- indicadores de remoção de linhas
- indicadores de duplicidade
- indicadores de null rate
- indicadores de freshness e latest load
- composite health scores

### Por Que Esta Camada Existe

Sem observability, o dashboard pode exibir KPIs bem apresentados enquanto esconde fragilidade operacional. Esta camada transforma o report em uma plataforma que consegue defender sua própria confiabilidade.

### Preocupação De Escala

À medida que a densidade de observability cresce, existe o risco de transformar um semantic model em um operations console. A separação entre páginas de governance e de observability mantém esse escopo controlado.

## 9. Measures De Governança

As measures de governança foram desenhadas para expressar analytical trust, e não apenas status técnico.

### Tópicos Comuns

- read success e failure
- inventory footprint
- asset freshness
- quality readiness
- traceability signals
- auditability indicators

### Por Que Governance É Separada De Observability

Observability responde:

- o que aconteceu no pipeline
- onde a qualidade degradou

Governance responde:

- se esses dados podem ser confiados para consumo executivo
- se a camada analítica está operationally ready

Essa separação melhora tanto a narrativa quanto a manutenção.

## 10. Decisões De Performance

A camada Power BI evita usar HTML como motor de computação.

### Regras De Performance Seguidas

- calcular métricas em DAX measures dedicadas primeiro
- formatar apenas os valores finais dentro das HTML measures
- manter o desenho de relacionamentos simples e unidirecional
- evitar fact-to-fact relationships
- consumir exports curados em vez de fontes brutas ou excessivamente largas

### Por Que Isso Importa

HTML measures são difíceis de manter quando viram containers de lógica. Manter a lógica upstream ou em measures analíticas reduz:

- tempo de debugging
- ambiguidade de recomputação
- regressões de performance sob mudança de slicers

### Otimizações Semânticas Relacionadas

- date dimension dedicada
- historical snapshot facts separados por subject area
- helper dimensions para filtros de observability
- measures centralizadas em `_Measures`

## 11. Filosofia De DAX

A camada DAX segue uma filosofia voltada a produção.

### Princípios

- manter measures explícitas
- usar `COALESCE` para evitar blanks desnecessários em views executivas
- separar formatação de aritmética quando possível
- criar synthetic scores apenas quando a lógica for explicável
- preferir clareza semântica em vez de esperteza

### Por Que Esses Princípios Foram Escolhidos

Reporting executivo exige confiança mais do que novidade. As measures devem ser:

- inspecionáveis
- testáveis
- mantíveis
- reutilizáveis entre páginas

### Tradeoff

Algumas fórmulas visuais dentro de HTML measures calculam scores leves de exibição inline. Isso é aceitável para sinais de UX, mas o padrão mais forte no longo prazo é externalizar a lógica reutilizável para measures analíticas quando o reuso aumentar.

## 12. Padrões Reutilizáveis De Blocos HTML

Embora as measures sejam específicas por página, elas reutilizam padrões estáveis de UI.

### Padrões Comuns

- hero header com badges contextuais
- KPI grid card
- analytical block card
- narrative / insight card
- governance status chip
- compact metric row

### Por Que Reuso Importa

A reutilização mantém o report:

- visualmente coerente
- mais fácil de compactar
- mais fácil de revisar
- mais fácil de evoluir página por página

### Implicação De Manutenção

Quanto mais esses padrões permanecerem consistentes, mais fácil fica automatizar updates por meio de scripts de measures sem drift visual.

## 13. Limitações E Workarounds

### Limitações Do HTML Content

- sem JavaScript
- sem drill interno nativo
- sem interação arbitrária com DOM
- viewport limitado pelo container do visual Power BI

### Workarounds Usados

- slicers externos dirigem o contexto
- variáveis DAX dirigem o texto dinâmico e o rendering dos KPIs
- layouts compactos evitam scroll no container
- compaction responsiva é feita measure a measure

### Limitações Semânticas

- os snapshots são tão completos quanto o histórico de eventos subjacente
- as timelines de observability dependem da riqueza do audit do pipeline
- o report favorece narrativa executiva governada em vez de exploração livre

## 14. Próximas Evoluções Possíveis

- extrair templates comuns de blocos HTML para utilitários mais seguros
- adicionar validação semântica automática para fit das HTML measures
- formalizar lint de measures para folders, descriptions e formatação
- expandir páginas históricas para consumir mais trends de snapshot diretamente
- melhorar automação de deployment para updates via TOM/XMLA
- adicionar governance de release para regressões de UX do report

## Avaliação Final

A camada Power BI é posicionada intencionalmente como a camada final de semântica e UX de um sistema de analytics com mentalidade corporativa.

O valor vem da combinação de:

- upstream data governada
- semantic model controlado
- apresentação executiva premium
- inteligência histórica
- observability operacional

É essa combinação que torna o report mais forte do que um dashboard padrão e mais próximo de um produto analítico de produção.
