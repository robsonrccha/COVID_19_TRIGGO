# Estrutura do Esquema Snowflake para Análise de Ocupação de Leitos

## Visão Geral

Este projeto implementa uma solução completa de engenharia de dados para análise de ocupação de leitos hospitalares durante a pandemia de COVID-19, utilizando dados do DataSUS. A solução é construída sobre o Snowflake como data warehouse e utiliza dbt (data build tool) para transformações e modelagem dimensional, seguindo as melhores práticas de engenharia de dados moderna.

O projeto abrange dados de ocupação de leitos de 2021, organizados em um esquema estrela (star schema) otimizado para análises de saúde pública. A arquitetura permite análises temporais detalhadas, comparações geográficas e insights sobre padrões de ocupação hospitalar durante diferentes fases da pandemia.



## Como Rodar o Projeto

Para configurar e executar este projeto dbt, siga os passos abaixo:

### Pré-requisitos

Certifique-se de ter as seguintes ferramentas instaladas e configuradas:

*   **Snowflake:** Uma conta ativa no Snowflake com as permissões necessárias para criar bancos de dados, schemas, tabelas e stages.
*   **dbt Core:** Instalado e configurado para se conectar à sua conta Snowflake. Você pode instalá-lo via pip:
    ```bash
    pip install dbt-snowflake
    ```
*   **Git:** Para controle de versão do seu projeto dbt.

### Configuração do Snowflake

Certifique-se de que os seguintes bancos de dados e schemas existam em sua conta Snowflake. Eles são utilizados para organizar os dados brutos (BRONZE) e os dados modelados (GOLD) por ano:

*   `COVID_2021`
    *   `BRONZE`
    *   `GOLD`

Além disso, o stage interno para o upload do arquivo CSV deve estar configurado:

*   `COVID_2021.BRONZE.LEITO_OCUPACAO_2021`

O arquivo CSV (e.g., `esus-vepi.LeitoOcupacao_2021.csv`) deve ser carregado para seu respectivo stage. Um exemplo de comando SQL para criar a tabela de staging e carregar os dados é:

```sql
-- Tabela de Staging para 2021
CREATE TABLE IF NOT EXISTS COVID_2021.BRONZE.STG_LEITO_OCUPACAO_2021 (
UNNAMED_0 INT,
_ID VARCHAR,
DATA_NOTIFICACAO TIMESTAMP_NTZ,
CNES VARCHAR,
OCUPACAO_SUSPEITO_CLI FLOAT,
OCUPACAO_SUSPEITO_UTI FLOAT,
OCUPACAO_CONFIRMADO_CLI FLOAT,
OCUPACAO_CONFIRMADO_UTI FLOAT,
OCUPACAO_COVID_UTI FLOAT,
OCUPACAO_COVID_CLI FLOAT,
OCUPACAO_HOSPITALAR_UTI FLOAT,
OCUPACAO_HOSPITALAR_CLI FLOAT,
SAIDA_SUSPEITA_OBITOS FLOAT,
SAIDA_SUSPEITA_ALTAS FLOAT,
SAIDA_CONFIRMADA_OBITOS FLOAT,
SAIDA_CONFIRMADA_ALTAS FLOAT,
ORIGEM VARCHAR,
P_USUARIO VARCHAR,
ESTADO_NOTIFICACAO VARCHAR,
MUNICIPIO_NOTIFICACAO VARCHAR,
ESTADO VARCHAR,
MUNICIPIO VARCHAR,
EXCLUIDO BOOLEAN,
VALIDADO BOOLEAN,
CREATED_AT TIMESTAMP_NTZ,
UPDATED_AT TIMESTAMP_NTZ
);

-- Carregamento dos dados para 2021
COPY INTO COVID_2021.BRONZE.STG_LEITO_OCUPACAO_2021
FROM @COVID_2021.BRONZE.LEITO_OCUPACAO_2021
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = "," SKIP_HEADER = 1 EMPTY_FIELD_AS_NULL = TRUE)
ON_ERROR = 'CONTINUE';
```

### Estrutura do Projeto dbt

O projeto dbt deve seguir a seguinte estrutura de diretórios:

```
dbt_project/
├── models/
│   ├── staging/
│   │   ├── stg_leito_ocupacao_2020.sql
│   │   ├── stg_leito_ocupacao_2021.sql
│   │   ├── stg_leito_ocupacao_2022.sql
│   │   └── stg_leito_ocupacao_unified.sql
│   ├── dimensions/
│   │   ├── dim_tempo.sql
│   │   ├── dim_localidade.sql
│   │   ├── dim_unidade_saude.sql
│   │   └── dim_ocupacao_tipo.sql
│   └── facts/
│       └── fact_ocupacao_leitos.sql
├── dbt_project.yml
├── profiles.yml (ou use o global ~/.dbt/profiles.yml)
└── README.md
```

### Execução do Projeto dbt

Após configurar o Snowflake, navegue até o diretório raiz do seu projeto dbt no terminal e execute os seguintes comandos:

1.  **Testar a conexão com o Snowflake:**
    ```dbt debug```

2.  **Compilar os modelos dbt (opcional, para verificar a sintaxe):**
    ```dbt compile```

3.  **Executar os modelos dbt e construir o data warehouse:**
    ```dbt run ```

4.  **Executar os testes definidos nos modelos:**
    ```dbt test```

5.  **Gerar a documentação do dbt:**
    ```dbt docs generate```



## Arquitetura da Solução

A arquitetura da solução é baseada em um modelo de data warehouse moderno, utilizando o Snowflake como plataforma de dados e o dbt para orquestração e transformação. O fluxo de dados segue uma abordagem de múltiplas camadas, garantindo modularidade, rastreabilidade e escalabilidade.

### Fluxo de Dados

1.  **Coleta e Ingestão (Staging/Bronze):** Os dados brutos, provenientes de arquivos CSV do DataSUS, são carregados para uma área de staging no Snowflake. Esta camada, denominada `BRONZE`, armazena os dados em seu formato original, sem transformações complexas. O carregamento é realizado via comandos `COPY INTO` do Snowflake, utilizando stages internos.

2.  **Transformação e Modelagem (dbt/Gold):** O dbt é a ferramenta central para a transformação e modelagem dos dados. A partir da camada `BRONZE`, o dbt aplica lógica de negócio, limpa, padroniza e agrega os dados, construindo as tabelas dimensionais e de fatos. Esta camada, denominada `GOLD`, representa o modelo dimensional otimizado para análises.

3.  **Consumo:** Os dados modelados na camada `GOLD` estão prontos para serem consumidos por ferramentas de Business Intelligence (BI), dashboards e relatórios, fornecendo insights acionáveis para a saúde pública.

### Camadas do Snowflake

*   **BRONZE:** Contém os dados brutos, exatamente como foram ingeridos. É a camada de staging.
*   **GOLD:** Contém os dados transformados e modelados em um esquema estrela, prontos para consumo analítico. É a camada de apresentação.

### Papel do dbt

O dbt desempenha um papel crucial na pipeline, sendo responsável por:

*   **Transformação de Dados:** Define e executa as transformações SQL para limpar, padronizar e enriquecer os dados.
*   **Modelagem Dimensional:** Constrói as tabelas de dimensão (`DIM_TEMPO`, `DIM_LOCALIDADE`, `DIM_UNIDADE_SAUDE`, `DIM_OCUPACAO_TIPO`) e a tabela de fatos (`FACT_OCUPACAO_LEITOS`) de acordo com o esquema estrela.
*   **Materialização:** Gerencia a materialização das tabelas no Snowflake (views, tabelas, incrementais), otimizando o desempenho e o custo.
*   **Testes de Dados:** Implementa testes automatizados para garantir a integridade, unicidade e consistência dos dados em cada estágio da transformação.
*   **Documentação:** Gera automaticamente a documentação do modelo de dados e das transformações, promovendo a transparência e o conhecimento compartilhado.

### Esquema Snowflake (Star Schema)

O modelo dimensional proposto segue um esquema estrela, otimizando o desempenho de consultas analíticas e facilitando a compreensão dos dados. Ele é composto por uma tabela de fatos central e várias tabelas de dimensão:

*   **FACT_OCUPACAO_LEITOS:** Tabela de fatos central que contém as métricas quantitativas (ocupação de leitos, óbitos, altas) e as chaves estrangeiras para as tabelas de dimensão.
*   **DIM_TEMPO:** Dimensão que armazena informações detalhadas sobre as datas de notificação, permitindo análises temporais flexíveis.
*   **DIM_LOCALIDADE:** Dimensão que armazena informações geográficas (estado e município).
*   **DIM_UNIDADE_SAUDE:** Dimensão que contém informações sobre as unidades de saúde (CNES).
*   **DIM_OCUPACAO_TIPO:** Dimensão que categoriza os tipos de ocupação de leitos (suspeito, confirmado, COVID, hospitalar) e o tipo de leito (clínico, UTI).

Este design permite que os analistas consultem facilmente os dados de ocupação de leitos por diferentes dimensões (tempo, localidade, unidade de saúde, tipo de ocupação), facilitando a identificação de tendências e padrões.



## Decisões de Design e Justificativas


### 1. Escolha do Snowflake como Data Warehouse

*   **Justificativa:** O Snowflake foi escolhido por sua arquitetura de computação e armazenamento desacoplados, que oferece escalabilidade elástica, performance otimizada para cargas de trabalho analíticas e um modelo de precificação baseado em uso. Sua capacidade de lidar com grandes volumes de dados semi-estruturados e estruturados, juntamente com recursos como Time Travel e Zero-Copy Cloning, o torna ideal para um projeto de dados de saúde pública que pode exigir auditoria e experimentação.

### 2. Utilização do dbt para Transformação e Modelagem

*   **Justificativa:** O dbt (data build tool) foi selecionado para gerenciar as transformações de dados devido à sua abordagem de "analytics engineering". Ele permite que os engenheiros de dados construam pipelines de transformação usando SQL, aplicando princípios de engenharia de software como controle de versão, testes automatizados e documentação. Isso resulta em um código SQL mais modular, testável e reutilizável, acelerando o desenvolvimento e melhorando a qualidade dos dados.

### 3. Esquema Estrela (Star Schema) para Modelagem Dimensional

*   **Justificativa:** A adoção de um esquema estrela com tabelas de dimensão e uma tabela de fatos central (`FACT_OCUPACAO_LEITOS`) é uma prática recomendada para data warehouses. Este design otimiza o desempenho de consultas analíticas, pois minimiza o número de junções necessárias para recuperar dados. Além disso, facilita a compreensão do modelo de dados por parte dos usuários de negócio e das ferramentas de BI, promovendo a autoatendimento e a exploração de dados.

### 4. Materializações Específicas no dbt

*   **`DIM_TEMPO`, `DIM_LOCALIDADE`, `DIM_UNIDADE_SAUDE`, `DIM_OCUPACAO_TIPO` como `TABLE` ou `VIEW`:
    *   **Justificativa:** As tabelas de dimensão são materializadas como `TABLE` para garantir performance em consultas, pois seus dados são relativamente estáticos e não mudam com frequência. Em alguns casos, `VIEW` pode ser usada para dimensões menores ou que precisam refletir dados em tempo quase real, mas para este projeto, a performance de `TABLE` é preferível. A `DIM_OCUPACAO_TIPO` é um exemplo de dimensão pequena que pode ser uma `VIEW` ou até mesmo um `SEED` no dbt, pois seus valores são fixos.

*   **`FACT_OCUPACAO_LEITOS` como `INCREMENTAL`:
    *   **Justificativa:** A tabela de fatos (`FACT_OCUPACAO_LEITOS`) é materializada como `INCREMENTAL` devido ao seu potencial volume de dados e à natureza contínua da ingestão de dados de ocupação de leitos. A materialização incremental permite que o dbt processe apenas os novos registros desde a última execução, reduzindo significativamente o tempo de execução e o custo de computação no Snowflake, em vez de reconstruir a tabela inteira a cada vez. A coluna `updated_at` é utilizada para identificar os novos registros.

### 5. Tratamento de Nulos e Padronização de Dados

*   **Justificativa:** A análise exploratória dos dados brutos revelou a presença de valores nulos e inconsistências (e.g., `cnes` com tipos mistos). Decisões foram tomadas para tratar esses casos:
    *   **Métricas (e.g., `ocupacao_suspeito_cli`):** Valores nulos são substituídos por `0` (zero) usando `COALESCE`. Isso garante que as métricas sejam numéricas e que a ausência de dados seja interpretada como zero ocupação ou saída, o que é uma suposição razoável para este contexto.
    *   **Atributos Categóricos (e.g., `estado`, `municipio`):** Valores nulos são substituídos por `'Desconhecido'` usando `COALESCE`. Isso evita que registros sejam descartados devido a dados ausentes em dimensões e permite que a análise inclua esses casos, categorizando-os de forma explícita.
    *   **`CNES`:** A coluna `cnes` é padronizada para `VARCHAR` e tem seus espaços em branco removidos (`TRIM`). Isso resolve o problema de tipos mistos e garante a consistência para junções com a `DIM_UNIDADE_SAUDE`.


### 6. Testes de Dados com dbt

*   **Justificativa:** A inclusão de testes automatizados com o dbt é essencial para garantir a qualidade, confiabilidade e integridade dos dados utilizados nas análises. Estes testes ajudam a identificar erros, duplicidades, dados inválidos ou inconsistentes de forma precoce na pipeline, reduzindo o risco de geração de insights incorretos.

*   **Testes Implementados no Projeto:**

    - **`test_no_future_dates.sql`**  
      Este teste verifica se há datas no futuro na tabela `fact_ocupacao_leitos`. Ele garante que não haja registros com `data_notificacao` maior que a data atual, o que indicaria erro de entrada.  

*   **Justificativa:** A inclusão de testes de dados (e.g., `test_no_future_dates.sql`) é fundamental para garantir a qualidade e a integridade dos dados transformados. Testes automatizados ajudam a identificar problemas precocemente na pipeline, como datas futuras ou chaves duplicadas, garantindo que os insights gerados sejam baseados em dados confiáveis.

    - **Relacionamentos (`relationships`)**  
      Garante que todas as chaves estrangeiras na `fact_ocupacao_leitos` realmente existam nas respectivas tabelas dimensionais. Evita registros "órfãos", que prejudicariam as análises.
        * `id_tempo` → `dim_tempo`
        * `id_localidade` → `dim_localidade`
        * `id_unidade_saude` → `dim_unidade_saude`
        * `id_ocupacao_tipo` → `dim_ocupacao_tipo`

    - **Chaves primárias únicas e não nulas (`unique`, `not_null`)**  
      Validam a integridade das dimensões e da tabela de fatos, garantindo que:
        * `id_tempo`, `id_localidade`, `id_ocupacao_tipo`, `id_fato` sejam únicos e não nulos.

    - **Validação de coluna `updated_at`**  
      Este campo é essencial para o funcionamento da materialização incremental. O teste `not_null` nessa coluna assegura que todos os registros tenham data de atualização válida.

*   **Justificativa:** Esses testes foram definidos no arquivo `schema.yml` do projeto e ajudam a manter o pipeline de dados confiável e auditável.



Essas decisões de design, em conjunto, formam uma solução robusta e eficiente para a análise de ocupação de leitos, alinhada com as melhores práticas de engenharia de dados e otimizada para o ambiente Snowflake.



### 7. Automação da Pipeline: Ingestão + Atualização via Task e Job

#### Parte 1: Task no Snowflake para ingestão automatizada

1. Foi criada uma task no Snowflake que executa automaticamente a cada 30 dias, baseada nos dados públicos do OpenDataSUS sobre ocupação hospitalar (arquivo: `COVID-19 de 2021`). 

**Justificativa:**  
Essa estratégia evita duplicações e garante atualização eficiente dos dados, pois o `MERGE` compara o identificador `_ID` para atualizar ou inserir os registros.

---

#### Parte 2: Job do dbt após ingestão

No dbt Cloud, foi criado um job chamado `Atualização COVID (Mensal)`, configurado para rodar automaticamente todo dia 1º do mês às 03:00 UTC.

**Cron configurado no job:**
```
0 3 1 * *
```

**Pipeline:**
1. A task `task_merge_leitos_covid_2021` roda e atualiza os dados no schema `BRONZE`.
   ```
   CREATE OR REPLACE TASK covid_2021_task_merge_ingest
   WAREHOUSE = transforming
   SCHEDULE = 'USING CRON 0 3 1 * * UTC'  -- Rodar dia 1 de cada mês às 03h UTC
   AS
   MERGE INTO COVID_2021.BRONZE.STG_LEITO_OCUPACAO_2021 AS target
   USING (
   SELECT
    $1 AS UNNAMED_0,
    $2 AS _ID,
    $3 AS DATA_NOTIFICACAO,
    $4 AS CNES,
    $5 AS OCUPACAO_SUSPEITO_CLI,
    $6 AS OCUPACAO_SUSPEITO_UTI,
    $7 AS OCUPACAO_CONFIRMADO_CLI,
    $8 AS OCUPACAO_CONFIRMADO_UTI,
    $9 AS OCUPACAO_COVID_UTI,
    $10 AS OCUPACAO_COVID_CLI,
    $11 AS OCUPACAO_HOSPITALAR_UTI,
    $12 AS OCUPACAO_HOSPITALAR_CLI,
    $13 AS SAIDA_SUSPEITA_OBITOS,
    $14 AS SAIDA_SUSPEITA_ALTAS,
    $15 AS SAIDA_CONFIRMADA_OBITOS,
    $16 AS SAIDA_CONFIRMADA_ALTAS,
    $17 AS ORIGEM,
    $18 AS P_USUARIO,
    $19 AS ESTADO_NOTIFICACAO,
    $20 AS MUNICIPIO_NOTIFICACAO,
    $21 AS ESTADO,
    $22 AS MUNICIPIO,
    $23 AS EXCLUIDO,
    $24 AS VALIDADO,
    $25 AS CREATED_AT,
    $26 AS UPDATED_AT
   FROM @COVID_2021.BRONZE.LEITO_OCUPACAO_2021 (FILE_FORMAT => covid_csv_format)
   ) AS source
   ON target._ID = source._ID
   WHEN NOT MATCHED THEN
   INSERT (
    UNNAMED_0, _ID, DATA_NOTIFICACAO, CNES,
    OCUPACAO_SUSPEITO_CLI, OCUPACAO_SUSPEITO_UTI,
    OCUPACAO_CONFIRMADO_CLI, OCUPACAO_CONFIRMADO_UTI,
    OCUPACAO_COVID_UTI, OCUPACAO_COVID_CLI,
    OCUPACAO_HOSPITALAR_UTI, OCUPACAO_HOSPITALAR_CLI,
    SAIDA_SUSPEITA_OBITOS, SAIDA_SUSPEITA_ALTAS,
    SAIDA_CONFIRMADA_OBITOS, SAIDA_CONFIRMADA_ALTAS,
    ORIGEM, P_USUARIO, ESTADO_NOTIFICACAO, MUNICIPIO_NOTIFICACAO,
    ESTADO, MUNICIPIO, EXCLUIDO, VALIDADO, CREATED_AT, UPDATED_AT
   )
   VALUES (
    source.UNNAMED_0, source._ID, source.DATA_NOTIFICACAO, source.CNES,
    source.OCUPACAO_SUSPEITO_CLI, source.OCUPACAO_SUSPEITO_UTI,
    source.OCUPACAO_CONFIRMADO_CLI, source.OCUPACAO_CONFIRMADO_UTI,
    source.OCUPACAO_COVID_UTI, source.OCUPACAO_COVID_CLI,
    source.OCUPACAO_HOSPITALAR_UTI, source.OCUPACAO_HOSPITALAR_CLI,
    source.SAIDA_SUSPEITA_OBITOS, source.SAIDA_SUSPEITA_ALTAS,
    source.SAIDA_CONFIRMADA_OBITOS, source.SAIDA_CONFIRMADA_ALTAS,
    source.ORIGEM, source.P_USUARIO, source.ESTADO_NOTIFICACAO, source.MUNICIPIO_NOTIFICACAO,
    source.ESTADO, source.MUNICIPIO, source.EXCLUIDO, source.VALIDADO, source.CREATED_AT, source.UPDATED_AT
   );
   ```
2. Executar a task: RESUME

3. Após criação da TASK no SNOWFLAKE criar o JOB no dbt com schedule definido diariamente. Assim o job do dbt é acionado em seguida e atualiza os modelos nas camadas `GOLD`, `DIMENSIONS` e `FACTS`.

**Benefícios:**
- Garante que os modelos do dbt sejam atualizados somente após a ingestão de novos dados.
- Evita execução desnecessária do pipeline em dias sem mudanças.
- Sincroniza transformação e testes com a frequência de atualização da base nacional (OpenDataSUS).


## Resultados e Insights Obtidos

Este projeto permite a análise aprofundada dos dados de ocupação de leitos, possibilitando a extração de insights relevantes para a saúde pública. O modelo dimensional facilita a identificação de tendências, padrões e anomalias na utilização de recursos hospitalares.


###Insight Potencial:### Mostrar picos de ocupação de leitos de UTI por COVID-19 em determinados estados e meses, correlacionando-os com o número de óbitos e altas. Isso pode indicar a necessidade de alocação de recursos adicionais, a eficácia de campanhas de vacinação ou a severidade de novas ondas da doença.

### Outros Insights Possíveis

*   **Distribuição Geográfica da Ocupação:** Analisar a ocupação de leitos por município para identificar áreas com maior ou menor demanda.
*   **Tendências Temporais:** Observar a evolução da ocupação de leitos ao longo do tempo (diária, semanal, mensal) para prever necessidades futuras.
*   **Taxas de Ocupação por Tipo de Leito:** Comparar a ocupação de leitos clínicos vs. UTI para diferentes tipos de ocupação (suspeito, confirmado, COVID, hospitalar).
*   **Desfechos (Óbitos/Altas):** Analisar a proporção de óbitos e altas em relação à ocupação de leitos para diferentes categorias.




## Inovações Implementadas

Este projeto incorpora diversas inovações e boas práticas que o tornam uma solução robusta e moderna para análise de dados de saúde pública:

1.  **Modelagem Dimensional Orientada a Insights:** O esquema estrela foi cuidadosamente projetado para não apenas armazenar dados, mas para facilitar a extração de insights acionáveis. A criação de dimensões como `DIM_OCUPACAO_TIPO` (que categoriza tipos de ocupação e leito) e a unificação de dados de diferentes anos na camada de fatos, mesmo com fontes separadas, demonstram uma preocupação em tornar a análise mais intuitiva e poderosa.

2.  **Abordagem Data-as-Code com dbt:** A utilização do dbt eleva a engenharia de dados a um novo patamar, tratando as transformações SQL como código de software. Isso permite:
    *   **Controle de Versão:** Todas as transformações são versionadas no Git, garantindo rastreabilidade e colaboração.
    *   **Testes Automatizados:** A inclusão de testes (e.g., `test_no_future_dates.sql`) assegura a qualidade e a integridade dos dados, um aspecto crítico em dados de saúde.
    *   **Documentação Automática:** O dbt gera documentação interativa do modelo de dados, promovendo a transparência e reduzindo a dependência de documentação manual desatualizada.
    *   **Reutilização de Código:** A modularização dos modelos dbt (staging, dimensions, facts) promove a reutilização e a manutenibilidade do código SQL.

3.  **Pipeline Incremental para Fatos:** A materialização incremental da tabela `FACT_OCUPACAO_LEITOS` é uma inovação crucial para lidar com grandes volumes de dados que crescem continuamente. Ao processar apenas os novos registros (`updated_at`), a solução otimiza o uso de recursos computacionais no Snowflake, reduzindo custos e tempo de execução, o que é essencial para pipelines de dados em produção.

4.  **Tratamento Inteligente de Dados Brutos:** A estratégia de tratamento de nulos e padronização de colunas (como `CNES` e `estado/municipio`) diretamente nas transformações dbt demonstra uma abordagem proativa para lidar com a qualidade dos dados na fonte. Isso garante que os dados modelados sejam limpos e consistentes para análise, mesmo com as imperfeições dos dados brutos do DataSUS.

5.  **Flexibilidade para Análise Temporal e Geográfica:** A criação de dimensões robustas como `DIM_TEMPO` (com granularidade de dia, mês, ano, semana, trimestre) e `DIM_LOCALIDADE` (estado e município) permite análises multidimensionais flexíveis. Isso é fundamental para entender a dinâmica da ocupação de leitos em diferentes períodos e regiões, apoiando decisões estratégicas em saúde pública.

Essas inovações, combinadas com a escolha de tecnologias de ponta como Snowflake e dbt, resultam em uma solução de engenharia de dados que não é apenas funcional, mas também eficiente, confiável e preparada para o futuro.




## Link para o dbt Docs Gerado

O dbt oferece uma funcionalidade robusta para gerar e servir a documentação do seu projeto. Esta documentação interativa inclui o grafo de dependências dos seus modelos, definições de colunas, testes e muito mais.

Para gerar e visualizar a documentação do seu projeto dbt, siga os passos abaixo no terminal, a partir do diretório raiz do seu projeto dbt:

1.  **Link da documentação do dbt:**
https://pc263.us1.dbt.com/accounts/70471823483155/develop/70471824038343/docs/index.html#!/overview


