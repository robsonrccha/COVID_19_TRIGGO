# Projeto de Engenharia de Dados: Ocupação de Leitos Hospitalares - COVID-19

## Visão Geral

Este projeto simula uma solução real de engenharia de dados desenvolvida com o objetivo de analisar dados públicos de ocupação hospitalar durante a pandemia da COVID-19. Os dados foram obtidos do [OpenDataSUS](https://opendatasus.saude.gov.br/) e tratados utilizando **Snowflake** como data warehouse e **dbt (data build tool)** para transformação e modelagem dimensional.

A arquitetura do projeto adota um modelo de **Data Warehouse Moderno**, com camadas **BRONZE**, **SILVER** (intermediária) e **GOLD**, estruturando os dados em um **Esquema Estrela**.

---

# COMO EXECUTAR O PROJETO

## Dados Utilizados

Foram utilizados arquivos CSV obtidos pelo DataSUS com registros de ocupação hospitalar para os anos de **2020**, **2021** e **2022**, além de arquivos auxiliares:

- `Registro de Ocupação Hospitalar COVID-19 - 2020`
- `Registro de Ocupação Hospitalar COVID-19 - 2021`
- `Registro de Ocupação Hospitalar COVID-19 - 2022`
- `cnes_estabelecimentos.csv`
- `municipios.csv`

Esses dados foram carregados na camada **BRONZE** do banco `COVID_19`, criando as seguintes tabelas:

### Tabelas BRONZE
- `RAW_LEITO_OCUPACAO_2020`
  ```sql
  create or replace TABLE COVID_19.BRONZE.RAW_LEITO_OCUPACAO_2020 (
	UNNAMED_0 NUMBER(38,0),
	_ID VARCHAR(16777216),
	DATA_NOTIFICACAO TIMESTAMP_NTZ(9),
	CNES VARCHAR(16777216),
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
	ORIGEM VARCHAR(16777216),
	P_USUARIO VARCHAR(16777216),
	ESTADO_NOTIFICACAO VARCHAR(16777216),
	MUNICIPIO_NOTIFICACAO VARCHAR(16777216),
	ESTADO VARCHAR(16777216),
	MUNICIPIO VARCHAR(16777216),
	EXCLUIDO BOOLEAN,
	VALIDADO BOOLEAN,
	CREATED_AT TIMESTAMP_NTZ(9),
	UPDATED_AT TIMESTAMP_NTZ(9),
	ANO_DADOS NUMBER(38,0)
  )COMMENT='A tabela contém registros de dados de ocupação relacionados à COVID-19 do ano de 2020, especificamente sobre o status de ocupação de casos suspeitos e confirmados. Cada registro inclui detalhes sobre a situação de ocupação     de indivíduos em diversos ambientes, como clínicas e hospitais, bem como o município e o estado da notificação.'
  ;
  ```
   
- `RAW_LEITO_OCUPACAO_2021`
  - ** repetir codigo acima e alterar o ano: ```create or replace TABLE COVID_19.BRONZE.RAW_LEITO_OCUPACAO_2021 ( ..... ```
    
- `RAW_LEITO_OCUPACAO_2022`
  - ** repetir codigo acima e alterar o ano: ```create or replace TABLE COVID_19.BRONZE.RAW_LEITO_OCUPACAO_2022 ( ..... ```
    
- `RAW_ESTABELECIMENTOS_CNES`
  ```sql
  create or replace TABLE COVID_19.BRONZE.RAW_ESTABELECIMENTOS_CNES (
	CO_CNES VARCHAR(16777216),
	CO_UNIDADE VARCHAR(16777216),
	CO_UF VARCHAR(16777216),
	CO_IBGE VARCHAR(16777216),
	NU_CNPJ_MANTENEDORA VARCHAR(16777216),
	NO_RAZAO_SOCIAL VARCHAR(16777216),
	NO_FANTASIA VARCHAR(16777216),
	CO_NATUREZA_ORGANIZACAO VARCHAR(16777216),
	DS_NATUREZA_ORGANIZACAO VARCHAR(16777216),
	TP_GESTAO VARCHAR(16777216),
	CO_NIVEL_HIERARQUIA VARCHAR(16777216),
	DS_NIVEL_HIERARQUIA VARCHAR(16777216),
	CO_ESFERA_ADMINISTRATIVA VARCHAR(16777216),
	DS_ESFERA_ADMINISTRATIVA VARCHAR(16777216),
	CO_ATIVIDADE VARCHAR(16777216),
	TP_UNIDADE VARCHAR(16777216),
	CO_CEP VARCHAR(16777216),
	NO_LOGRADOURO VARCHAR(16777216),
	NU_ENDERECO VARCHAR(16777216),
	NO_BAIRRO VARCHAR(16777216),
	NU_TELEFONE VARCHAR(16777216),
	NU_LATITUDE VARCHAR(16777216),
	NU_LONGITUDE VARCHAR(16777216),
	CO_TURNO_ATENDIMENTO VARCHAR(16777216),
	DS_TURNO_ATENDIMENTO VARCHAR(16777216),
	NU_CNPJ VARCHAR(16777216),
	NO_EMAIL VARCHAR(16777216),
	CO_NATUREZA_JUR VARCHAR(16777216),
	ST_CENTRO_CIRURGICO VARCHAR(16777216),
	ST_CENTRO_OBSTETRICO VARCHAR(16777216),
	ST_CENTRO_NEONATAL VARCHAR(16777216),
	ST_ATEND_HOSPITALAR VARCHAR(16777216),
	ST_SERVICO_APOIO VARCHAR(16777216),
	ST_ATEND_AMBULATORIAL VARCHAR(16777216),
	CO_MOTIVO_DESAB VARCHAR(16777216),
	CO_AMBULATORIAL_SUS VARCHAR(16777216)
  )COMMENT='Tabela que mostra as unidades de saude do SUS'
  ;
  ```

  
- `RAW_MUNICIPIOS_IBGE`
  ```sql
  create or replace TABLE COVID_19.BRONZE.RAW_MUNICIPIOS_IBGE (
	COD_UF NUMBER(38,0),
	COD_IBGE VARCHAR(16777216),
	NOME_MUNICIPIO VARCHAR(16777216)
  )COMMENT='A tabela contém dados geográficos dos municípios. Cada registro representa um único município e inclui detalhes sobre seu código e nome.'
  ;
  ```

---

## Estrutura do Projeto dbt

### Diretórios e Modelos
```
models/
├── dimensions/
│   ├── dim_cnes.sql
│   ├── dim_data.sql
│   ├── dim_localidade.sql
│   ├── dim_ocupacao_tipo.sql
│   ├── dim_tempo.sql
│   └── dim_unidade_saude.sql
├── facts/
│   └── fact_ocupacao_leitos.sql
├── intermediate/
│   └── int_leitos_ocupacao_unificado.sql
├── staging/
│   ├── stg_leito_ocupacao_2020.sql
│   ├── stg_leito_ocupacao_2021.sql
│   ├── stg_leito_ocupacao_2022.sql
│   └── stg_leito_ocupacao_consolidados.sql
```

---

## Esquema Snowflake - Banco `COVID_19`

### Camada BRONZE
Armazena os dados brutos ingeridos dos CSVs, conforme estrutura original.

### Camada SILVER (Intermediária)
Contém views intermediárias como:
- `INT_LEITOS_OCUPACAO_UNIFICADO`

### Camada GOLD
Contém as tabelas modeladas:

#### Tabelas de Dimensão
- `DIM_CNES`
- `DIM_DATA`
- `DIM_LOCALIDADE`
- `DIM_OCUPACAO_TIPO`
- `DIM_TEMPO`
- `DIM_UNIDADE_SAUDE`

#### Tabela de Fato
- `FACT_OCUPACAO_LEITOS`

#### Views Auxiliares
- `INT_LEITOS_OCUPACAO_UNIFICADO`

---


## 📘 Descrição das Tabelas Principais

### Tabelas de Fato e Dimensão (Camada GOLD)

- **FACT_OCUPACAO_LEITOS**: Registra métricas de ocupação hospitalar por data, localidade, tipo de leito e unidade. Contém dados de leitos ocupados, óbitos e altas.
- **DIM_TEMPO**: Dimensão temporal com granularidade diária. Inclui nome do dia, trimestre, semana do ano e feriados.
- **DIM_DATA**: Dimensão temporal simplificada com ano, mês, dia e dia da semana.
- **DIM_LOCALIDADE**: Contém o estado e município dos registros, permitindo agregações geográficas.
- **DIM_CNES**: Traz os códigos CNES e os nomes das unidades de saúde.
- **DIM_UNIDADE_SAUDE**: Lista os identificadores das unidades de saúde (com possíveis duplicidades numéricas).
- **DIM_OCUPACAO_TIPO**: Classifica os tipos de ocupação (suspeito, confirmado, COVID, hospitalar) e o tipo de leito (UTI ou clínico).

### Tabelas de Ingestão (Camada BRONZE)

- **RAW_LEITO_OCUPACAO_20XX**: Dados brutos de ocupação hospitalar por ano, extraídos do OpenDataSUS.
- **RAW_ESTABELECIMENTOS_CNES**: Cadastro nacional dos estabelecimentos de saúde, com dados de endereço, atendimento e localização.
- **RAW_MUNICIPIOS_IBGE**: Referência com códigos e nomes de municípios segundo o IBGE.

### View Intermediária (Camada SILVER)

- **INT_LEITOS_OCUPACAO_UNIFICADO**: Consolida os dados de 2020 a 2022 com padronização de campos e chaves de junção.


## Execução dbt

### Pré-requisitos
- Snowflake account com warehouse e permissão de criação de objetos
- dbt Core (`pip install dbt-snowflake`)
- Git

### Comandos principais
- No terminal, acesse a raiz do projeto dbt e execute:
```bash
dbt debug         # Testa a conexão
dbt compile       # (Opcional) Compila os modelos
dbt run           # Executa os modelos e cria as tabelas/views
dbt build         # Executa run + test em sequência
dbt test          # Executa testes definidos
dbt docs generate # Gera documentação
```

---

## Estratégia de Materialização

- **Staging e Intermediárias**: `view`
- **Tabelas Dimensionais**: `table`
- **Fato**: `incremental` (com base na coluna `updated_at`)

---

## Testes Automatizados (schema.yml)
- `unique` e `not_null` para chaves primárias
- `relationships` entre fato e dimensões
- Teste de integridade de datas (`test_no_future_dates.sql`)

---


## 🧪 Testes de Qualidade de Dados com dbt

O projeto implementa testes automatizados nas tabelas modeladas, garantindo consistência, integridade relacional e confiabilidade nos dados.

### ✅ Testes Declarativos (via `schema.yml`)
- **`unique` e `not_null`**: aplicados a chaves primárias nas tabelas de dimensão e fato.
- **`relationships`**: validação de integridade entre as chaves estrangeiras da fato e suas respectivas dimensões (`id_tempo`, `id_localidade`, `id_ocupacao_tipo`, `id_cnes`).
- **Validação de colunas críticas**:
  - `quantidade_leitos_ocupados` → `not_null`
  - `updated_at` → `not_null`
  - `dim_tempo.data` → `unique` e `not_null`

### ✅ Testes Personalizados (`.sql` em `tests/`)
- **`test_no_future_dates.sql`**  
  Verifica se não há datas futuras na tabela `fact_ocupacao_leitos`.

- **`test_leitos_nao_negativos.sql`**  
  Garante que não existam valores negativos na métrica `quantidade_leitos_ocupados`.

- **`test_obitos_altas_negativas.sql`**  
  Valida que não há registros com óbitos ou altas negativas. Essa validação motivou um tratamento direto no modelo `fact_ocupacao_leitos` para excluir registros inválidos.

Todos os testes são executados com o comando:

```bash
dbt test
```



## ⚙️ Orquestração e Automação da Pipeline

A ingestão e transformação de dados foi orquestrada integralmente no **Snowflake**, aproveitando as funcionalidades nativas de **Tasks** para garantir atualização periódica e incremental dos dados, **sem necessidade de Pipes**.

---

### 🧭 Snowflake Tasks com `MERGE INTO`

Para os anos de **2020**, **2021** e **2022**, foram criadas Tasks no schema `BRONZE` que realizam ingestão incremental usando `MERGE INTO`. O identificador `_ID` é utilizado para garantir que registros já existentes **não sejam duplicados**, assegurando consistência nas tabelas `RAW`.

#### ✅ Exemplo de Task criada (para 2021):

```sql
CREATE OR REPLACE TASK COVID_19.BRONZE.COVID_2021_TASK_MERGE_INGEST
WAREHOUSE = TRANSFORMING
SCHEDULE = 'USING CRON 0 3 1 * * UTC'
AS
MERGE INTO COVID_19.BRONZE.RAW_LEITO_OCUPACAO_2021 AS target
USING (
  SELECT
    $1 AS UNNAMED_0,
    $2 AS _ID,
    ...
    $26 AS UPDATED_AT
  FROM @COVID_19.BRONZE.LEITO_OCUPACAO (FILE_FORMAT => covid_csv_format)
) AS source
ON target._ID = source._ID
WHEN NOT MATCHED THEN
INSERT (...);
```

---

### 🕒 Detalhes da Task

| Parâmetro        | Valor                                     |
|------------------|-------------------------------------------|
| **Schedule**     | 03:00 AM (UTC), dia 1 de cada mês         |
| **Warehouse**    | TRANSFORMING                              |
| **Auto-Suspend** | Após 10 falhas consecutivas               |
| **Timeout**      | 60 minutos                                |
| **Status**       | Started (ativa)                           |

---

### 🔄 Integração com dbt Cloud

Após a execução da Task, um **Job agendado no dbt Cloud** executa os modelos do projeto, reconstruindo e testando as camadas `SILVER` e `GOLD`.  
Esse Job está programado para rodar **no dia 2º de cada mês às 03:00 UTC**, garantindo total sincronização com a ingestão automatizada.

---


### 📅 Detalhes do Job no dbt Cloud

| Parâmetro              | Valor                                  |
|------------------------|----------------------------------------|
| **Nome do Job**        | Atualização COVID (mensal)             |
| **Execução Automática**| Desativada (pode ser ativada manualmente) |
| **Agendamento (UTC)**  | 03:00 AM no dia 2 de cada mês          |
| **Cron**               | `0 3 2 * *`                             |
| **Geração de Docs**    | Ativada (`generate docs on run`)       |
| **Disparo por outro Job** | Desativado                           |

> 💡 O Job pode ser ativado para rodar automaticamente a cada mês logo após a Task Snowflake (que roda no dia 1º). Isso garante ingestão ➜ transformação sem conflito.


### ✅ Benefícios dessa abordagem

- ✅ **Evita duplicações** com `MERGE` baseado em `_ID`
- ✅ **Automatiza a ingestão mensal** sem intervenção manual
- ✅ **Dispensa o uso de Pipes**
- ✅ **Integra-se diretamente com o dbt Cloud**
- ✅ **Reduz o custo e a complexidade operacional**


## 📊 Resultados e Insights Obtidos

A estrutura criada permite gerar diversos insights relevantes para a saúde pública, com base na modelagem dimensional e dados históricos padronizados.

---

## Insights:
- **Exemplo de Consulta:**
```sql
SELECT
  l.estado,
  t.ano,
  t.mes,
  SUM(f.quantidade_leitos_ocupados) AS total_uti_ocupados,
  SUM(f.saida_confirmada_obitos) AS total_obitos,
  SUM(f.saida_confirmada_altas) AS total_altas
FROM COVID_19.GOLD.FACT_OCUPACAO_LEITOS f
JOIN COVID_19.GOLD.DIM_TEMPO t ON f.id_tempo = t.id_tempo
JOIN COVID_19.GOLD.DIM_LOCALIDADE l ON f.id_localidade = l.id_localidade
JOIN COVID_19.GOLD.DIM_OCUPACAO_TIPO o ON f.id_ocupacao_tipo = o.id_ocupacao_tipo
WHERE o.tipo_ocupacao = 'COVID' AND o.tipo_leito = 'UTI'
GROUP BY l.estado, t.ano, t.mes
ORDER BY t.ano, t.mes;
```

### 💡 Insights extraído:
- Identificar **picos de ocupação de leitos de UTI por COVID-19** em determinados estados e meses.
- Relacionar o aumento da ocupação hospitalar com os números de **óbitos** e **altas hospitalares**.
- Apoiar decisões de **alocação de recursos hospitalares**, **abertura de novos leitos** e **avaliação da eficácia de políticas públicas e medidas sanitárias**.

### 💡 Outros Insights com visualização para o Power BI:
- A quantidade de obitos nos tres nos três anos por estado
- A quantidade de leitos ocupados ao longo dos meses por COVID-19. Onde o mês de junho de 2021 teve o maior registro de ocupação.

> segue as imagens do relátorio: [InsightsPowerBI.pdf](https://github.com/user-attachments/files/21690395/Relatorio_Unico.pdf)




## 🚀 Inovações Implementadas

- ✅ **Pipeline de ingestão incremental automatizada com Snowflake Tasks**  
  Cada ano de dados (2020, 2021, 2022) é carregado com `MERGE INTO`, garantindo atualização sem duplicidade.

- ✅ **Integração agendada com dbt Cloud para transformação e validação**  
  Um Job programado para o dia 2 de cada mês transforma os dados assim que a ingestão é concluída no dia 1.

- ✅ **Eliminação de registros inválidos diretamente no modelo de fato**  
  Registros com valores negativos em `óbitos` e `altas` são filtrados na etapa intermediária para garantir qualidade.

- ✅ **Documentação gerada automaticamente com `dbt docs generate`**  
  Toda a estrutura de dados e testes são documentados e atualizados com cada execução do pipeline.

- ✅ **Uso de `generate_surrogate_key()` para garantir unicidade na tabela fato**  
  Garante chaves consistentes mesmo com múltiplas combinações de dimensões envolvidas.

---

## Documentação dbt
[Visualizar Documentação](https://robsonrccha.github.io/COVID_19_TRIGGO/#!/overview)


---
