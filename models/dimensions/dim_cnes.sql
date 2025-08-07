-- CTE para ler os dados da tabela de consulta de estabelecimentos
WITH estabelecimentos_cnes AS (
    SELECT
        CAST(CO_CNES AS STRING) AS id_cnes,
        NO_FANTASIA AS nm_estabelecimento
    FROM
        {{ source('bronze_source', 'RAW_ESTABELECIMENTOS_CNES') }}
),

-- CTE para pegar todos os códigos CNES únicos dos seus dados de leitos
cnes_nos_dados AS (
    SELECT DISTINCT
        cnes AS id_cnes
    FROM
         {{ ref('stg_leito_ocupacao_consolidado') }}
    WHERE
        cnes IS NOT NULL
)

-- Junção para criar a dimensão final
SELECT
    c.id_cnes,
    COALESCE(cnes.nm_estabelecimento, 'Não Informado') AS nm_estabelecimento
FROM
    cnes_nos_dados c
LEFT JOIN
    estabelecimentos_cnes cnes ON c.id_cnes = cnes.id_cnes