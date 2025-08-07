-- models/intermediate/int_leitos_ocupacao_unificado.sql

WITH staging_data AS (
    SELECT * 
    FROM {{ ref('stg_leito_ocupacao_consolidado') }}
    WHERE 
        COALESCE(saida_confirmada_obitos, 0) >= 0
        AND COALESCE(saida_confirmada_altas, 0) >= 0
),

dim_localidade AS (
    SELECT * FROM {{ ref('dim_localidade') }}
)

SELECT
    stg.*,  
    loc.id_localidade

FROM staging_data stg

LEFT JOIN dim_localidade loc ON
    UPPER(COALESCE(stg.municipio_notificacao, stg.municipio, 'Desconhecido')) = UPPER(loc.municipio)
    AND UPPER(COALESCE(stg.estado_notificacao, stg.estado, 'Desconhecido')) = UPPER(loc.estado)
