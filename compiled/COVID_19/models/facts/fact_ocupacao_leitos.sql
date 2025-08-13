WITH intermediate_data AS (
    SELECT 
        *
    FROM COVID_19.silver.int_leitos_ocupacao_unificado
    
    WHERE updated_at > (SELECT MAX(updated_at) FROM COVID_19.gold.fact_ocupacao_leitos)
    
),

unpivoted_data AS (

    -- Ocupação por COVID
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'COVID' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_covid_cli AS ocupacao
    FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'COVID' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_covid_uti AS ocupacao
    FROM intermediate_data

    UNION ALL

    -- Ocupação Hospitalar Total
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'Hospitalar' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_hospitalar_cli AS ocupacao
    FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'Hospitalar' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_hospitalar_uti AS ocupacao
    FROM intermediate_data

    UNION ALL

    -- Ocupação Confirmada
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'Confirmado' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_confirmado_cli AS ocupacao
    FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'Confirmado' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_confirmado_uti AS ocupacao
    FROM intermediate_data

    UNION ALL

    -- Ocupação Suspeita
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'Suspeito' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_suspeito_cli AS ocupacao
    FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at,
           'Suspeito' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_suspeito_uti AS ocupacao
    FROM intermediate_data
),

saidas_data AS (
    SELECT
        id_registro,
        saida_confirmada_obitos,
        saida_confirmada_altas
    FROM intermediate_data
    WHERE
        COALESCE(saida_confirmada_obitos, 0) >= 0
        AND COALESCE(saida_confirmada_altas, 0) >= 0
)

SELECT
    md5(cast(coalesce(cast(u.id_registro as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ot.id_ocupacao_tipo as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS id_fato,
    t.id_tempo,
    u.id_localidade,
    u.cnes AS id_cnes,
    ot.id_ocupacao_tipo,
    u.ocupacao AS quantidade_leitos_ocupados,
    s.saida_confirmada_obitos,
    s.saida_confirmada_altas,
    u.updated_at
FROM unpivoted_data u
JOIN COVID_19.gold.dim_data t ON DATE(u.data_notificacao) = t.data
JOIN COVID_19.gold.dim_ocupacao_tipo ot ON u.tipo_ocupacao = ot.tipo_ocupacao AND u.tipo_leito = ot.tipo_leito
LEFT JOIN saidas_data s ON u.id_registro = s.id_registro
WHERE u.ocupacao > 0