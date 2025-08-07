-- models/facts/fact_ocupacao_leitos.sql

-- Passo 1: Referenciar a camada intermediária, que já deve ter o ID da localidade.
WITH intermediate_data AS (
    SELECT 
        *
    FROM {{ ref('int_leitos_ocupacao_unificado') }}
    {% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
),

-- Passo 2: Fazer o UNPIVOT completo de TODAS as colunas de ocupação
unpivoted_data AS (

    -- Ocupação por COVID
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'COVID' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_covid_cli AS ocupacao FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'COVID' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_covid_uti AS ocupacao FROM intermediate_data

    UNION ALL

    -- Ocupação Hospitalar Total
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'Hospitalar' AS tipo_ocupacao, 'Clínico' AS tipo_leito, ocupacao_hospitalar_cli AS ocupacao FROM intermediate_data
    UNION ALL
    SELECT id_registro, data_notificacao, cnes, id_localidade, updated_at, 'Hospitalar' AS tipo_ocupacao, 'UTI' AS tipo_leito, ocupacao_hospitalar_uti AS ocupacao FROM intermediate_data

    -- Adicione aqui os outros tipos (Confirmado, Suspeito) se forem necessários
),

-- Passo 3: Trazer as métricas de saídas (óbito/alta) de volta, filtrando negativas
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

-- Passo 4: Montar a tabela de fatos final
SELECT
    -- Chave primária única para a tabela de fatos
    {{ dbt_utils.generate_surrogate_key(['u.id_registro', 'ot.id_ocupacao_tipo']) }} AS id_fato,

    -- Chaves de dimensão
    t.id_tempo,
    u.id_localidade,
    u.cnes AS id_cnes,
    ot.id_ocupacao_tipo,

    -- Métricas
    u.ocupacao AS quantidade_leitos_ocupados,
    s.saida_confirmada_obitos,
    s.saida_confirmada_altas,

    -- Metadados
    u.updated_at

FROM unpivoted_data u

JOIN {{ ref('dim_data') }} t ON DATE(u.data_notificacao) = t.data

JOIN {{ ref('dim_ocupacao_tipo') }} ot 
    ON u.tipo_ocupacao = ot.tipo_ocupacao AND u.tipo_leito = ot.tipo_leito

LEFT JOIN saidas_data s ON u.id_registro = s.id_registro

-- Filtra registros com ocupação inválida
WHERE u.ocupacao > 0