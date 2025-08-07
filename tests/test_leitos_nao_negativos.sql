SELECT *
FROM {{ ref('fact_ocupacao_leitos') }}
WHERE quantidade_leitos_ocupados < 0
