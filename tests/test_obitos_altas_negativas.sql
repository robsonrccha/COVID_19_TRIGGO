SELECT *
FROM {{ ref('fact_ocupacao_leitos') }}
WHERE
  saida_confirmada_obitos < 0 OR
  saida_confirmada_altas < 0
