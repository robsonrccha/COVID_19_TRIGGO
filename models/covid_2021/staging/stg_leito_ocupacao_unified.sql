SELECT * FROM {{ ref('stg_leito_ocupacao_2020') }}
UNION ALL
SELECT * FROM {{ ref('stg_leito_ocupacao_2021') }}
UNION ALL
SELECT * FROM {{ ref('stg_leito_ocupacao_2022') }}