WITH datas_distintas AS (
    SELECT DISTINCT CAST(data_notificacao AS DATE) AS data
    FROM COVID_19.bronze.stg_leito_ocupacao_consolidado
)
SELECT
    TO_CHAR(data, 'YYYYMMDD')::INT AS id_tempo,
    data,
    EXTRACT(YEAR FROM data) AS ano,
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(DAY FROM data) AS dia,
    EXTRACT(DAYOFWEEK FROM data) AS dia_da_semana
FROM datas_distintas
ORDER BY data