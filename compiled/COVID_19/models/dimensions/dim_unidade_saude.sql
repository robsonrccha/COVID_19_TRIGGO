WITH unidades_distintas AS (
 SELECT DISTINCT
 TRIM(cnes) AS cnes
 FROM COVID_19.bronze.stg_leito_ocupacao_consolidado
 WHERE cnes IS NOT NULL AND cnes != ''
)
SELECT
 cnes AS id_unidade_saude,
 cnes AS nome_unidade
FROM unidades_distintas
ORDER BY cnes