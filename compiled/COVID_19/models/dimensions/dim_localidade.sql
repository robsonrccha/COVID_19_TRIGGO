WITH localidades_distintas AS (
 SELECT DISTINCT
 COALESCE(estado_notificacao, estado, 'Desconhecido') AS estado,
 COALESCE(municipio_notificacao, municipio, 'Desconhecido') AS
municipio
 FROM COVID_19.bronze.stg_leito_ocupacao_consolidado
 WHERE estado IS NOT NULL OR municipio IS NOT NULL
)
SELECT
 ROW_NUMBER() OVER (ORDER BY estado, municipio) AS id_localidade,
 estado,
 municipio
FROM localidades_distintas
ORDER BY estado, municipio