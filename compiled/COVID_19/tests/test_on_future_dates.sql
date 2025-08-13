-- tests/test_no_future_dates.sql
SELECT *
FROM COVID_19.gold.fact_ocupacao_leitos f
JOIN COVID_19.gold.dim_tempo t ON f.id_tempo = t.id_tempo
WHERE t.data > CURRENT_DATE()