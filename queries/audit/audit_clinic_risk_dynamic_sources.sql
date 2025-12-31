/*
  Auditoria (sem materializar): encontrar fonte de risco/score/rating dinâmico da clínica.
  Hipótese forte: CAPIM_ANALYTICS.CLINIC_SCORE_LOGS (ou tabelas de RETAIL_UPDATE_LOGS / ENRICHED_RETAILS).
*/

/* [A] CLINIC_SCORE_LOGS: colunas */
SELECT
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='CAPIM_ANALYTICS'
  AND c.table_name='CLINIC_SCORE_LOGS'
ORDER BY c.ordinal_position
;

/* [B] CLINIC_SCORE_LOGS: amostra de linhas recentes (ver schema real) */
SELECT *
FROM CAPIM_DATA.CAPIM_ANALYTICS.CLINIC_SCORE_LOGS
ORDER BY 1 DESC
LIMIT 50
;

/* [C] ENRICHED_RETAILS: colunas com rating/score */
SELECT
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='CAPIM_ANALYTICS'
  AND c.table_name='ENRICHED_RETAILS'
  AND (c.column_name ILIKE '%RATING%' OR c.column_name ILIKE '%SCORE%')
ORDER BY c.column_name
;

