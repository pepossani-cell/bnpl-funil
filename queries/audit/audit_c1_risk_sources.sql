/*
  Inventário de fontes para "risk" (C1):
    - Risco paciente vs risco clínica (se existirem separados)
    - Formato/escala (0..5, -1, 9 etc.)
*/

/* [A] CREDIT_SIMULATIONS (fonte) — colunas com 'RISK' no nome */
SELECT
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='CAPIM_PRODUCTION'
  AND c.table_name='CREDIT_SIMULATIONS'
  AND c.column_name ILIKE '%RISK%'
ORDER BY c.column_name
;

/* [B] PRE_ANALYSES (fonte) — colunas com 'RISK' no nome */
SELECT
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='CAPIM_ANALYTICS'
  AND c.table_name='PRE_ANALYSES'
  AND c.column_name ILIKE '%RISK%'
ORDER BY c.column_name
;

/* [C] CREDIT_ENGINE_INFORMATION (fonte) — colunas com 'RISK' no nome */
SELECT
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='SOURCE_STAGING'
  AND c.table_name='SOURCE_CREDIT_ENGINE_INFORMATION'
  AND c.column_name ILIKE '%RISK%'
ORDER BY c.column_name
;

