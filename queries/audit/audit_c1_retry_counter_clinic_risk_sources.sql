/*
  Auditoria: flags canônicas e fontes para
    - retry/appeal (retentar com responsável)
    - counter proposal (contra-oferta)
    - risco paciente vs risco clínica (inclui rating dinâmico)

  Não materializa nada. Só inventory + amostras.
*/

/* =========================================================
   [A] INVENTÁRIO DE COLUNAS (CREDIT_SIMULATIONS)
   ========================================================= */
SELECT
  'CAPIM_PRODUCTION.CREDIT_SIMULATIONS' AS table_id,
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='CAPIM_PRODUCTION'
  AND c.table_name='CREDIT_SIMULATIONS'
  AND (
    c.column_name ILIKE '%APPEAL%'
    OR c.column_name ILIKE '%RETRY%'
    OR c.column_name ILIKE '%RESPONS%'
    OR c.column_name ILIKE '%COUNTER%'
    OR c.column_name ILIKE '%PROPOS%'
    OR c.column_name ILIKE '%RISK%'
    OR c.column_name ILIKE '%RATING%'
    OR c.column_name ILIKE '%SCORE%'
  )
ORDER BY c.column_name
;

/* =========================================================
   [B] INVENTÁRIO DE COLUNAS (PRE_ANALYSES)
   ========================================================= */
SELECT
  'CAPIM_ANALYTICS.PRE_ANALYSES' AS table_id,
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='CAPIM_ANALYTICS'
  AND c.table_name='PRE_ANALYSES'
  AND (
    c.column_name ILIKE '%APPEAL%'
    OR c.column_name ILIKE '%RETRY%'
    OR c.column_name ILIKE '%RESPONS%'
    OR c.column_name ILIKE '%COUNTER%'
    OR c.column_name ILIKE '%PROPOS%'
    OR c.column_name ILIKE '%RISK%'
    OR c.column_name ILIKE '%RATING%'
    OR c.column_name ILIKE '%SCORE%'
  )
ORDER BY c.column_name
;

/* =========================================================
   [C] TABELAS potenciais de clínica/rating (por nome)
   ========================================================= */
SELECT
  t.table_schema,
  t.table_name,
  t.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.TABLES t
WHERE t.table_schema IN ('CAPIM_PRODUCTION','CAPIM_ANALYTICS','SOURCE_STAGING','RESTRICTED')
  AND (
    t.table_name ILIKE '%CLINIC%'
    OR t.table_name ILIKE '%RETAIL%'
    OR t.table_name ILIKE '%RATING%'
    OR t.table_name ILIKE '%SCORE%'
  )
ORDER BY t.table_schema, t.table_name
LIMIT 200
;

/* =========================================================
   [D] SOURCE_CREDIT_ENGINE_INFORMATION: procurar colunas de risco/flags (por nome)
   ========================================================= */
SELECT
  'SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION' AS table_id,
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema='SOURCE_STAGING'
  AND c.table_name='SOURCE_CREDIT_ENGINE_INFORMATION'
  AND (
    c.column_name ILIKE '%APPEAL%'
    OR c.column_name ILIKE '%RETRY%'
    OR c.column_name ILIKE '%RESPONS%'
    OR c.column_name ILIKE '%COUNTER%'
    OR c.column_name ILIKE '%RISK%'
    OR c.column_name ILIKE '%RATING%'
    OR c.column_name ILIKE '%SCORE%'
  )
ORDER BY c.column_name
;

