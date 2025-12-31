/*
  Auditoria: CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK
  Objetivo: verificar se essa tabela é uma bridge confiável entre pre_analysis_id e credit_check_id.
*/

SET pacc_table = 'CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK';
SET pa_table   = 'CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES';
SET spa_table  = 'CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API';
SET cc_table   = 'CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API';

/* [A0] Schema */
SELECT
  ordinal_position,
  column_name,
  data_type,
  is_nullable
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema='CAPIM_ANALYTICS'
  AND table_name='PRE_ANALYSIS_CREDIT_CHECK'
ORDER BY ordinal_position
;

/* [A1] Amostra enxuta (evita output gigante) */
SELECT
  PRE_ANALYSIS_ID,
  PRE_ANALYSIS_TYPE,
  PRE_ANALYSIS_CREATED_AT,
  HASH_CPF,
  CPF_SEVENTH_AND_EIGHTH_NUMBER
FROM IDENTIFIER($pacc_table)
QUALIFY ROW_NUMBER() OVER (ORDER BY UNIFORM(0,1000000,RANDOM())) <= 50
;

/* [A2] Candidatos de colunas por nome */
SELECT
  column_name,
  data_type
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema='CAPIM_ANALYTICS'
  AND table_name='PRE_ANALYSIS_CREDIT_CHECK'
  AND (
    column_name ILIKE '%PRE%ANAL%'
    OR column_name ILIKE '%CREDIT_CHECK%'
    OR column_name ILIKE '%CHECK_ID%'
    OR column_name ILIKE '%CPF%'
    OR column_name ILIKE '%SOURCE%'
    OR column_name ILIKE '%KIND%'
    OR column_name ILIKE '%CREATED%'
    OR column_name ILIKE '%UPDATED%'
  )
ORDER BY column_name
;

/* [B0] Volume total */
SELECT COUNT(*)::NUMBER AS n_rows
FROM IDENTIFIER($pacc_table)
;

/* [B1] Join-rate: PRE_ANALYSIS_CREDIT_CHECK.pre_analysis_id -> SOURCE_PRE_ANALYSIS_API.pre_analysis_id (legado) */
WITH spa AS (
  SELECT PRE_ANALYSIS_ID
  FROM IDENTIFIER($spa_table)
),
pacc AS (
  SELECT *
  FROM IDENTIFIER($pacc_table)
)
SELECT
  'pacc_pre_analysis_id_to_source_pre_analysis_api' AS test_name,
  COUNT(*)::NUMBER AS n_pacc_rows,
  COUNT_IF(spa.PRE_ANALYSIS_ID IS NOT NULL)::NUMBER AS n_match,
  (COUNT_IF(spa.PRE_ANALYSIS_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_match
FROM pacc
LEFT JOIN spa
  ON spa.PRE_ANALYSIS_ID = TRY_TO_NUMBER(pacc.PRE_ANALYSIS_ID::string)
;

/* [B2] Join-rate: pre_analysis_id -> PRE_ANALYSES (type=pre_analysis) */
WITH pa AS (
  SELECT PRE_ANALYSIS_ID
  FROM IDENTIFIER($pa_table)
  WHERE PRE_ANALYSIS_TYPE='pre_analysis'
),
pacc AS (
  SELECT *
  FROM IDENTIFIER($pacc_table)
)
SELECT
  'pacc_pre_analysis_id_to_pre_analyses' AS test_name,
  COUNT(*)::NUMBER AS n_pacc_rows,
  COUNT_IF(pa.PRE_ANALYSIS_ID IS NOT NULL)::NUMBER AS n_match,
  (COUNT_IF(pa.PRE_ANALYSIS_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_match
FROM pacc
LEFT JOIN pa
  ON pa.PRE_ANALYSIS_ID = TRY_TO_NUMBER(pacc.PRE_ANALYSIS_ID::string)
;

/* [B3] Join-rate: credit_check_id -> INCREMENTAL_CREDIT_CHECKS_API.id (se existir coluna CREDIT_CHECK_ID) */
SELECT
  'pacc_has_any_credit_check_id_column' AS test_name,
  COUNT(*)::NUMBER AS n_cols_like_credit_check_id
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema='CAPIM_ANALYTICS'
  AND table_name='PRE_ANALYSIS_CREDIT_CHECK'
  AND (
    column_name ILIKE '%CREDIT_CHECK_ID%'
    OR column_name ILIKE '%CHECK_ID%'
    OR column_name ILIKE '%CREDIT_CHECK%'
  )
;

/* [B4] Se a tabela é “curada”, quais sinais por eixo já aparecem? (heurística por nome) */
SELECT
  column_name,
  data_type
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema='CAPIM_ANALYTICS'
  AND table_name='PRE_ANALYSIS_CREDIT_CHECK'
  AND (
    column_name ILIKE '%SCORE%'
    OR column_name ILIKE '%SERASA%'
    OR column_name ILIKE '%BVS%'
    OR column_name ILIKE '%PEFIN%'
    OR column_name ILIKE '%REFIN%'
    OR column_name ILIKE '%PROTEST%'
    OR column_name ILIKE '%CCF%'
    OR column_name ILIKE '%RENDA%'
    OR column_name ILIKE '%INCOME%'
    OR column_name ILIKE '%LIMIT%'
    OR column_name ILIKE '%SCR%'
  )
ORDER BY column_name
;


