/*
  Auditoria Snowflake-first:
    CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION

  Objetivo (bem específico):
    Entender se essa tabela permite associar uma C1 legado (pre_analysis_id)
    a:
      - crivo_check (crivo_check_id / crivo_id / engineable)
      - credit_checks (credit_check_id / cpf / timestamps)

  Como usar:
    - Rode no Snowflake Worksheet (múltiplos result sets).
    - Comece em [A0] para ver colunas e ajustar as hipóteses de join se necessário.

  Guardrails:
    - Não assumir que existe 1 chave única.
    - Priorizar métricas de cobertura + amostras pequenas.
*/

SET cei_table = 'CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION';
SET pa_table  = 'CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES';
SET spa_table = 'CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API';
SET cs_table  = 'CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS';
SET crivo_table = 'CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS';
SET cc_table    = 'CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API';

/* =========================================================
   [A0] INFORMATION_SCHEMA: colunas e tipos
   ========================================================= */
SELECT
  ordinal_position,
  column_name,
  data_type,
  is_nullable
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'SOURCE_STAGING'
  AND table_name   = 'SOURCE_CREDIT_ENGINE_INFORMATION'
ORDER BY ordinal_position
;

/* Candidatos de chaves por nome */
SELECT
  column_name,
  data_type
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema='SOURCE_STAGING'
  AND table_name='SOURCE_CREDIT_ENGINE_INFORMATION'
  AND (
    column_name ILIKE '%PRE%ANAL%'
    OR column_name ILIKE '%SIMULATION%'
    OR column_name ILIKE '%LEAD%'
    OR column_name ILIKE '%REQUEST%'
    OR column_name ILIKE '%CRIVO%'
    OR column_name ILIKE '%CREDIT_CHECK%'
    OR column_name ILIKE '%CHECK_ID%'
    OR column_name ILIKE '%CPF%'
    OR column_name ILIKE '%DOCUMENT%'
    OR column_name ILIKE '%USER%'
    OR column_name ILIKE '%RETAIL%'
    OR column_name ILIKE '%CLINIC%'
    OR column_name ILIKE '%CREATED%'
    OR column_name ILIKE '%UPDATED%'
    OR column_name ILIKE '%TIMESTAMP%'
  )
ORDER BY column_name
;

/* Colunas VARIANT/OBJECT/ARRAY (potenciais JSONs) */
SELECT
  column_name,
  data_type
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema='SOURCE_STAGING'
  AND table_name='SOURCE_CREDIT_ENGINE_INFORMATION'
  AND data_type IN ('VARIANT','OBJECT','ARRAY')
ORDER BY column_name
;

/* =========================================================
   [A1] Volume + range temporal (se houver created_at)
   ========================================================= */
SELECT
  COUNT(*)::NUMBER AS n_rows
FROM IDENTIFIER($cei_table)
;

/* =========================================================
   [A2] Amostra de 50 linhas (para ver nomes reais / semântica)
   ========================================================= */
SELECT *
FROM IDENTIFIER($cei_table)
QUALIFY ROW_NUMBER() OVER (ORDER BY UNIFORM(0,1000000,RANDOM())) <= 50
;

/* =========================================================
   [B] Vínculo via ENGINEABLE_TYPE/ENGINEABLE_ID (canônico)
   - A tabela NÃO tem pre_analysis_id explícito; o caminho é engineable.
   ========================================================= */

/* [B0] Distribuição de ENGINEABLE_TYPE */
SELECT
  ENGINEABLE_TYPE,
  COUNT(*)::NUMBER AS n_rows
FROM IDENTIFIER($cei_table)
GROUP BY 1
ORDER BY n_rows DESC
;

/* [B1] ENGINEABLE_TYPE=CreditSimulation → CREDIT_SIMULATIONS.id */
WITH cei AS (
  SELECT ENGINEABLE_ID
  FROM IDENTIFIER($cei_table)
  WHERE LOWER(ENGINEABLE_TYPE) = 'credit_simulation'
),
cs AS (
  SELECT id
  FROM IDENTIFIER($cs_table)
)
SELECT
  'cei_engineable_credit_simulation' AS test_name,
  COUNT(*)::NUMBER AS n_cei_rows,
  COUNT_IF(cs.id IS NOT NULL)::NUMBER AS n_match,
  (COUNT_IF(cs.id IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_match
FROM cei
LEFT JOIN cs
  ON cs.id = cei.ENGINEABLE_ID
;

/* [B2] ENGINEABLE_TYPE=PreAnalysis → SOURCE_PRE_ANALYSIS_API.pre_analysis_id */
WITH cei AS (
  SELECT ENGINEABLE_ID
  FROM IDENTIFIER($cei_table)
  WHERE LOWER(ENGINEABLE_TYPE) IN ('pre_analysis','preanalysis','pre-analysis')
),
spa AS (
  SELECT PRE_ANALYSIS_ID
  FROM IDENTIFIER($spa_table)
)
SELECT
  'cei_engineable_pre_analysis' AS test_name,
  COUNT(*)::NUMBER AS n_cei_rows,
  COUNT_IF(spa.PRE_ANALYSIS_ID IS NOT NULL)::NUMBER AS n_match,
  (COUNT_IF(spa.PRE_ANALYSIS_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_match
FROM cei
LEFT JOIN spa
  ON spa.PRE_ANALYSIS_ID = cei.ENGINEABLE_ID
;

/* [B3] ENGINEABLE_TYPE=PreAnalysis → PRE_ANALYSES (type=pre_analysis) */
WITH cei AS (
  SELECT ENGINEABLE_ID
  FROM IDENTIFIER($cei_table)
  WHERE LOWER(ENGINEABLE_TYPE) IN ('pre_analysis','preanalysis','pre-analysis')
),
pa AS (
  SELECT PRE_ANALYSIS_ID
  FROM IDENTIFIER($pa_table)
  WHERE PRE_ANALYSIS_TYPE = 'pre_analysis'
)
SELECT
  'cei_engineable_pre_analysis_to_pre_analyses' AS test_name,
  COUNT(*)::NUMBER AS n_cei_rows,
  COUNT_IF(pa.PRE_ANALYSIS_ID IS NOT NULL)::NUMBER AS n_match,
  (COUNT_IF(pa.PRE_ANALYSIS_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_match
FROM cei
LEFT JOIN pa
  ON pa.PRE_ANALYSIS_ID = cei.ENGINEABLE_ID
;

/* =========================================================
   [C] Vínculo com CRIVO (via CRIVO_ID)
   - CEI tem CRIVO_ID (TEXT). SOURCE_CRIVO_CHECKS também tem CRIVO_ID (TEXT).
   ========================================================= */
WITH cei AS (
  SELECT CRIVO_ID
  FROM IDENTIFIER($cei_table)
  WHERE CRIVO_ID IS NOT NULL
),
cr AS (
  SELECT CRIVO_ID::STRING AS crivo_id_str
  FROM IDENTIFIER($crivo_table)
)
SELECT
  'cei_crivo_id_to_source_crivo_checks' AS test_name,
  COUNT(*)::NUMBER AS n_cei_rows,
  COUNT_IF(cr.crivo_id_str IS NOT NULL)::NUMBER AS n_match,
  (COUNT_IF(cr.crivo_id_str IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_match
FROM cei
LEFT JOIN cr
  ON cr.crivo_id_str = cei.CRIVO_ID::string
;

/* =========================================================
   [D] Sinais de credit checks dentro de CEI.DATA (TEXT)
   - A tabela não tem credit_check_id explícito; tentar parsear DATA como JSON e inspecionar keys.
   ========================================================= */
WITH base AS (
  SELECT
    SOURCE,
    CREDIT_ENGINE_CONSULTATION_CREATED_AT AS created_at,
    TRY_PARSE_JSON(DATA) AS j
  FROM IDENTIFIER($cei_table)
),
typeof_dist AS (
  SELECT
    SOURCE,
    TYPEOF(j) AS j_typeof,
    COUNT(*)::NUMBER AS n_rows,
    (COUNT_IF(j IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_json_parse_ok
  FROM base
  GROUP BY 1,2
)
SELECT *
FROM typeof_dist
ORDER BY n_rows DESC
;

/* Top keys (amostra por source) */
WITH base AS (
  SELECT
    SOURCE,
    TRY_PARSE_JSON(DATA) AS j
  FROM IDENTIFIER($cei_table)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE ORDER BY UNIFORM(0,1000000,RANDOM())) <= 5000
),
keys_flat AS (
  SELECT
    b.SOURCE,
    k.value::string AS key_name
  FROM base b,
  LATERAL FLATTEN(input => IFF(TYPEOF(b.j)='OBJECT', OBJECT_KEYS(b.j), NULL)) k
  WHERE TYPEOF(b.j)='OBJECT'
),
top_keys AS (
  SELECT
    SOURCE,
    key_name,
    COUNT(*)::NUMBER AS n
  FROM keys_flat
  GROUP BY 1,2
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SOURCE ORDER BY COUNT(*) DESC, key_name ASC) <= 30
)
SELECT *
FROM top_keys
ORDER BY SOURCE, n DESC
;

/* [D3] Sanidade: CEI tem algum ID de check escondido em texto? (deveria ser 0; é só guardrail) */
SELECT
  SOURCE,
  COUNT(*)::NUMBER AS n_rows,
  COUNT_IF(REGEXP_LIKE(LOWER(DATA), 'credit\\s*check'))::NUMBER AS n_like_credit_check,
  COUNT_IF(REGEXP_LIKE(LOWER(DATA), 'creditcheck'))::NUMBER AS n_like_creditcheck,
  COUNT_IF(REGEXP_LIKE(LOWER(DATA), 'incremental_?credit_?checks'))::NUMBER AS n_like_incremental_cc
FROM IDENTIFIER($cei_table)
GROUP BY 1
ORDER BY n_rows DESC
;


