/*
  CREDIT_SIMULATIONS: comparar
    - CREDIT_SIMULATIONS.SCORE (texto; normalizar "9,00" -> 9)
    - PRE_ANALYSES.RISK_CAPIM (type=credit_simulation)
  para verificar se são a "mesma" variável (risco paciente Capim) em duas fontes.
*/

WITH cs AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    TRY_TO_NUMBER(REPLACE(cs.SCORE, ',', '.'))::NUMBER AS cs_score_num,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.SCORE IS NOT NULL
),
pa AS (
  SELECT
    pa.PRE_ANALYSIS_ID::NUMBER AS credit_simulation_id,
    TRY_TO_NUMBER(pa.RISK_CAPIM)::NUMBER AS risk_capim,
    pa.RISK_CAPIM_SUBCLASS
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='credit_simulation'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pa.PRE_ANALYSIS_ID
    ORDER BY pa.PRE_ANALYSIS_UPDATED_AT DESC, pa.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
),
j AS (
  SELECT
    cs.*,
    pa.risk_capim,
    pa.risk_capim_subclass
  FROM cs
  LEFT JOIN pa ON pa.credit_simulation_id = cs.credit_simulation_id
)
SELECT
  COUNT(*)::NUMBER AS n_with_cs_score,
  COUNT_IF(risk_capim IS NOT NULL)::NUMBER AS n_with_risk_capim,
  COUNT_IF(risk_capim IS NOT NULL AND cs_score_num IS NOT NULL AND risk_capim = cs_score_num)::NUMBER AS n_exact_match,
  (COUNT_IF(risk_capim IS NOT NULL AND cs_score_num IS NOT NULL AND risk_capim = cs_score_num) / NULLIF(COUNT_IF(risk_capim IS NOT NULL AND cs_score_num IS NOT NULL),0))::FLOAT AS match_rate
FROM j
;

WITH cs AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    TRY_TO_NUMBER(REPLACE(cs.SCORE, ',', '.'))::NUMBER AS cs_score_num,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.SCORE IS NOT NULL
),
pa AS (
  SELECT
    pa.PRE_ANALYSIS_ID::NUMBER AS credit_simulation_id,
    TRY_TO_NUMBER(pa.RISK_CAPIM)::NUMBER AS risk_capim,
    pa.RISK_CAPIM_SUBCLASS
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='credit_simulation'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pa.PRE_ANALYSIS_ID
    ORDER BY pa.PRE_ANALYSIS_UPDATED_AT DESC, pa.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
),
j AS (
  SELECT
    cs.*,
    pa.risk_capim,
    pa.risk_capim_subclass
  FROM cs
  LEFT JOIN pa ON pa.credit_simulation_id = cs.credit_simulation_id
)
SELECT
  cs_score_num,
  risk_capim,
  COUNT(*)::NUMBER AS n
FROM j
WHERE cs_score_num IS NOT NULL
  AND risk_capim IS NOT NULL
GROUP BY 1,2
ORDER BY n DESC
LIMIT 60
;

/* divergências mais comuns */
WITH cs AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    TRY_TO_NUMBER(REPLACE(cs.SCORE, ',', '.'))::NUMBER AS cs_score_num,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.SCORE IS NOT NULL
),
pa AS (
  SELECT
    pa.PRE_ANALYSIS_ID::NUMBER AS credit_simulation_id,
    TRY_TO_NUMBER(pa.RISK_CAPIM)::NUMBER AS risk_capim,
    pa.RISK_CAPIM_SUBCLASS
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='credit_simulation'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pa.PRE_ANALYSIS_ID
    ORDER BY pa.PRE_ANALYSIS_UPDATED_AT DESC, pa.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
),
j AS (
  SELECT
    cs.*,
    pa.risk_capim,
    pa.risk_capim_subclass
  FROM cs
  LEFT JOIN pa ON pa.credit_simulation_id = cs.credit_simulation_id
)
SELECT
  cs_score_num,
  risk_capim,
  state,
  appealable,
  COUNT(*)::NUMBER AS n
FROM j
WHERE cs_score_num IS NOT NULL
  AND risk_capim IS NOT NULL
  AND cs_score_num <> risk_capim
GROUP BY 1,2,3,4
ORDER BY n DESC
LIMIT 80
;

