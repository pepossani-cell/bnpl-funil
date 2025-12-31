/*
  Como trazer risco/score dinâmico de clínica no tempo do C1 (sem materializar).
  Fonte: CAPIM_ANALYTICS.CLINIC_SCORE_LOGS (log temporal) com CLINIC_CREDIT_SCORE.

  Objetivo:
    - Demonstrar join temporal "último score <= c1_created_at" em amostra de C1.
*/

/* [A] Cobertura do log: range temporal e volume */
SELECT
  COUNT(*)::NUMBER AS n_rows,
  MIN(CLINIC_SCORE_CHANGED_AT) AS min_ts,
  MAX(CLINIC_SCORE_CHANGED_AT) AS max_ts,
  COUNT(DISTINCT CLINIC_ID)::NUMBER AS n_clinics
FROM CAPIM_DATA.CAPIM_ANALYTICS.CLINIC_SCORE_LOGS
;

/* [B] Amostra de C1 (últimos 90 dias) e join temporal */
WITH c1 AS (
  SELECT
    cs.ID::NUMBER AS c1_entity_id,
    'credit_simulation' AS c1_entity_type,
    cs.RETAIL_ID::NUMBER AS clinic_id,
    cs.CREATED_AT AS c1_created_at
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.CREATED_AT >= DATEADD('day', -90, CURRENT_TIMESTAMP())
  QUALIFY ROW_NUMBER() OVER (ORDER BY cs.CREATED_AT DESC) <= 2000
),
log_ranked AS (
  SELECT
    c1.c1_entity_id,
    c1.clinic_id,
    c1.c1_created_at,
    l.CLINIC_SCORE_CHANGED_AT,
    l.CLINIC_CREDIT_SCORE,
    ROW_NUMBER() OVER (
      PARTITION BY c1.c1_entity_id
      ORDER BY l.CLINIC_SCORE_CHANGED_AT DESC
    ) AS rn
  FROM c1
  LEFT JOIN CAPIM_DATA.CAPIM_ANALYTICS.CLINIC_SCORE_LOGS l
    ON l.CLINIC_ID = c1.clinic_id
   AND l.CLINIC_SCORE_CHANGED_AT <= c1.c1_created_at
)
SELECT
  COUNT(*)::NUMBER AS n_c1,
  COUNT_IF(CLINIC_CREDIT_SCORE IS NOT NULL)::NUMBER AS n_with_clinic_score,
  (COUNT_IF(CLINIC_CREDIT_SCORE IS NOT NULL) / NULLIF(COUNT(*),0))::FLOAT AS pct_with_score,
  APPROX_PERCENTILE(CLINIC_CREDIT_SCORE, 0.50) AS score_p50,
  APPROX_PERCENTILE(CLINIC_CREDIT_SCORE, 0.95) AS score_p95
FROM log_ranked
WHERE rn = 1
;

/* [C] Exemplos recentes */
WITH c1 AS (
  SELECT
    cs.ID::NUMBER AS c1_entity_id,
    'credit_simulation' AS c1_entity_type,
    cs.RETAIL_ID::NUMBER AS clinic_id,
    cs.CREATED_AT AS c1_created_at
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.CREATED_AT >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  QUALIFY ROW_NUMBER() OVER (ORDER BY cs.CREATED_AT DESC) <= 50
),
log_ranked AS (
  SELECT
    c1.*,
    l.CLINIC_SCORE_CHANGED_AT,
    l.CLINIC_CREDIT_SCORE,
    ROW_NUMBER() OVER (
      PARTITION BY c1.c1_entity_id
      ORDER BY l.CLINIC_SCORE_CHANGED_AT DESC
    ) AS rn
  FROM c1
  LEFT JOIN CAPIM_DATA.CAPIM_ANALYTICS.CLINIC_SCORE_LOGS l
    ON l.CLINIC_ID = c1.clinic_id
   AND l.CLINIC_SCORE_CHANGED_AT <= c1.c1_created_at
)
SELECT
  c1_entity_id,
  clinic_id,
  c1_created_at,
  clinic_score_changed_at AS matched_score_ts,
  clinic_credit_score AS clinic_credit_score_at_c1
FROM log_ranked
WHERE rn = 1
ORDER BY c1_created_at DESC
;

