/*
  Diagnóstico: por que existem CREDIT_SIMULATIONS com state='rejected' mas permitted_amount>0?
  Hipóteses típicas:
    - "permitted" é um cálculo intermediário (limite) que pode existir mesmo em rejeição final
    - rejeição posterior por regra não-financeira (ex.: clinic_rating / política) sem zerar permitted
    - estado final "rejected" mas algum pipeline persistiu permitted antes de rejeitar
*/

WITH base AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    cs.CREATED_AT,
    cs.UPDATED_AT,
    cs.RETAIL_ID AS clinic_id,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount,
    cs.FINANCING_CONDITIONS
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.STATE = 'rejected'
    AND cs.PERMITTED_AMOUNT IS NOT NULL
    AND cs.PERMITTED_AMOUNT > 0
),
profile AS (
  SELECT
    COUNT(*)::NUMBER AS n,
    COUNT_IF(approved_at IS NOT NULL)::NUMBER AS n_has_approved_at,
    COUNT_IF(financing_conditions IS NOT NULL)::NUMBER AS n_financing_nonnull,
    COUNT_IF(TYPEOF(financing_conditions)='OBJECT')::NUMBER AS n_financing_object,
    COUNT_IF(TYPEOF(financing_conditions)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(financing_conditions))=0)::NUMBER AS n_financing_empty_object,
    COUNT_IF(TYPEOF(financing_conditions)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(financing_conditions))>0)::NUMBER AS n_financing_nonempty_object,
    MIN(permitted_amount) AS permitted_min,
    APPROX_PERCENTILE(permitted_amount, 0.50) AS permitted_p50,
    APPROX_PERCENTILE(permitted_amount, 0.95) AS permitted_p95,
    MAX(permitted_amount) AS permitted_max
  FROM base
),
by_reason AS (
  SELECT
    rejection_reason,
    COUNT(*)::NUMBER AS n,
    COUNT_IF(approved_at IS NOT NULL)::NUMBER AS n_has_approved_at,
    APPROX_PERCENTILE(permitted_amount, 0.50) AS p50,
    APPROX_PERCENTILE(permitted_amount, 0.95) AS p95,
    MAX(permitted_amount) AS max
  FROM base
  GROUP BY 1
  ORDER BY n DESC
  LIMIT 50
),
by_clinic AS (
  SELECT
    clinic_id,
    COUNT(*)::NUMBER AS n,
    APPROX_PERCENTILE(permitted_amount, 0.50) AS p50,
    MAX(permitted_amount) AS max
  FROM base
  GROUP BY 1
  ORDER BY n DESC
  LIMIT 50
)

SELECT 'profile' AS section, * FROM profile
;

WITH base AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    cs.CREATED_AT,
    cs.UPDATED_AT,
    cs.RETAIL_ID AS clinic_id,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount,
    cs.FINANCING_CONDITIONS
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.STATE = 'rejected'
    AND cs.PERMITTED_AMOUNT IS NOT NULL
    AND cs.PERMITTED_AMOUNT > 0
)
SELECT
  rejection_reason,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(approved_at IS NOT NULL)::NUMBER AS n_has_approved_at,
  APPROX_PERCENTILE(permitted_amount, 0.50) AS p50,
  APPROX_PERCENTILE(permitted_amount, 0.95) AS p95,
  MAX(permitted_amount) AS max
FROM base
GROUP BY 1
ORDER BY n DESC
LIMIT 50
;

WITH base AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    cs.CREATED_AT,
    cs.UPDATED_AT,
    cs.RETAIL_ID AS clinic_id,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount,
    cs.FINANCING_CONDITIONS
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.STATE = 'rejected'
    AND cs.PERMITTED_AMOUNT IS NOT NULL
    AND cs.PERMITTED_AMOUNT > 0
)
SELECT
  clinic_id,
  COUNT(*)::NUMBER AS n,
  APPROX_PERCENTILE(permitted_amount, 0.50) AS p50,
  MAX(permitted_amount) AS max
FROM base
GROUP BY 1
ORDER BY n DESC
LIMIT 50
;

/* amostra */
WITH base AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    cs.CREATED_AT,
    cs.UPDATED_AT,
    cs.RETAIL_ID AS clinic_id,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount,
    cs.FINANCING_CONDITIONS
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.STATE = 'rejected'
    AND cs.PERMITTED_AMOUNT IS NOT NULL
    AND cs.PERMITTED_AMOUNT > 0
)
SELECT
  credit_simulation_id,
  clinic_id,
  created_at,
  updated_at,
  approved_at,
  rejection_reason,
  permitted_amount,
  TYPEOF(financing_conditions) AS financing_type
FROM base
ORDER BY created_at DESC
LIMIT 100
;

