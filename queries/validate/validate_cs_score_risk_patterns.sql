/*
  CREDIT_SIMULATIONS: investigar SCORE (0..5,-1,9) e como se relaciona com rejection_reason/state/appealable.
  Importante: SCORE vem como TEXT e às vezes usa vírgula ("9,00"). Normalizamos.
*/

WITH base AS (
  SELECT
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount,
    cs.SCORE AS score_raw,
    TRY_TO_NUMBER(REPLACE(cs.SCORE, ',', '.'))::NUMBER AS score_num
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.SCORE IS NOT NULL
)
SELECT
  score_raw,
  score_num,
  COUNT(*)::NUMBER AS n
FROM base
GROUP BY 1,2
ORDER BY n DESC
LIMIT 80
;

WITH base AS (
  SELECT
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE,
    TRY_TO_NUMBER(REPLACE(cs.SCORE, ',', '.'))::NUMBER AS score_num
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.SCORE IS NOT NULL
)
SELECT
  score_num,
  state,
  appealable,
  COUNT(*)::NUMBER AS n
FROM base
WHERE score_num IN (-1,9)
GROUP BY 1,2,3
ORDER BY score_num, n DESC
LIMIT 120
;

WITH base AS (
  SELECT
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE,
    TRY_TO_NUMBER(REPLACE(cs.SCORE, ',', '.'))::NUMBER AS score_num
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.SCORE IS NOT NULL
)
SELECT
  score_num,
  state,
  rejection_reason,
  COUNT(*)::NUMBER AS n
FROM base
WHERE score_num IN (-1,9)
GROUP BY 1,2,3
ORDER BY score_num, n DESC
LIMIT 150
;

