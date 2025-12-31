/*
  Checar se existe sinal de "appealable/retry" dentro de SOURCE_CREDIT_ENGINE_INFORMATION.DATA (JSON),
  tanto para credit_simulation quanto para pre_analysis.
*/

WITH base AS (
  SELECT
    ce.ENGINEABLE_TYPE,
    ce.SOURCE,
    ce.DATA,
    TO_VARCHAR(ce.DATA) AS data_txt
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION ce
  WHERE ce.DATA IS NOT NULL
    AND ce.ENGINEABLE_TYPE IN ('credit_simulation','pre_analysis')
)
SELECT
  engineable_type,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(LOWER(data_txt) LIKE '%appeal%')::NUMBER AS n_has_appeal_substring,
  COUNT_IF(LOWER(data_txt) LIKE '%retry%')::NUMBER AS n_has_retry_substring
FROM base
GROUP BY 1
ORDER BY 1
;

/* exemplos (se existirem) */
WITH base AS (
  SELECT
    ce.ENGINEABLE_TYPE,
    ce.SOURCE,
    ce.ENGINEABLE_ID,
    ce.CREDIT_ENGINE_CONSULTATION_CREATED_AT,
    TO_VARCHAR(ce.DATA) AS data_txt
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION ce
  WHERE ce.DATA IS NOT NULL
    AND ce.ENGINEABLE_TYPE IN ('credit_simulation','pre_analysis')
    AND (LOWER(TO_VARCHAR(ce.DATA)) LIKE '%appeal%' OR LOWER(TO_VARCHAR(ce.DATA)) LIKE '%retry%')
)
SELECT *
FROM base
ORDER BY CREDIT_ENGINE_CONSULTATION_CREATED_AT DESC
LIMIT 20
;

