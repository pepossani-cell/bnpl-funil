/*
  Diagnóstico temporal (legado): quem são os 9.788 'expired' com sinal de financing?
  E também: 'eligible' com rejection_reason (anomalias) — em que período ocorrem?
*/

/* [A] expired: financing signal por mês */
WITH base AS (
  SELECT
    DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) AS month,
    pa.PRE_ANALYSIS_STATE,
    pa.REJECTION_REASON,
    IFF(
      (pa.MINIMUM_TERM_AVAILABLE IS NOT NULL AND pa.MAXIMUM_TERM_AVAILABLE IS NOT NULL)
      OR (pa.INTEREST_RATES_ARRAY IS NOT NULL AND TYPEOF(pa.INTEREST_RATES_ARRAY)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(pa.INTEREST_RATES_ARRAY))>0),
      TRUE, FALSE
    ) AS has_financing_signal
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
    AND pa.PRE_ANALYSIS_STATE='expired'
)
SELECT
  month,
  COUNT(*)::NUMBER AS n_expired,
  COUNT_IF(has_financing_signal)::NUMBER AS n_with_financing_signal,
  (COUNT_IF(has_financing_signal) / NULLIF(COUNT(*),0))::FLOAT AS pct_with_financing_signal,
  COUNT_IF(rejection_reason IS NOT NULL)::NUMBER AS n_with_rejection_reason
FROM base
GROUP BY 1
ORDER BY 1
;

/* [B] expired with financing: top rejection reasons por mês (últimos 12 meses) */
WITH base AS (
  SELECT
    DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) AS month,
    pa.REJECTION_REASON,
    IFF(
      (pa.MINIMUM_TERM_AVAILABLE IS NOT NULL AND pa.MAXIMUM_TERM_AVAILABLE IS NOT NULL)
      OR (pa.INTEREST_RATES_ARRAY IS NOT NULL AND TYPEOF(pa.INTEREST_RATES_ARRAY)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(pa.INTEREST_RATES_ARRAY))>0),
      TRUE, FALSE
    ) AS has_financing_signal
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
    AND pa.PRE_ANALYSIS_STATE='expired'
    AND pa.PRE_ANALYSIS_CREATED_AT >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE()))
)
SELECT
  month,
  rejection_reason,
  COUNT(*)::NUMBER AS n
FROM base
WHERE has_financing_signal = TRUE
GROUP BY 1,2
ORDER BY month DESC, n DESC
LIMIT 200
;

/* [C] eligible com rejection_reason por mês */
WITH base AS (
  SELECT
    DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) AS month,
    pa.REJECTION_REASON
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
    AND pa.PRE_ANALYSIS_STATE='eligible'
    AND pa.REJECTION_REASON IS NOT NULL
)
SELECT
  month,
  COUNT(*)::NUMBER AS n
FROM base
GROUP BY 1
ORDER BY 1
;

/* [D] eligible com rejection_reason: top reasons (global) */
SELECT
  pa.REJECTION_REASON,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
  AND pa.PRE_ANALYSIS_STATE='eligible'
  AND pa.REJECTION_REASON IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 50
;

