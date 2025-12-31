/*
  Inferência auditável de "appealable" no legado (pre_analysis):
    - Fonte canônica: CREDIT_SIMULATIONS.APPEALABLE (somente no CS)
    - Estratégia: aprender distribuição P(appealable=TRUE | rejected, rejection_reason) no CS
                 e aplicar no legado por rejection_reason (e opcionalmente risk_capim).

  Resultado esperado:
    - Lista de razões com alta taxa de appealable no CS
    - Cobertura dessas razões no legado
    - Proposta de heurística: c1_appealable_legacy = (rejection_reason in whitelist) ou prob >= limiar
*/

/* =========================================================
   [A] CS: taxa de appealable por rejection_reason (somente rejected)
   ========================================================= */
WITH cs AS (
  SELECT
    cs.REJECTION_REASON,
    cs.APPEALABLE
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.STATE='rejected'
)
SELECT
  rejection_reason,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(appealable = TRUE)::NUMBER AS n_true,
  COUNT_IF(appealable = FALSE)::NUMBER AS n_false,
  COUNT_IF(appealable IS NULL)::NUMBER AS n_null,
  (COUNT_IF(appealable = TRUE) / NULLIF(COUNT(*),0))::FLOAT AS p_true
FROM cs
GROUP BY 1
ORDER BY n DESC
LIMIT 200
;

/* =========================================================
   [B] CS: taxa de appealable por (risk_capim, rejection_reason) via SCORE normalizado
   ========================================================= */
WITH cs AS (
  SELECT
    TRY_TO_NUMBER(REPLACE(cs.SCORE, ',', '.'))::NUMBER AS risk_capim,
    cs.REJECTION_REASON,
    cs.APPEALABLE
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.STATE='rejected'
    AND cs.SCORE IS NOT NULL
)
SELECT
  risk_capim,
  rejection_reason,
  COUNT(*)::NUMBER AS n,
  (COUNT_IF(appealable=TRUE) / NULLIF(COUNT(*),0))::FLOAT AS p_true
FROM cs
GROUP BY 1,2
HAVING COUNT(*) >= 200
ORDER BY p_true DESC, n DESC
LIMIT 200
;

/* =========================================================
   [C] Legado: razões de reprovação e volume (pre_analysis)
   ========================================================= */
SELECT
  pa.REJECTION_REASON,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
  AND pa.REJECTION_REASON IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 200
;

/* =========================================================
   [D] Interseção: razões do legado com p_true do CS
   ========================================================= */
WITH cs_rates AS (
  SELECT
    rejection_reason,
    (COUNT_IF(appealable = TRUE) / NULLIF(COUNT(*),0))::FLOAT AS p_true,
    COUNT(*)::NUMBER AS n_cs
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS
  WHERE state='rejected'
  GROUP BY 1
),
pa_reasons AS (
  SELECT
    pa.REJECTION_REASON AS rejection_reason,
    COUNT(*)::NUMBER AS n_pa
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
    AND pa.REJECTION_REASON IS NOT NULL
  GROUP BY 1
)
SELECT
  p.rejection_reason,
  p.n_pa,
  r.p_true,
  r.n_cs,
  IFF(r.p_true >= 0.80 AND r.n_cs >= 5000, 'HIGH', IFF(r.p_true >= 0.50 AND r.n_cs >= 1000, 'MED', 'LOW_OR_UNKNOWN')) AS suggested_bucket
FROM pa_reasons p
LEFT JOIN cs_rates r
  ON r.rejection_reason = p.rejection_reason
ORDER BY p.n_pa DESC
LIMIT 250
;

