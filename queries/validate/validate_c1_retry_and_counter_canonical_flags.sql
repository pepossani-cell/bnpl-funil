/*
  Validação (sem materializar):
    - CREDIT_SIMULATIONS.APPEALABLE como flag canônica de "retry com responsável"
    - PRE_ANALYSES.IS_ELEGIBLE_WITH_COUNTER_PROPOSAL como flag canônica de contra-proposta (legado)
    - Relação patient/financial_responsible no legado via PATIENT_FIN_RESPONSIBLE_RELATIONSHIP
*/

/* =========================================================
   [A] CREDIT_SIMULATIONS: appealable por state/rejection_reason
   ========================================================= */
WITH base AS (
  SELECT
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
)
SELECT
  state,
  appealable,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(approved_at IS NOT NULL)::NUMBER AS n_has_approved_at,
  COUNT_IF(permitted_amount > 0)::NUMBER AS n_permitted_gt_0
FROM base
GROUP BY 1,2
ORDER BY state, appealable DESC
;

WITH base AS (
  SELECT
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPEALABLE
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.STATE='rejected'
)
SELECT
  appealable,
  rejection_reason,
  COUNT(*)::NUMBER AS n
FROM base
GROUP BY 1,2
ORDER BY appealable DESC, n DESC
LIMIT 80
;

/* =========================================================
   [B] CREDIT_SIMULATIONS: coluna SCORE (investigar se é o risk 0..5/-1/9)
   ========================================================= */
SELECT
  cs.SCORE,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
WHERE cs.SCORE IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 80
;

SELECT
  COUNT(*)::NUMBER AS n,
  COUNT_IF(TRY_TO_NUMBER(SCORE) IS NOT NULL)::NUMBER AS n_numeric_like,
  COUNT_IF(TRY_TO_NUMBER(SCORE) IN (-1,0,1,2,3,4,5,9))::NUMBER AS n_in_expected_set,
  COUNT_IF(TRY_TO_NUMBER(SCORE) NOT IN (-1,0,1,2,3,4,5,9) AND TRY_TO_NUMBER(SCORE) IS NOT NULL)::NUMBER AS n_numeric_outside_set
FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS
WHERE SCORE IS NOT NULL
;

/* =========================================================
   [C] PRE_ANALYSES (legado): is_elegible_with_counter_proposal vs state e valores
   ========================================================= */
WITH base AS (
  SELECT
    pa.PRE_ANALYSIS_STATE,
    pa.IS_ELEGIBLE_WITH_COUNTER_PROPOSAL,
    pa.PRE_ANALYSIS_AMOUNT,
    pa.COUNTER_PROPOSAL_AMOUNT,
    pa.REJECTION_REASON,
    pa.PATIENT_FIN_RESPONSIBLE_RELATIONSHIP,
    pa.FINANCIAL_RESPONSIBLE_ID
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
)
SELECT
  pre_analysis_state,
  is_elegible_with_counter_proposal,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(counter_proposal_amount IS NOT NULL)::NUMBER AS n_counter_amount,
  COUNT_IF(pre_analysis_amount IS NOT NULL AND counter_proposal_amount IS NOT NULL AND counter_proposal_amount < pre_analysis_amount)::NUMBER AS n_counter_lt_requested,
  COUNT_IF(rejection_reason IS NOT NULL)::NUMBER AS n_has_rejection_reason,
  COUNT_IF(financial_responsible_id IS NOT NULL)::NUMBER AS n_has_fin_resp_id
FROM base
GROUP BY 1,2
ORDER BY pre_analysis_state, is_elegible_with_counter_proposal DESC
;

/* top relacionamentos */
SELECT
  patient_fin_responsible_relationship,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES
WHERE PRE_ANALYSIS_TYPE='pre_analysis'
  AND PATIENT_FIN_RESPONSIBLE_RELATIONSHIP IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 50
;

