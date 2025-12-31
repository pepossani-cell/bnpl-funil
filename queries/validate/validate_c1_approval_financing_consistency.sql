/*
  Audit final — coerência: aprovação vs financing + semântica de estados

  Perguntas que este arquivo responde:
    1) C1s "aprovados" têm financing? (por definição, deveriam)
       - validar no RAW/fonte e no materializado/view.
    2) Semântica de estados no legado:
       - expired parece "aprovado que expirou"? (deveria ter financing)
       - eligible é aprovação ou recusa com tentativa via responsável?
       - eligible_with_counter_proposal é aprovado com valor menor?
    3) Coerência simples:
       - reprovadas deveriam ter rejection_reason
       - aprovadas deveriam ter financing (mesmo se state muda com o tempo)
*/

/* =========================================================
   [A] CREDIT_SIMULATIONS (fonte) — approved_at vs financing_conditions
   ========================================================= */
WITH base AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    cs.CREATED_AT,
    cs.UPDATED_AT,
    cs.RETAIL_ID AS clinic_id,
    cs.STATE AS cs_state,
    cs.REJECTION_REASON,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount,
    cs.FINANCING_CONDITIONS
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
)
SELECT
  cs_state,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(approved_at IS NOT NULL)::NUMBER AS n_has_approved_at,
  COUNT_IF(permitted_amount IS NOT NULL AND permitted_amount > 0)::NUMBER AS n_permitted_gt_0,
  COUNT_IF(financing_conditions IS NOT NULL)::NUMBER AS n_financing_nonnull,
  COUNT_IF(TYPEOF(financing_conditions)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(financing_conditions))>0)::NUMBER AS n_financing_nonempty_object,
  COUNT_IF(REJECTION_REASON IS NOT NULL)::NUMBER AS n_has_rejection_reason
FROM base
GROUP BY 1
ORDER BY n DESC
;

/* =========================================================
   [B] CREDIT_SIMULATIONS (materializado) — was_approved vs financing_*
   ========================================================= */
SELECT
  credit_simulation_state,
  credit_simulation_was_approved,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(permitted_amount IS NOT NULL AND permitted_amount > 0)::NUMBER AS n_permitted_gt_0,
  COUNT_IF(financing_term_min IS NOT NULL)::NUMBER AS n_has_financing_term_min,
  COUNT_IF(financing_total_debt_min IS NOT NULL)::NUMBER AS n_has_financing_debt_min,
  COUNT_IF(credit_simulation_rejection_reason IS NOT NULL)::NUMBER AS n_has_rejection_reason
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
GROUP BY 1,2
ORDER BY credit_simulation_state, credit_simulation_was_approved DESC
;

/* =========================================================
   [C] C1 view (oficial) — aprovados devem ter financing (por tipo)
   ========================================================= */
SELECT
  c1_entity_type,
  c1_was_approved,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(financing_term_min IS NOT NULL)::NUMBER AS n_fin_term_min,
  COUNT_IF(financing_total_debt_min IS NOT NULL)::NUMBER AS n_fin_debt_min,
  COUNT_IF(c1_rejection_reason IS NOT NULL)::NUMBER AS n_has_rejection_reason,
  COUNT_IF(c1_was_approved = TRUE AND financing_term_min IS NULL AND financing_total_debt_min IS NULL)::NUMBER AS n_approved_missing_financing
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
GROUP BY 1,2
ORDER BY 1,2
;

/* =========================================================
   [D] PRE_ANALYSES (fonte) — estados e sinais de financing
   Observações:
     - FINANCING_CONDITIONS costuma vir NULL no legado.
     - interest_rates_array + min/max_term são proxy de "há oferta/financing".
   ========================================================= */
WITH base AS (
  SELECT
    pa.PRE_ANALYSIS_TYPE,
    pa.PRE_ANALYSIS_ID,
    pa.PRE_ANALYSIS_CREATED_AT,
    pa.PRE_ANALYSIS_UPDATED_AT,
    pa.RETAIL_ID AS clinic_id,
    pa.PRE_ANALYSIS_STATE,
    pa.REJECTION_REASON,
    pa.PRE_ANALYSIS_AMOUNT,
    pa.COUNTER_PROPOSAL_AMOUNT,
    pa.MINIMUM_TERM_AVAILABLE,
    pa.MAXIMUM_TERM_AVAILABLE,
    pa.INTEREST_RATES_ARRAY
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE = 'pre_analysis'
)
SELECT
  pre_analysis_state,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(rejection_reason IS NOT NULL)::NUMBER AS n_has_rejection_reason,
  COUNT_IF(counter_proposal_amount IS NOT NULL)::NUMBER AS n_has_counter_proposal,
  COUNT_IF(minimum_term_available IS NOT NULL AND maximum_term_available IS NOT NULL)::NUMBER AS n_has_term_range,
  COUNT_IF(interest_rates_array IS NOT NULL AND TYPEOF(interest_rates_array)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(interest_rates_array))>0)::NUMBER AS n_has_interest_rates,
  COUNT_IF(
    (minimum_term_available IS NOT NULL AND maximum_term_available IS NOT NULL)
    OR (interest_rates_array IS NOT NULL AND TYPEOF(interest_rates_array)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(interest_rates_array))>0)
  )::NUMBER AS n_has_any_financing_signal,
  COUNT_IF(counter_proposal_amount IS NOT NULL AND pre_analysis_amount IS NOT NULL AND counter_proposal_amount < pre_analysis_amount)::NUMBER AS n_counter_lt_requested
FROM base
GROUP BY 1
ORDER BY n DESC
;

/* =========================================================
   [E] PRE_ANALYSES (fonte) — casos contraditórios para inspeção
   ========================================================= */
SELECT
  PRE_ANALYSIS_ID,
  PRE_ANALYSIS_CREATED_AT,
  RETAIL_ID AS clinic_id,
  PRE_ANALYSIS_STATE,
  REJECTION_REASON,
  PRE_ANALYSIS_AMOUNT,
  COUNTER_PROPOSAL_AMOUNT,
  MINIMUM_TERM_AVAILABLE,
  MAXIMUM_TERM_AVAILABLE,
  TYPEOF(INTEREST_RATES_ARRAY) AS ir_type,
  IFF(TYPEOF(INTEREST_RATES_ARRAY)='OBJECT', ARRAY_SIZE(OBJECT_KEYS(INTEREST_RATES_ARRAY)), NULL) AS ir_keys
FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES
WHERE PRE_ANALYSIS_TYPE='pre_analysis'
  AND (
    /* "aprovado" mas sem sinais de financing */
    (PRE_ANALYSIS_STATE IN ('eligible','eligible_with_counter_proposal','expired')
      AND MINIMUM_TERM_AVAILABLE IS NULL
      AND MAXIMUM_TERM_AVAILABLE IS NULL
      AND (INTEREST_RATES_ARRAY IS NULL OR (TYPEOF(INTEREST_RATES_ARRAY)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(INTEREST_RATES_ARRAY))=0))
    )
    OR
    /* "reprovado" mas sem rejection_reason */
    (PRE_ANALYSIS_STATE IN ('rejected')
      AND REJECTION_REASON IS NULL
    )
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY PRE_ANALYSIS_STATE ORDER BY PRE_ANALYSIS_CREATED_AT DESC) <= 50
ORDER BY PRE_ANALYSIS_STATE, PRE_ANALYSIS_CREATED_AT DESC
;

/* =========================================================
   [F] CREDIT_SIMULATIONS — casos raros: was_approved=TRUE mas sem financing_*
   (para entender se é parse vazio/objeto vazio/bug upstream)
   ========================================================= */
SELECT
  credit_simulation_id,
  clinic_id,
  cs_created_at,
  credit_simulation_state,
  credit_simulation_was_approved,
  permitted_amount,
  financing_term_min,
  financing_term_max,
  financing_total_debt_min,
  financing_total_debt_max
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
WHERE credit_simulation_was_approved = TRUE
  AND financing_term_min IS NULL
  AND financing_total_debt_min IS NULL
ORDER BY cs_created_at DESC
LIMIT 50
;

/* =========================================================
   [G] PRE_ANALYSES legado — decompor 'expired' (com/sem financing; com/sem rejection_reason)
   ========================================================= */
WITH base AS (
  SELECT
    pa.PRE_ANALYSIS_ID,
    pa.PRE_ANALYSIS_CREATED_AT,
    pa.RETAIL_ID AS clinic_id,
    pa.PRE_ANALYSIS_STATE,
    pa.REJECTION_REASON,
    pa.MINIMUM_TERM_AVAILABLE,
    pa.MAXIMUM_TERM_AVAILABLE,
    pa.INTEREST_RATES_ARRAY,
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
  has_financing_signal,
  IFF(rejection_reason IS NULL, 'no_rejection_reason', 'has_rejection_reason') AS rejection_bucket,
  COUNT(*)::NUMBER AS n
FROM base
GROUP BY 1,2
ORDER BY 1,2
;

/* =========================================================
   [H] PRE_ANALYSES legado — 'expired' com financing: quais rejection_reason?
   ========================================================= */
WITH base AS (
  SELECT
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
  REJECTION_REASON,
  COUNT(*)::NUMBER AS n
FROM base
WHERE has_financing_signal = TRUE
GROUP BY 1
ORDER BY n DESC
LIMIT 50
;

/* =========================================================
   [I] PRE_ANALYSES legado — 'eligible' sem financing (sanity histórica)
   ========================================================= */
WITH base AS (
  SELECT
    pa.PRE_ANALYSIS_ID,
    pa.PRE_ANALYSIS_CREATED_AT,
    pa.RETAIL_ID AS clinic_id,
    pa.PRE_ANALYSIS_AMOUNT,
    pa.MINIMUM_TERM_AVAILABLE,
    pa.MAXIMUM_TERM_AVAILABLE,
    pa.INTEREST_RATES_ARRAY
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='pre_analysis'
    AND pa.PRE_ANALYSIS_STATE='eligible'
    AND pa.MINIMUM_TERM_AVAILABLE IS NULL
    AND pa.MAXIMUM_TERM_AVAILABLE IS NULL
    AND (pa.INTEREST_RATES_ARRAY IS NULL OR (TYPEOF(pa.INTEREST_RATES_ARRAY)='OBJECT' AND ARRAY_SIZE(OBJECT_KEYS(pa.INTEREST_RATES_ARRAY))=0))
)
SELECT *
FROM base
ORDER BY PRE_ANALYSIS_CREATED_AT DESC
LIMIT 50
;


