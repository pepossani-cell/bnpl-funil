/*
  Validação profunda (raio-x) — C1 oficial enriquecido
  Alvo:
    - CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER (view oficial)
    - CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
    - CAPIM_DATA_DEV.POSSANI_SANDBOX.PRE_ANALYSES_ENRICHED_BORROWER

  Objetivos:
    - Duplicidade e grão: 1 linha por (c1_entity_type, c1_entity_id)
    - Escala/unidade: valores monetários comparáveis (reais vs centavos), percentis, outliers
    - Coerência lógica: aprovado vs valores, min/max, não-negatividade, formatos UF/CEP
*/

/* =========================================================
   [A] Contagem + unicidade (view oficial)
   ========================================================= */
SELECT
  c1_entity_type,
  COUNT(*)::NUMBER AS n_rows,
  COUNT(DISTINCT c1_entity_type || ':' || c1_entity_id)::NUMBER AS n_distinct_key,
  (COUNT(*) - COUNT(DISTINCT c1_entity_type || ':' || c1_entity_id))::NUMBER AS n_duplicate_rows
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
GROUP BY 1
ORDER BY 1
;

/* =========================================================
   [B] Duplicatas (se existirem): lista top offenders
   ========================================================= */
SELECT
  c1_entity_type,
  c1_entity_id,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY n DESC, c1_entity_type, c1_entity_id
LIMIT 200
;

/* =========================================================
   [C] A view bate com suas bases? (contagens)
   ========================================================= */
SELECT
  'cs_table' AS src,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
UNION ALL
SELECT
  'pa_table_pre_analysis_only' AS src,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.PRE_ANALYSES_ENRICHED_BORROWER
WHERE c1_entity_type = 'pre_analysis'
UNION ALL
SELECT
  'view_credit_simulation' AS src,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
WHERE c1_entity_type = 'credit_simulation'
UNION ALL
SELECT
  'view_pre_analysis' AS src,
  COUNT(*)::NUMBER AS n
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
WHERE c1_entity_type = 'pre_analysis'
;

/* =========================================================
   [D] Distribuição de valores monetários (percentis + outliers)
   - flags para detectar centavos vs reais
   ========================================================= */
WITH base AS (
  SELECT
    c1_entity_type,
    c1_requested_amount,
    c1_approved_amount,
    financing_installment_value_min,
    financing_installment_value_max,
    financing_total_debt_min,
    financing_total_debt_max,
    income_estimated,
    pefin_value,
    refin_value,
    protesto_value,
    total_negative_value
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
),
stats AS (
  SELECT
    c1_entity_type,

    /* requested */
    COUNT_IF(c1_requested_amount IS NOT NULL) AS n_req,
    APPROX_PERCENTILE(c1_requested_amount, 0.50) AS req_p50,
    APPROX_PERCENTILE(c1_requested_amount, 0.95) AS req_p95,
    APPROX_PERCENTILE(c1_requested_amount, 0.99) AS req_p99,
    MAX(c1_requested_amount) AS req_max,
    COUNT_IF(c1_requested_amount > 500000) AS n_req_gt_500k,
    COUNT_IF(c1_requested_amount > 5000000) AS n_req_gt_5m,

    /* approved */
    COUNT_IF(c1_approved_amount IS NOT NULL) AS n_app,
    APPROX_PERCENTILE(c1_approved_amount, 0.50) AS app_p50,
    APPROX_PERCENTILE(c1_approved_amount, 0.95) AS app_p95,
    APPROX_PERCENTILE(c1_approved_amount, 0.99) AS app_p99,
    MAX(c1_approved_amount) AS app_max,

    /* installment */
    COUNT_IF(financing_installment_value_min IS NOT NULL) AS n_pmt_min,
    APPROX_PERCENTILE(financing_installment_value_min, 0.50) AS pmt_min_p50,
    APPROX_PERCENTILE(financing_installment_value_min, 0.99) AS pmt_min_p99,
    MAX(financing_installment_value_min) AS pmt_min_max,

    /* total debt */
    COUNT_IF(financing_total_debt_min IS NOT NULL) AS n_debt_min,
    APPROX_PERCENTILE(financing_total_debt_min, 0.50) AS debt_min_p50,
    APPROX_PERCENTILE(financing_total_debt_min, 0.99) AS debt_min_p99,
    MAX(financing_total_debt_min) AS debt_min_max,

    /* income */
    COUNT_IF(income_estimated IS NOT NULL) AS n_income,
    APPROX_PERCENTILE(income_estimated, 0.50) AS income_p50,
    APPROX_PERCENTILE(income_estimated, 0.99) AS income_p99,
    MAX(income_estimated) AS income_max,

    /* negativation value */
    COUNT_IF(total_negative_value IS NOT NULL) AS n_neg_total,
    APPROX_PERCENTILE(total_negative_value, 0.50) AS neg_p50,
    APPROX_PERCENTILE(total_negative_value, 0.99) AS neg_p99,
    MAX(total_negative_value) AS neg_max,

    /* “assinatura” de escala: quantos valores parecem inteiros? (típico de centavos) */
    COUNT_IF(c1_requested_amount IS NOT NULL AND ABS(c1_requested_amount - ROUND(c1_requested_amount)) < 1e-6) AS n_req_integer_like,
    COUNT_IF(c1_requested_amount IS NOT NULL AND ABS((c1_requested_amount * 100) - ROUND(c1_requested_amount * 100)) < 1e-6) AS n_req_2dp_like
  FROM base
  GROUP BY 1
)
SELECT *
FROM stats
ORDER BY c1_entity_type
;

/* =========================================================
   [E] Regras de coerência: aprovado vs valores
   ========================================================= */
SELECT
  c1_entity_type,
  COUNT(*) AS n,
  COUNT_IF(c1_was_approved = TRUE) AS n_approved,
  COUNT_IF(c1_was_approved = FALSE) AS n_not_approved,
  COUNT_IF(c1_was_approved IS NULL) AS n_approved_unknown,

  /* aprovado mas sem valor aprovado */
  COUNT_IF(c1_was_approved = TRUE AND c1_approved_amount IS NULL) AS n_approved_missing_amount,

  /* não aprovado mas com valor aprovado */
  COUNT_IF(c1_was_approved = FALSE AND c1_approved_amount IS NOT NULL) AS n_not_approved_with_amount,

  /* aprovado com approved_amount <= 0 */
  COUNT_IF(c1_was_approved = TRUE AND c1_approved_amount IS NOT NULL AND c1_approved_amount <= 0) AS n_approved_amount_nonpos
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
GROUP BY 1
ORDER BY 1
;

/* =========================================================
   [F] Financing: integridade (min/max, ranges, sinais)
   ========================================================= */
SELECT
  c1_entity_type,
  COUNT(*) AS n,

  COUNT_IF(financing_term_min IS NOT NULL AND financing_term_max IS NOT NULL AND financing_term_min > financing_term_max) AS n_term_min_gt_max,
  COUNT_IF(financing_term_min IS NOT NULL AND financing_term_min <= 0) AS n_term_min_nonpos,
  COUNT_IF(financing_term_max IS NOT NULL AND financing_term_max <= 0) AS n_term_max_nonpos,
  COUNT_IF(financing_term_max IS NOT NULL AND financing_term_max > 120) AS n_term_max_gt_120,

  COUNT_IF(financing_installment_value_min IS NOT NULL AND financing_installment_value_max IS NOT NULL AND financing_installment_value_min > financing_installment_value_max) AS n_pmt_min_gt_max,
  COUNT_IF(financing_total_debt_min IS NOT NULL AND financing_total_debt_max IS NOT NULL AND financing_total_debt_min > financing_total_debt_max) AS n_debt_min_gt_max,

  COUNT_IF(financing_installment_value_min IS NOT NULL AND financing_installment_value_min <= 0) AS n_pmt_min_nonpos,
  COUNT_IF(financing_total_debt_min IS NOT NULL AND financing_total_debt_min <= 0) AS n_debt_min_nonpos
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
GROUP BY 1
ORDER BY 1
;

/* =========================================================
   [G] Financing vs principal: total_debt >= principal (quando ambos existem)
   e parcela não absurda vs principal
   ========================================================= */
SELECT
  c1_entity_type,
  COUNT(*) AS n,
  COUNT_IF(
    financing_total_debt_min IS NOT NULL
    AND (
      IFF(c1_entity_type='credit_simulation', c1_approved_amount, c1_requested_amount)
    ) IS NOT NULL
    AND financing_total_debt_min + 1e-6 < IFF(c1_entity_type='credit_simulation', c1_approved_amount, c1_requested_amount)
  ) AS n_debt_lt_principal,
  COUNT_IF(
    financing_installment_value_min IS NOT NULL
    AND (
      IFF(c1_entity_type='credit_simulation', c1_approved_amount, c1_requested_amount)
    ) IS NOT NULL
    AND financing_installment_value_min > IFF(c1_entity_type='credit_simulation', c1_approved_amount, c1_requested_amount)
  ) AS n_installment_gt_principal,
  COUNT_IF(
    financing_installment_value_min IS NOT NULL
    AND (
      IFF(c1_entity_type='credit_simulation', c1_approved_amount, c1_requested_amount)
    ) IS NOT NULL
    AND financing_installment_value_min > (IFF(c1_entity_type='credit_simulation', c1_approved_amount, c1_requested_amount) / 2.0)
  ) AS n_installment_gt_half_principal
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
GROUP BY 1
ORDER BY 1
;

/* =========================================================
   [H] Negativação: não-negatividade + integridade total_negative_value
   ========================================================= */
WITH base AS (
  SELECT
    c1_entity_type,
    pefin_value,
    refin_value,
    protesto_value,
    total_negative_value
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
),
calc AS (
  SELECT
    c1_entity_type,
    pefin_value,
    refin_value,
    protesto_value,
    total_negative_value,
    (COALESCE(pefin_value,0) + COALESCE(refin_value,0) + COALESCE(protesto_value,0)) AS sum_components
  FROM base
)
SELECT
  c1_entity_type,
  COUNT(*) AS n,
  COUNT_IF(pefin_value IS NOT NULL AND pefin_value < 0) AS n_pefin_value_negative,
  COUNT_IF(refin_value IS NOT NULL AND refin_value < 0) AS n_refin_value_negative,
  COUNT_IF(protesto_value IS NOT NULL AND protesto_value < 0) AS n_protesto_value_negative,
  COUNT_IF(total_negative_value IS NOT NULL AND total_negative_value < 0) AS n_total_negative_value_negative,

  /* total != soma (tolerância pequena) */
  COUNT_IF(total_negative_value IS NOT NULL AND ABS(total_negative_value - sum_components) > 0.01) AS n_total_mismatch,
  APPROX_PERCENTILE(ABS(total_negative_value - sum_components), 0.50) AS mismatch_abs_p50,
  APPROX_PERCENTILE(ABS(total_negative_value - sum_components), 0.99) AS mismatch_abs_p99
FROM calc
GROUP BY 1
ORDER BY 1
;

/* =========================================================
   [I] Formatos: UF/CEP
   ========================================================= */
SELECT
  c1_entity_type,
  COUNT(*) AS n,
  COUNT_IF(borrower_state IS NOT NULL AND (LENGTH(TRIM(borrower_state)) <> 2 OR borrower_state <> UPPER(borrower_state))) AS n_state_bad_format,
  COUNT_IF(borrower_zipcode IS NOT NULL AND (LENGTH(REGEXP_REPLACE(borrower_zipcode, '\\D','')) <> 8)) AS n_zip_bad_len,
  COUNT_IF(borrower_zipcode IS NOT NULL AND borrower_zipcode <> REGEXP_REPLACE(borrower_zipcode, '\\D','')) AS n_zip_has_non_digits
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
GROUP BY 1
ORDER BY 1
;

/* =========================================================
   [J] “Casos estranhos” (amostra) — ajuda inspeção manual
   ========================================================= */
SELECT
  c1_entity_type,
  c1_entity_id,
  c1_created_at,
  clinic_id,
  c1_was_approved,
  c1_outcome_bucket,
  c1_rejection_reason,
  c1_requested_amount,
  c1_approved_amount,
  financing_term_min,
  financing_term_max,
  financing_installment_value_min,
  financing_total_debt_min,
  income_estimated,
  serasa_score,
  boa_vista_score,
  pefin_count,
  refin_count,
  protesto_count,
  total_negative_value,
  negativacao_source,
  renda_proxies_source,
  cadastro_evidence_source,
  serasa_score_source
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
WHERE
  /* valores suspeitos (potencial centavos/outliers) */
  (c1_requested_amount IS NOT NULL AND c1_requested_amount > 1000000)
  OR (income_estimated IS NOT NULL AND income_estimated > 500000)
  OR (financing_installment_value_min IS NOT NULL AND c1_requested_amount IS NOT NULL AND financing_installment_value_min > c1_requested_amount)
  OR (total_negative_value IS NOT NULL AND total_negative_value > 5000000)
QUALIFY ROW_NUMBER() OVER (PARTITION BY c1_entity_type ORDER BY c1_created_at DESC) <= 50
ORDER BY c1_entity_type, c1_created_at DESC
;

/* =========================================================
   [K] Escala por fonte — income_estimated_source
   (onde mora o risco de “centavos vs reais”)
   ========================================================= */
WITH base AS (
  SELECT
    c1_entity_type,
    income_estimated_source,
    income_estimated
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
  WHERE income_estimated IS NOT NULL
)
SELECT
  c1_entity_type,
  income_estimated_source,
  COUNT(*)::NUMBER AS n,
  APPROX_PERCENTILE(income_estimated, 0.50) AS p50,
  APPROX_PERCENTILE(income_estimated, 0.95) AS p95,
  APPROX_PERCENTILE(income_estimated, 0.99) AS p99,
  MAX(income_estimated) AS max,
  COUNT_IF(ABS(income_estimated - ROUND(income_estimated)) < 1e-6) AS n_integer_like,
  COUNT_IF(ABS((income_estimated * 100) - ROUND(income_estimated * 100)) < 1e-6) AS n_2dp_like
FROM base
GROUP BY 1,2
ORDER BY c1_entity_type, n DESC
LIMIT 200
;

/* =========================================================
   [L] Escala por fonte — negativacao_source (valores)
   ========================================================= */
WITH base AS (
  SELECT
    c1_entity_type,
    negativacao_source,
    total_negative_value
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
  WHERE total_negative_value IS NOT NULL
)
SELECT
  c1_entity_type,
  negativacao_source,
  COUNT(*)::NUMBER AS n,
  APPROX_PERCENTILE(total_negative_value, 0.50) AS p50,
  APPROX_PERCENTILE(total_negative_value, 0.95) AS p95,
  APPROX_PERCENTILE(total_negative_value, 0.99) AS p99,
  MAX(total_negative_value) AS max
FROM base
GROUP BY 1,2
ORDER BY c1_entity_type, n DESC
LIMIT 200
;

/* =========================================================
   [M] Inspeção de UF (top values) — ajuda decidir normalização/mapeamento
   ========================================================= */
SELECT
  c1_entity_type,
  borrower_state,
  COUNT(*)::NUMBER AS n,
  MIN(LENGTH(TRIM(borrower_state))) AS min_len,
  MAX(LENGTH(TRIM(borrower_state))) AS max_len
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER
WHERE borrower_state IS NOT NULL
GROUP BY 1,2
ORDER BY c1_entity_type, n DESC
LIMIT 200
;

