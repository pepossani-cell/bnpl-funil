/*
  View oficial (camada única de consumo) — C1 unificado com dados comparáveis dos 4 eixos.

  Motivação:
    - Evitar duplicação do `credit_simulation` (não usar a linha 'credit_simulation' vinda de PRE_ANALYSES_ENRICHED_*).
    - Fornecer uma única “tabela oficial” de consumo com:
        chave canônica = (c1_entity_type, c1_entity_id)
        clinic_id
        4 eixos + *_source (para comparabilidade e auditoria)

  Pré-requisitos:
    - CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
    - CAPIM_DATA_DEV.POSSANI_SANDBOX.PRE_ANALYSES_ENRICHED_BORROWER (materializar via src/materialize_enriched_pre_analyses_borrower.py)
*/

/* Views não podem depender de variáveis de sessão: use nomes fully-qualified diretamente. */
DROP VIEW IF EXISTS CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER_V1;

CREATE OR REPLACE VIEW CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER AS
WITH
cs AS (
  SELECT
    'credit_simulation' AS c1_entity_type,
    credit_simulation_id::NUMBER AS c1_entity_id,
    cs_created_at AS c1_created_at,
    clinic_id,

    /* risk */
    risk_capim,
    risk_capim_subclass,
    payment_default_risk,

    /* estado/outcome */
    credit_simulation_state AS c1_state_raw,
    credit_simulation_was_approved AS c1_was_approved,
    IFF(credit_simulation_was_approved, 'approved', 'not_approved') AS c1_outcome_bucket,
    credit_simulation_rejection_reason AS c1_rejection_reason,
    c1_appealable AS c1_can_retry_with_financial_responsible,
    c1_appealable AS c1_appealable,
    NULL::FLOAT AS c1_appealable_prob,
    'canonical_credit_simulations'::TEXT AS c1_appealable_inference_source,

    /* valores */
    credit_lead_requested_amount AS c1_requested_amount,
    IFF(credit_simulation_was_approved, permitted_amount, NULL) AS c1_approved_amount,
    c1_has_counter_proposal,

    /* financing summary */
    financing_term_min,
    financing_term_max,
    financing_installment_value_min,
    financing_installment_value_max,
    financing_total_debt_min,
    financing_total_debt_max,

    /* eixo cadastro/demografia */
    borrower_birthdate,
    borrower_gender,
    borrower_city,
    IFF(borrower_state IS NOT NULL AND LENGTH(TRIM(borrower_state))=2, UPPER(TRIM(borrower_state)), NULL) AS borrower_state,
    NULLIF(REGEXP_REPLACE(borrower_zipcode, '\\D',''), '') AS borrower_zipcode,
    borrower_birthdate_source,
    borrower_gender_source,
    borrower_zipcode_source,
    cadastro_evidence_source,

    /* eixo negativação */
    pefin_count,
    refin_count,
    protesto_count,
    pefin_value,
    refin_value,
    protesto_value,
    total_negative_value,
    negativacao_source,

    /* eixo renda */
    IFF(income_estimated_source = 'sensitive_last_resort', income_estimated / 100.0, income_estimated) AS income_estimated,
    income_estimated_source,
    scr_operations_count,
    renda_proxies_source,

    /* eixo scores */
    serasa_score,
    serasa_score_source,
    boa_vista_score
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
),
pa AS (
  SELECT
    'pre_analysis' AS c1_entity_type,
    c1_entity_id::NUMBER AS c1_entity_id,
    c1_created_at,
    clinic_id,

    /* risk */
    TRY_TO_NUMBER(risk_capim)::NUMBER AS risk_capim,
    risk_capim_subclass,
    NULL::FLOAT AS payment_default_risk,

    /* estado/outcome */
    c1_state_raw,
    c1_was_approved,
    c1_outcome_bucket,
    c1_rejection_reason,
    c1_can_retry_with_financial_responsible,
    c1_appealable,
    c1_appealable_prob,
    c1_appealable_inference_source,

    /* valores */
    c1_requested_amount,
    c1_approved_amount,
    c1_has_counter_proposal,

    /* financing summary */
    financing_term_min,
    financing_term_max,
    financing_installment_value_min,
    financing_installment_value_max,
    financing_total_debt_min,
    financing_total_debt_max,

    borrower_birthdate,
    borrower_gender,
    borrower_city,
    IFF(borrower_state IS NOT NULL AND LENGTH(TRIM(borrower_state))=2, UPPER(TRIM(borrower_state)), NULL) AS borrower_state,
    NULLIF(REGEXP_REPLACE(borrower_zipcode, '\\D',''), '') AS borrower_zipcode,
    borrower_birthdate_source,
    borrower_gender_source,
    borrower_zipcode_source,
    cadastro_evidence_source,
    pefin_count,
    refin_count,
    protesto_count,
    pefin_value,
    refin_value,
    protesto_value,
    total_negative_value,
    negativacao_source,
    income_estimated,
    income_estimated_source,
    scr_operations_count,
    renda_proxies_source,
    serasa_score,
    serasa_score_source,
    boa_vista_score
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.PRE_ANALYSES_ENRICHED_BORROWER
  WHERE c1_entity_type = 'pre_analysis'
),

c1_union AS (
  SELECT * FROM cs
  UNION ALL
  SELECT * FROM pa
),

clinic_score_ranked AS (
  SELECT
    u.c1_entity_type,
    u.c1_entity_id,
    l.CLINIC_SCORE_CHANGED_AT,
    l.CLINIC_CREDIT_SCORE,
    ROW_NUMBER() OVER (
      PARTITION BY u.c1_entity_type, u.c1_entity_id
      ORDER BY l.CLINIC_SCORE_CHANGED_AT DESC
    ) AS rn
  FROM c1_union u
  LEFT JOIN CAPIM_DATA.CAPIM_ANALYTICS.CLINIC_SCORE_LOGS l
    ON l.CLINIC_ID = u.clinic_id
   AND l.CLINIC_SCORE_CHANGED_AT <= u.c1_created_at
)
SELECT
  u.*,

  /* risk safe-for-aggregation */
  u.risk_capim AS risk_capim_raw,
  IFF(u.risk_capim BETWEEN 0 AND 5, u.risk_capim, NULL) AS risk_capim_0_5,
  IFF(u.risk_capim IN (-1,9), TRUE, FALSE) AS risk_capim_is_special,
  CASE
    WHEN u.risk_capim = -1 THEN 'engine_error_or_unknown'
    WHEN u.risk_capim =  9 THEN 'special_high_risk'
    ELSE NULL
  END AS risk_capim_special_kind,

  /* clinic risk dynamic (temporal) */
  cs.CLINIC_CREDIT_SCORE AS clinic_credit_score_at_c1,
  cs.CLINIC_SCORE_CHANGED_AT AS clinic_credit_score_changed_at_matched,
  IFF(cs.CLINIC_SCORE_CHANGED_AT IS NULL, NULL, DATEDIFF('day', cs.CLINIC_SCORE_CHANGED_AT, u.c1_created_at)) AS clinic_credit_score_days_from_c1,
  CASE
    WHEN cs.CLINIC_SCORE_CHANGED_AT IS NULL THEN 'no_match'
    WHEN DATEDIFF('day', cs.CLINIC_SCORE_CHANGED_AT, u.c1_created_at) <= 7 THEN '<=7d'
    WHEN DATEDIFF('day', cs.CLINIC_SCORE_CHANGED_AT, u.c1_created_at) <= 30 THEN '<=30d'
    WHEN DATEDIFF('day', cs.CLINIC_SCORE_CHANGED_AT, u.c1_created_at) <= 90 THEN '<=90d'
    WHEN DATEDIFF('day', cs.CLINIC_SCORE_CHANGED_AT, u.c1_created_at) <= 365 THEN '<=365d'
    ELSE '>365d'
  END AS clinic_credit_score_match_stage
FROM c1_union u
LEFT JOIN clinic_score_ranked cs
  ON cs.c1_entity_type = u.c1_entity_type
 AND cs.c1_entity_id = u.c1_entity_id
 AND cs.rn = 1
;

