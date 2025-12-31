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
    IFF(
      credit_simulation_was_approved = FALSE
      AND under_age_patient_verified = TRUE
      AND borrower_role = 'patient',
      TRUE, FALSE
    ) AS c1_can_retry_with_financial_responsible,

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

    /* valores */
    c1_requested_amount,
    c1_approved_amount,
    IFF(c1_approved_amount IS NOT NULL AND c1_requested_amount IS NOT NULL AND c1_approved_amount + 1e-6 < c1_requested_amount, TRUE, FALSE) AS c1_has_counter_proposal,

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
)
SELECT * FROM cs
UNION ALL
SELECT * FROM pa
;

