/*
  Painel canônico (SÉRIE HISTÓRICA) — C1 unificado para análise dos 4 eixos.

  Importante (evitar duplicação):
    - `credit_simulation` deve vir SOMENTE de CREDIT_SIMULATIONS_ENRICHED_BORROWER (canônico).
    - `pre_analysis` (legado) deve vir de PRE_ANALYSES_ENRICHED_BORROWER filtrando c1_entity_type='pre_analysis'.

  Requisitos:
    - CREDIT_SIMULATIONS_ENRICHED_BORROWER materializada.
    - PRE_ANALYSES_ENRICHED_BORROWER materializada via `src/materialize_enriched_pre_analyses_borrower.py`.
*/

SET cs_table = 'CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER';
SET pa_table = 'CAPIM_DATA_DEV.POSSANI_SANDBOX.PRE_ANALYSES_ENRICHED_BORROWER';

cs AS (
  SELECT
    'credit_simulation' AS c1_entity_type,
    credit_simulation_id::NUMBER AS c1_entity_id,
    cs_created_at AS c1_created_at,
    clinic_id,
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
    credit_lead_requested_amount AS c1_requested_amount,
    permitted_amount AS c1_approved_amount,
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
    borrower_state,
    borrower_zipcode,
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
    income_estimated,
    income_estimated_source,
    scr_operations_count,
    renda_proxies_source,
    /* eixo scores */
    serasa_score,
    serasa_score_source,
    boa_vista_score
  FROM IDENTIFIER($cs_table)
),

pa AS (
  SELECT
    c1_entity_type,
    c1_entity_id::NUMBER AS c1_entity_id,
    c1_created_at AS c1_created_at,
    clinic_id,
    c1_state_raw,
    c1_was_approved,
    c1_outcome_bucket,
    c1_rejection_reason,
    c1_can_retry_with_financial_responsible,
    c1_requested_amount,
    c1_approved_amount,
    financing_term_min,
    financing_term_max,
    financing_installment_value_min,
    financing_installment_value_max,
    financing_total_debt_min,
    financing_total_debt_max,
    borrower_birthdate,
    borrower_gender,
    borrower_city,
    borrower_state,
    borrower_zipcode,
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
  FROM IDENTIFIER($pa_table)
  WHERE c1_entity_type = 'pre_analysis'
)

SELECT * FROM cs
UNION ALL
SELECT * FROM pa
;

