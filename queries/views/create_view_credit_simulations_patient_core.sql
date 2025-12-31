/*
  View "patient core" (C1 / topo do funil BNPL)
  Objetivo: reduzir colunas para caracterizar o paciente e o contexto da simulação,
  mantendo o enrichment wide como base de debug/linhagem.

  Princípios:
  - Sem PII direta (CPF, nome, telefone etc.)
  - Sem IDs operacionais não necessários (patient_id, financial_responsible_id, etc.)
  - Mantém apenas o mínimo de contexto da simulação + eixos do paciente (cadastro, negativação, renda, scores)

  Ajuste DB/SCHEMA se necessário (por padrão, cria no schema atual).
*/

CREATE OR REPLACE VIEW VW_CREDIT_SIMULATIONS_PATIENT_CORE AS
SELECT
  /* chaves mínimas */
  credit_simulation_id,
  credit_lead_id,
  clinic_id,
  borrower_role,

  /* contexto do evento (topo do funil) */
  cs_created_at,
  credit_simulation_state,
  credit_simulation_rejection_reason,
  credit_simulation_was_approved,
  credit_lead_requested_amount,
  permitted_amount,
  total_credit_checks_count,

  /* oferta (resultado do motor) */
  financing_term_min,
  financing_term_max,
  financing_installment_value_min,
  financing_installment_value_max,
  financing_total_debt_min,
  financing_total_debt_max,

  /* ===== EIXO 1: cadastro/demografia ===== */
  borrower_birthdate,
  borrower_gender,
  borrower_zipcode,
  borrower_state,
  borrower_registration_status,
  borrower_has_phone,
  borrower_has_address,

  /* ===== EIXO 2: negativação/restrições ===== */
  pefin_count,
  refin_count,
  protesto_count,
  total_negative_value,

  /* ===== EIXO 3: renda/proxies ===== */
  income_estimated,
  income_estimated_source,

  /* ===== EIXO 4: scores (sem score canônico) ===== */
  serasa_score,
  serasa_score_source,
  boa_vista_score,
  bacen_internal_score,
  bacen_is_not_banked
FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1
;


