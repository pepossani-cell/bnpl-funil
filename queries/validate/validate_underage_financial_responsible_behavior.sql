/*
  Validar comportamento de menor de idade vs responsável financeiro (sem materializar).
  Perguntas:
    - under_age_patient_verified implica financial_responsible_id presente?
    - pode existir responsible mesmo sem under_age?
*/

WITH cs AS (
  SELECT
    cs.ID::NUMBER AS credit_simulation_id,
    cs.CREDIT_LEAD_ID,
    cs.RETAIL_ID AS clinic_id,
    cs.PATIENT_ID,
    cs.FINANCIAL_RESPONSIBLE_ID,
    cs.STATE,
    cs.REJECTION_REASON,
    cs.APPROVED_AT,
    (cs.PERMITTED_AMOUNT / 100.0)::FLOAT AS permitted_amount
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
),
leads AS (
  SELECT
    l.credit_lead_id,
    l.under_age_patient_verified
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_LEADS l
)
SELECT
  under_age_patient_verified,
  IFF(financial_responsible_id IS NOT NULL AND financial_responsible_id <> patient_id, TRUE, FALSE) AS has_financial_responsible_distinct,
  COUNT(*)::NUMBER AS n,
  COUNT_IF(state='rejected')::NUMBER AS n_rejected,
  COUNT_IF(state='expired')::NUMBER AS n_expired,
  COUNT_IF(state='approved')::NUMBER AS n_approved,
  COUNT_IF(approved_at IS NOT NULL)::NUMBER AS n_has_approved_at,
  COUNT_IF(permitted_amount > 0)::NUMBER AS n_permitted_gt_0
FROM cs
LEFT JOIN leads ON leads.credit_lead_id = cs.credit_lead_id
GROUP BY 1,2
ORDER BY 1,2
;

