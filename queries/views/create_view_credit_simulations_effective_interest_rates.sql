/*
  View derivada — taxa efetiva a partir de financing_conditions (Snowflake)

  Por que uma view derivada?
  - Calcular taxa efetiva (solver numérico) é mais caro do que extrair min/max de termos/parcela/dívida.
  - Mantemos o enrichment base (queries/enrich/enrich_credit_simulations_borrower.sql) leve e reutilizável.
  - A view pode ser usada quando você realmente precisa de juros efetivo / comparações econômicas.

  O que esta view produz (por credit_simulation_id):
  - term_min/max
  - installment_value_at_term_min/max
  - total_debt_at_term_min/max
  - rate_effective_monthly_{min,max} (aprox via binary search)
  - rate_effective_annual_{min,max} = (1+r)^12 - 1

  Premissas:
  - PV (principal) = permitted_amount (credit_simulations), em reais (centavos/100)
  - PMT = installment_value do cenário (centavos/100)
  - N = term (meses)

  IMPORTANTE:
  - Ajuste o DB/SCHEMA da view conforme seu ambiente.
  - Se preferir, aplique um filtro por período (ex.: últimos 12 meses) para reduzir custo.
*/

-- Ajuste aqui:
-- USE DATABASE <DB>;
-- USE SCHEMA <SCHEMA>;

CREATE OR REPLACE VIEW V_CREDIT_SIMULATIONS_EFFECTIVE_RATES AS
WITH params AS (
  SELECT 30::INT AS n_iter, 1e-8::FLOAT AS r_low_init, 2.0::FLOAT AS r_high_init
),

cs AS (
  SELECT
    cs.id AS credit_simulation_id,
    cs.created_at AS cs_created_at,
    cs.permitted_amount AS permitted_amount_cents,
    cs.financing_conditions
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.financing_conditions IS NOT NULL
    AND TYPEOF(cs.financing_conditions) = 'OBJECT'
  -- Opcional (recomendado para custo): AND cs.created_at >= DATEADD('month', -12, CURRENT_DATE())
),

offers AS (
  SELECT
    s.credit_simulation_id,
    TRY_TO_NUMBER(f.key::string) AS term_months,
    TRY_TO_NUMBER(f.value:installment_value::string) AS installment_value_cents,
    TRY_TO_NUMBER(f.value:total_debt_amount::string) AS total_debt_amount_cents
  FROM cs s
  , LATERAL FLATTEN(input => s.financing_conditions) f
),

summary AS (
  SELECT
    credit_simulation_id,
    MIN(term_months) AS term_min,
    MAX(term_months) AS term_max
  FROM offers
  GROUP BY 1
),

scenarios AS (
  SELECT
    o.credit_simulation_id,
    'min_term' AS scenario,
    o.term_months AS n_months,
    o.installment_value_cents,
    o.total_debt_amount_cents
  FROM offers o
  JOIN summary s
    ON s.credit_simulation_id = o.credit_simulation_id
   AND s.term_min = o.term_months

  UNION ALL

  SELECT
    o.credit_simulation_id,
    'max_term' AS scenario,
    o.term_months AS n_months,
    o.installment_value_cents,
    o.total_debt_amount_cents
  FROM offers o
  JOIN summary s
    ON s.credit_simulation_id = o.credit_simulation_id
   AND s.term_max = o.term_months
),

rate_base AS (
  SELECT
    sc.credit_simulation_id,
    sc.scenario,
    sc.n_months,
    (c.permitted_amount_cents / 100.0)::FLOAT AS pv,
    (sc.installment_value_cents / 100.0)::FLOAT AS pmt,
    sc.installment_value_cents,
    sc.total_debt_amount_cents,
    IFF(sc.n_months IS NOT NULL AND sc.n_months > 0 AND c.permitted_amount_cents IS NOT NULL AND c.permitted_amount_cents > 0 AND sc.installment_value_cents IS NOT NULL AND sc.installment_value_cents > 0, 1, 0) AS is_valid_input
  FROM scenarios sc
  JOIN cs c
    ON c.credit_simulation_id = sc.credit_simulation_id
),

solver_base AS (
  SELECT
    rb.credit_simulation_id,
    rb.scenario,
    rb.n_months,
    rb.pv,
    rb.pmt,
    0 AS iter,
    p.r_low_init AS r_low,
    p.r_high_init AS r_high
  FROM rate_base rb
  JOIN params p ON TRUE
  WHERE rb.is_valid_input = 1
),

solver AS (
  WITH RECURSIVE rs (
    credit_simulation_id,
    scenario,
    n_months,
    pv,
    pmt,
    iter,
    r_low,
    r_high,
    r_mid,
    pv_mid
  ) AS (
    SELECT
      b.credit_simulation_id,
      b.scenario,
      b.n_months,
      b.pv,
      b.pmt,
      b.iter,
      b.r_low,
      b.r_high,
      ((b.r_low + b.r_high) / 2.0)::FLOAT AS r_mid,
      (b.pmt * (1 - POWER(1 + ((b.r_low + b.r_high) / 2.0), -b.n_months)) / NULLIF(((b.r_low + b.r_high) / 2.0), 0))::FLOAT AS pv_mid
    FROM solver_base b

    UNION ALL

    SELECT
      rs.credit_simulation_id,
      rs.scenario,
      rs.n_months,
      rs.pv,
      rs.pmt,
      rs.iter + 1 AS iter,
      IFF(rs.pv_mid > rs.pv, rs.r_mid, rs.r_low)  AS r_low,
      IFF(rs.pv_mid > rs.pv, rs.r_high, rs.r_mid) AS r_high,
      ((IFF(rs.pv_mid > rs.pv, rs.r_mid, rs.r_low) + IFF(rs.pv_mid > rs.pv, rs.r_high, rs.r_mid)) / 2.0)::FLOAT AS r_mid,
      (rs.pmt * (1 - POWER(1 + ((IFF(rs.pv_mid > rs.pv, rs.r_mid, rs.r_low) + IFF(rs.pv_mid > rs.pv, rs.r_high, rs.r_mid)) / 2.0), -rs.n_months)) /
        NULLIF(((IFF(rs.pv_mid > rs.pv, rs.r_mid, rs.r_low) + IFF(rs.pv_mid > rs.pv, rs.r_high, rs.r_mid)) / 2.0), 0)
      )::FLOAT AS pv_mid
    FROM rs
    JOIN params p ON TRUE
    WHERE rs.iter + 1 < p.n_iter
  )
  SELECT * FROM rs
),

rate_final AS (
  SELECT
    credit_simulation_id,
    scenario,
    n_months,
    r_mid AS rate_effective_monthly
  FROM solver
  QUALIFY ROW_NUMBER() OVER (PARTITION BY credit_simulation_id, scenario ORDER BY iter DESC) = 1
),

wide AS (
  SELECT
    rb.credit_simulation_id,
    MAX(IFF(rb.scenario='min_term', rb.n_months, NULL)) AS term_min,
    MAX(IFF(rb.scenario='max_term', rb.n_months, NULL)) AS term_max,
    MAX(IFF(rb.scenario='min_term', rb.installment_value_cents, NULL)) AS installment_value_at_term_min_cents,
    MAX(IFF(rb.scenario='max_term', rb.installment_value_cents, NULL)) AS installment_value_at_term_max_cents,
    MAX(IFF(rb.scenario='min_term', rb.total_debt_amount_cents, NULL)) AS total_debt_at_term_min_cents,
    MAX(IFF(rb.scenario='max_term', rb.total_debt_amount_cents, NULL)) AS total_debt_at_term_max_cents,
    MAX(IFF(rf.scenario='min_term', rf.rate_effective_monthly, NULL)) AS rate_effective_monthly_min_term,
    MAX(IFF(rf.scenario='max_term', rf.rate_effective_monthly, NULL)) AS rate_effective_monthly_max_term
  FROM rate_base rb
  LEFT JOIN rate_final rf
    ON rf.credit_simulation_id = rb.credit_simulation_id
   AND rf.scenario = rb.scenario
  GROUP BY 1
)

SELECT
  w.credit_simulation_id,
  w.term_min,
  w.term_max,
  w.installment_value_at_term_min_cents,
  (w.installment_value_at_term_min_cents / 100.0)::FLOAT AS installment_value_at_term_min,
  w.installment_value_at_term_max_cents,
  (w.installment_value_at_term_max_cents / 100.0)::FLOAT AS installment_value_at_term_max,
  w.total_debt_at_term_min_cents,
  (w.total_debt_at_term_min_cents / 100.0)::FLOAT AS total_debt_at_term_min,
  w.total_debt_at_term_max_cents,
  (w.total_debt_at_term_max_cents / 100.0)::FLOAT AS total_debt_at_term_max,
  w.rate_effective_monthly_min_term,
  w.rate_effective_monthly_max_term,
  IFF(w.rate_effective_monthly_min_term IS NOT NULL, POWER(1 + w.rate_effective_monthly_min_term, 12) - 1, NULL) AS rate_effective_annual_min_term,
  IFF(w.rate_effective_monthly_max_term IS NOT NULL, POWER(1 + w.rate_effective_monthly_max_term, 12) - 1, NULL) AS rate_effective_annual_max_term
FROM wide w
;


