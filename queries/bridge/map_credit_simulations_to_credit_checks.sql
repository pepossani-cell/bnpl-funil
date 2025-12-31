/*
  Associação canônica (v2): credit_simulations -> incremental_credit_checks_api

  Objetivo:
  - Preparar um mapeamento coerente e performático no grão credit_simulation_id
  - Sem extrair features do DATA (isso fica para a próxima etapa)

  Premissas/decisões:
  - Fonte da verdade: CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS
  - CPF efetivo:
      se financial_responsible_id existe e != patient_id -> usa CPF do responsável financeiro
      senão -> usa CPF do paciente
  - Checks não têm clinic_id nesta tabela, então a associação é por CPF+tempo.
  - Heurística em 2 estágios:
      1) primary: checks em torno do evento (±primary_hours do cs_created_at)
      2) fallback: para sims sem primary, usar lookback de cache_days ([-cache_days, 0])

  Como usar:
  - Ajuste params e/ou filtros em cs_base.
  - Para inspeção amostral, use WHERE/ORDER/LIMIT no final.
*/

WITH params AS (
  SELECT
    1   ::INT  AS primary_hours,      -- janela do evento (±1h) p/ credit_checks
    15  ::INT  AS cache_days,         -- cache/lookback p/ credit_checks
    1   ::INT  AS crivo_primary_hours, -- janela do evento (±1h) p/ crivo_checks
    15  ::INT  AS crivo_cache_days,    -- cache/lookback p/ crivo_checks
    180 ::INT  AS crivo_cap_days       -- fallback estendido (opcional) p/ crivo_checks
),

cs_base AS (
  SELECT
    cs.id          AS credit_simulation_id,
    cs.credit_lead_id,
    cs.retail_id   AS clinic_id,
    cs.patient_id,
    cs.financial_responsible_id,
    cs.state,
    cs.rejection_reason,
    cs.permitted_amount / 100 AS credit_simulation_permitted_amount,
    cs.financing_conditions,
    cs.base_installments_amount,
    cs.crivo_check_id,
    cs.created_at  AS cs_created_at,
    cs.updated_at  AS cs_updated_at
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  -- Opcional: recortes por tempo/estado aqui.
),

cs_cpf AS (
  SELECT
    b.*,
    p.cpf  AS patient_cpf,
    fr.cpf AS financial_responsible_cpf,
    IFF(
      b.financial_responsible_id IS NOT NULL
      AND b.financial_responsible_id <> b.patient_id,
      fr.cpf,
      p.cpf
    ) AS cpf_effective,
    REGEXP_REPLACE(
      IFF(
        b.financial_responsible_id IS NOT NULL
        AND b.financial_responsible_id <> b.patient_id,
        fr.cpf,
        p.cpf
      ),
      '\\D',''
    ) AS cpf_effective_digits
  FROM cs_base b
  LEFT JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API p
    ON p.id = b.patient_id
  LEFT JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API fr
    ON fr.id = b.financial_responsible_id
),

cs_enriched AS (
  SELECT
    c.*,
    l.credit_lead_requested_amount,
    l.under_age_patient_verified
  FROM cs_cpf c
  LEFT JOIN CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_LEADS l
    ON l.credit_lead_id = c.credit_lead_id
),

/* ============================
   CRIVO: resolução de crivo_check_id
   ============================ */
crivo_base AS (
  SELECT
    c.CRIVO_CHECK_ID,
    c.ENGINEABLE_TYPE,
    c.ENGINEABLE_ID,
    c.CRIVO_CHECK_CREATED_AT,
    c.POLITICA,
    c.KEY_PARAMETERS:campos:"CPF"::string AS crivo_cpf_raw,
    REGEXP_REPLACE(c.KEY_PARAMETERS:campos:"CPF"::string, '\\D','') AS crivo_cpf_digits
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS c
  WHERE c.ENGINEABLE_TYPE = 'CreditSimulation'
),

crivo_candidates AS (
  /* Candidatos para sims sem crivo_check_id original */
  SELECT
    cs.credit_simulation_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'engineable' AS crivo_resolution_stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS crivo_minutes_from_cs
  FROM cs_enriched cs
  JOIN crivo_base cb
    ON cb.ENGINEABLE_ID = cs.credit_simulation_id
  WHERE cs.crivo_check_id IS NULL

  UNION ALL

  SELECT
    cs.credit_simulation_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'cpf_primary' AS crivo_resolution_stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS crivo_minutes_from_cs
  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN crivo_base cb
    ON cb.crivo_cpf_digits = cs.cpf_effective_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('hour', -p.crivo_primary_hours, cs.cs_created_at)
                                    AND DATEADD('hour',  p.crivo_primary_hours, cs.cs_created_at)
  WHERE cs.crivo_check_id IS NULL

  UNION ALL

  SELECT
    cs.credit_simulation_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'cpf_fallback_15d' AS crivo_resolution_stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS crivo_minutes_from_cs
  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN crivo_base cb
    ON cb.crivo_cpf_digits = cs.cpf_effective_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('day', -p.crivo_cache_days, cs.cs_created_at)
                                    AND cs.cs_created_at
  WHERE cs.crivo_check_id IS NULL

  UNION ALL

  /* Opcional: aumenta recall, mas com maior risco de falso match */
  SELECT
    cs.credit_simulation_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'cpf_fallback_180d' AS crivo_resolution_stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS crivo_minutes_from_cs
  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN crivo_base cb
    ON cb.crivo_cpf_digits = cs.cpf_effective_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('day', -p.crivo_cap_days, cs.cs_created_at)
                                    AND cs.cs_created_at
  WHERE cs.crivo_check_id IS NULL
),

crivo_ranked AS (
  SELECT
    credit_simulation_id,
    CRIVO_CHECK_ID,
    CRIVO_CHECK_CREATED_AT,
    POLITICA,
    crivo_resolution_stage,
    crivo_minutes_from_cs,
    ROW_NUMBER() OVER (
      PARTITION BY credit_simulation_id
      ORDER BY
        IFF(crivo_resolution_stage='engineable', 0,
          IFF(crivo_resolution_stage='cpf_primary', 1,
            IFF(crivo_resolution_stage='cpf_fallback_15d', 2, 3)
          )
        ),
        ABS(crivo_minutes_from_cs) ASC,
        CRIVO_CHECK_CREATED_AT DESC,
        CRIVO_CHECK_ID DESC
    ) AS rn_best
  FROM crivo_candidates
),

crivo_resolution AS (
  SELECT
    cs.credit_simulation_id,
    /* mantém original */
    cs.crivo_check_id AS crivo_check_id_original,

    /* resolved: original ou melhor candidato */
    COALESCE(cs.crivo_check_id, cr.CRIVO_CHECK_ID) AS crivo_check_id_resolved,
    IFF(cs.crivo_check_id IS NOT NULL, 'original', cr.crivo_resolution_stage) AS crivo_resolution_stage,
    cr.CRIVO_CHECK_CREATED_AT AS crivo_check_created_at,
    cr.crivo_minutes_from_cs,
    cr.POLITICA AS crivo_politica
  FROM cs_enriched cs
  LEFT JOIN crivo_ranked cr
    ON cr.credit_simulation_id = cs.credit_simulation_id
   AND cr.rn_best = 1
),

primary_matches AS (
  SELECT
    cs.credit_simulation_id,
    cs.cs_created_at,
    cs.clinic_id,
    cs.credit_lead_id,
    cs.cpf_effective,

    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    cc.new_data_format,

    'primary'     AS association_stage,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS minutes_from_cs

  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON cc.cpf = cs.cpf_effective
   AND cc.created_at BETWEEN DATEADD('hour', -p.primary_hours, cs.cs_created_at)
                        AND DATEADD('hour',  p.primary_hours, cs.cs_created_at)
),

has_primary AS (
  SELECT
    cs.credit_simulation_id,
    IFF(COUNT(pm.credit_check_id) > 0, 1, 0) AS has_primary
  FROM cs_enriched cs
  LEFT JOIN primary_matches pm
    ON pm.credit_simulation_id = cs.credit_simulation_id
  GROUP BY 1
),

fallback_matches AS (
  SELECT
    cs.credit_simulation_id,
    cs.cs_created_at,
    cs.clinic_id,
    cs.credit_lead_id,
    cs.cpf_effective,

    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    cc.new_data_format,

    'fallback'    AS association_stage,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS minutes_from_cs

  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN has_primary hp
    ON hp.credit_simulation_id = cs.credit_simulation_id
   AND hp.has_primary = 0
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON cc.cpf = cs.cpf_effective
   AND cc.created_at BETWEEN DATEADD('day', -p.cache_days, cs.cs_created_at)
                        AND cs.cs_created_at
),

all_matches AS (
  SELECT * FROM primary_matches
  UNION ALL
  SELECT * FROM fallback_matches
),

ranked AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id, m.association_stage
      ORDER BY ABS(m.minutes_from_cs) ASC, m.credit_check_created_at ASC, m.credit_check_id ASC
    ) AS rn_stage_closest,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id, m.source, m.kind, m.association_stage
      ORDER BY ABS(m.minutes_from_cs) ASC, m.credit_check_created_at ASC, m.credit_check_id ASC
    ) AS rn_stage_source_kind_closest
  FROM all_matches m
)

SELECT
  cs.credit_simulation_id,
  cs.credit_lead_id,
  cs.clinic_id,
  cs.patient_id,
  cs.financial_responsible_id,
  cs.patient_cpf,
  cs.financial_responsible_cpf,
  cs.cpf_effective,
  cr.crivo_check_id_original,
  cr.crivo_check_id_resolved,
  cr.crivo_resolution_stage,
  cr.crivo_check_created_at,
  cr.crivo_minutes_from_cs,
  cr.crivo_politica,

  cs.state,
  cs.rejection_reason,
  cs.credit_simulation_permitted_amount,
  cs.financing_conditions,
  cs.base_installments_amount,
  cs.crivo_check_id, -- campo original da fonte
  cs.cs_created_at,
  cs.cs_updated_at,

  cs.credit_lead_requested_amount,
  cs.under_age_patient_verified,

  r.association_stage,
  r.credit_check_id,
  r.credit_check_created_at,
  r.source,
  r.kind,
  r.new_data_format,
  r.minutes_from_cs,
  r.rn_stage_closest,
  r.rn_stage_source_kind_closest

FROM cs_enriched cs
LEFT JOIN crivo_resolution cr
  ON cr.credit_simulation_id = cs.credit_simulation_id
LEFT JOIN ranked r
  ON r.credit_simulation_id = cs.credit_simulation_id

-- Para inspeção amostral:
-- WHERE cs.cs_created_at >= '2025-01-01'
ORDER BY cs.cs_created_at DESC, r.association_stage, ABS(r.minutes_from_cs) ASC
-- LIMIT 500;


