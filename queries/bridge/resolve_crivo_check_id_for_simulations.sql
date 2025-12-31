/*
  Descoberta / imputação de CRIVO_CHECK_ID para CREDIT_SIMULATIONS com crivo_check_id nulo.

  Estratégia (alto recall, em camadas):
    0) Se existir linha em SOURCE_CRIVO_CHECKS com ENGINEABLE_ID = credit_simulation_id,
       usar esse CRIVO_CHECK_ID (preferir o mais próximo do cs_created_at).

    1) Caso contrário, usar CPF efetivo (responsável financeiro quando aplicável),
       e buscar crivo_checks por CPF no entorno do evento:
         - primary: ±1h do cs_created_at

    2) fallback: lookback de 15 dias ([-15d, 0]) por CPF.

  Observações:
  - CPF em KEY_PARAMETERS vem mascarado; normalizamos removendo não-dígitos.
  - Pode haver múltiplos crivo_checks; escolhemos o mais próximo por abs(minutos).
  - Não materializa nada; é uma query para exploração/validação.
*/

WITH params AS (
  SELECT
    1  ::INT AS primary_hours,
    15 ::INT AS cache_days,
    180::INT AS cap_days        -- fallback estendido (opcional) para capturar reuso mais antigo
),

cs_base AS (
  SELECT
    cs.id AS credit_simulation_id,
    cs.created_at AS cs_created_at,
    cs.retail_id AS clinic_id,
    cs.patient_id,
    cs.financial_responsible_id,
    cs.state,
    cs.rejection_reason,
    cs.crivo_check_id AS crivo_check_id_current
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.crivo_check_id IS NULL
),

cs_cpf AS (
  SELECT
    b.*,
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

crivo_base AS (
  SELECT
    c.CRIVO_CHECK_ID,
    c.ENGINEABLE_TYPE,
    c.ENGINEABLE_ID,
    c.CRIVO_CHECK_CREATED_AT,
    c.POLITICA,
    c.KEY_PARAMETERS:campos:"CPF"::string AS cpf_raw,
    REGEXP_REPLACE(c.KEY_PARAMETERS:campos:"CPF"::string, '\\D','') AS cpf_digits
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS c
  WHERE c.ENGINEABLE_TYPE = 'CreditSimulation'
),

-- Camada 0: match direto por engineable_id
direct_engineable AS (
  SELECT
    cs.credit_simulation_id,
    cs.cs_created_at,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    'engineable' AS stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS minutes_from_cs
  FROM cs_cpf cs
  JOIN crivo_base cb
    ON cb.ENGINEABLE_ID = cs.credit_simulation_id
),

has_engineable AS (
  SELECT credit_simulation_id, IFF(COUNT(*)>0,1,0) AS has_engineable
  FROM direct_engineable
  GROUP BY 1
),

-- Camada 1: primary ±1h por CPF (para quem não tem engineable)
primary_cpf AS (
  SELECT
    cs.credit_simulation_id,
    cs.cs_created_at,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    'cpf_primary' AS stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS minutes_from_cs
  FROM cs_cpf cs
  JOIN params p ON TRUE
  LEFT JOIN has_engineable he
    ON he.credit_simulation_id = cs.credit_simulation_id
  JOIN crivo_base cb
    ON cb.cpf_digits = cs.cpf_effective_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('hour', -p.primary_hours, cs.cs_created_at)
                                    AND DATEADD('hour',  p.primary_hours, cs.cs_created_at)
  WHERE COALESCE(he.has_engineable, 0) = 0
),

has_primary AS (
  SELECT credit_simulation_id, IFF(COUNT(*)>0,1,0) AS has_primary
  FROM primary_cpf
  GROUP BY 1
),

-- Camada 2: fallback lookback 15d por CPF (para quem não tem engineable nem primary)
fallback_cpf AS (
  SELECT
    cs.credit_simulation_id,
    cs.cs_created_at,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    'cpf_fallback_15d' AS stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS minutes_from_cs
  FROM cs_cpf cs
  JOIN params p ON TRUE
  LEFT JOIN has_engineable he
    ON he.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN has_primary hp
    ON hp.credit_simulation_id = cs.credit_simulation_id
  JOIN crivo_base cb
    ON cb.cpf_digits = cs.cpf_effective_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('day', -p.cache_days, cs.cs_created_at)
                                    AND cs.cs_created_at
  WHERE COALESCE(he.has_engineable, 0) = 0
    AND COALESCE(hp.has_primary, 0) = 0
),

-- Camada 3 (opcional): fallback estendido 180d por CPF (para quem não tem engineable/primary/fallback 15d)
has_fallback_15d AS (
  SELECT credit_simulation_id, IFF(COUNT(*)>0,1,0) AS has_fallback_15d
  FROM fallback_cpf
  GROUP BY 1
),

fallback_cpf_180d AS (
  SELECT
    cs.credit_simulation_id,
    cs.cs_created_at,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    'cpf_fallback_180d' AS stage,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, cs.cs_created_at) AS minutes_from_cs
  FROM cs_cpf cs
  JOIN params p ON TRUE
  LEFT JOIN has_engineable he
    ON he.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN has_primary hp
    ON hp.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN has_fallback_15d hf
    ON hf.credit_simulation_id = cs.credit_simulation_id
  JOIN crivo_base cb
    ON cb.cpf_digits = cs.cpf_effective_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('day', -p.cap_days, cs.cs_created_at)
                                    AND cs.cs_created_at
  WHERE COALESCE(he.has_engineable, 0) = 0
    AND COALESCE(hp.has_primary, 0) = 0
    AND COALESCE(hf.has_fallback_15d, 0) = 0
),

all_candidates AS (
  SELECT * FROM direct_engineable
  UNION ALL
  SELECT * FROM primary_cpf
  UNION ALL
  SELECT * FROM fallback_cpf
  UNION ALL
  SELECT * FROM fallback_cpf_180d
),

ranked AS (
  SELECT
    cs.*,
    cand.stage,
    cand.CRIVO_CHECK_ID AS crivo_check_id_suggested,
    cand.CRIVO_CHECK_CREATED_AT AS crivo_check_created_at,
    cand.minutes_from_cs,
    ROW_NUMBER() OVER (
      PARTITION BY cs.credit_simulation_id
      ORDER BY
        IFF(cand.stage='engineable', 0, IFF(cand.stage='cpf_primary', 1, 2)),
        ABS(cand.minutes_from_cs) ASC,
        cand.CRIVO_CHECK_CREATED_AT DESC,
        cand.CRIVO_CHECK_ID DESC
    ) AS rn_best
  FROM cs_cpf cs
  LEFT JOIN all_candidates cand
    ON cand.credit_simulation_id = cs.credit_simulation_id
)

SELECT
  credit_simulation_id,
  cs_created_at,
  clinic_id,
  state,
  rejection_reason,
  cpf_effective,
  stage,
  crivo_check_id_suggested,
  crivo_check_created_at,
  minutes_from_cs
FROM ranked
WHERE rn_best = 1
ORDER BY cs_created_at DESC
-- LIMIT 1000;


