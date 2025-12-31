/*
  v1 — Enriquecimento auditável (1 linha por credit_simulation_id) com os 4 eixos:
    1) cadastro/demografia
    2) negativação/restrições
    3) renda/proxies
    4) scores

  Fonte da verdade:
    - CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS

  Regras/fallbacks (fonte de verdade):
    - docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md
    - docs/reference/CREDIT_CHECKS_INCREMENTAL_API_NOTES.md
    - docs/reference/CRIVO_CHECKS_NOTES.md
    - queries/bridge/map_credit_simulations_to_credit_checks.sql

  Objetivo:
    - Pushdown total no Snowflake (sem processamento local)
    - Colunas explícitas de linhagem/source para evitar ambiguidade

  LGPD:
    - Evita persistir PII “rica” (nome, endereço linha) — mantém flags e alguns campos (birthdate/zip) conforme necessidade analítica.
*/

WITH params AS (
  SELECT
    1  ::INT AS primary_hours,
    15 ::INT AS cache_days,
    1  ::INT AS crivo_primary_hours,
    15 ::INT AS crivo_cache_days,
    180::INT AS crivo_cap_days
),

/* ============================
   Fonte da verdade (simulações)
   ============================ */
cs_base AS (
  SELECT
    cs.id          AS credit_simulation_id,
    cs.credit_lead_id,
    cs.retail_id   AS clinic_id,
    cs.patient_id,
    cs.financial_responsible_id,
    cs.state,
    cs.rejection_reason,
    cs.approved_at,
    cs.crivo_check_id,
    cs.appealable,
    cs.score AS risk_score_raw,
    cs.payment_default_risk,
    (cs.permitted_amount / 100.0)::FLOAT AS permitted_amount,
    cs.financing_conditions,
    cs.created_at  AS cs_created_at,
    cs.updated_at  AS cs_updated_at
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  -- Opcional: recortes por tempo/estado aqui (para iteração).
),

/* ============================
   CPF efetivo + borrower_id (pessoa efetiva) + lead auxiliares
   ============================ */
cs_people AS (
  SELECT
    b.*,
    IFF(
      b.financial_responsible_id IS NOT NULL
      AND b.financial_responsible_id <> b.patient_id,
      b.financial_responsible_id,
      b.patient_id
    ) AS borrower_person_id,
    IFF(
      b.financial_responsible_id IS NOT NULL
      AND b.financial_responsible_id <> b.patient_id,
      'financial_responsible',
      'patient'
    ) AS borrower_role
  FROM cs_base b
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
  FROM cs_people b
  LEFT JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API p
    ON p.id = b.patient_id
  LEFT JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API fr
    ON fr.id = b.financial_responsible_id
),

cs_enriched AS (
  SELECT
    c.*,
    (l.credit_lead_requested_amount)::FLOAT AS credit_lead_requested_amount,
    l.under_age_patient_verified
  FROM cs_cpf c
  LEFT JOIN CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_LEADS l
    ON l.credit_lead_id = c.credit_lead_id
),

/* ============================
   FINANCING CONDITIONS (C1): prazo / parcela / dívida total (min/max)
   - financing_conditions é OBJECT com chaves sendo o prazo (ex.: "6","10",...)
   - valores observados: installment_value, term, total_debt_amount (em centavos)
   ============================ */
financing_offers AS (
  SELECT
    cs.credit_simulation_id,
    TRY_TO_NUMBER(f.key::string) AS term_months,
    TRY_TO_NUMBER(f.value:installment_value::string) AS installment_value_cents,
    TRY_TO_NUMBER(f.value:total_debt_amount::string) AS total_debt_amount_cents
  FROM cs_enriched cs
  , LATERAL FLATTEN(input => cs.financing_conditions) f
  WHERE cs.financing_conditions IS NOT NULL
    AND TYPEOF(cs.financing_conditions) = 'OBJECT'
),

financing_summary AS (
  SELECT
    credit_simulation_id,
    MIN(term_months) AS financing_term_min,
    MAX(term_months) AS financing_term_max,
    MIN(installment_value_cents) AS financing_installment_value_min_cents,
    MAX(installment_value_cents) AS financing_installment_value_max_cents,
    MIN(total_debt_amount_cents) AS financing_total_debt_min_cents,
    MAX(total_debt_amount_cents) AS financing_total_debt_max_cents
  FROM financing_offers
  GROUP BY 1
),

financing_features AS (
  SELECT
    s.credit_simulation_id,
    s.financing_term_min,
    s.financing_term_max,
    (s.financing_installment_value_min_cents / 100.0)::FLOAT AS financing_installment_value_min,
    (s.financing_installment_value_max_cents / 100.0)::FLOAT AS financing_installment_value_max,
    (s.financing_total_debt_min_cents / 100.0)::FLOAT AS financing_total_debt_min,
    (s.financing_total_debt_max_cents / 100.0)::FLOAT AS financing_total_debt_max
  FROM financing_summary s
),

borrower_sensitive AS (
  SELECT
    s.id AS borrower_person_id,
    s.birthdate,
    s.city,
    s.state,
    s.zipcode,
    s.monthly_income,
    s.occupation
  FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API s
),

/* ============================
   RISK (Capim) via PRE_ANALYSES (type=credit_simulation)
   - PRE_ANALYSES carrega RISK_CAPIM/RISK_CAPIM_SUBCLASS no grão (type,id)
   - Join seguro: PRE_ANALYSIS_ID == CREDIT_SIMULATIONS.id quando PRE_ANALYSIS_TYPE='credit_simulation'
   ============================ */
pa_cs_dedup AS (
  SELECT
    pa.PRE_ANALYSIS_ID::NUMBER AS credit_simulation_id,
    TRY_TO_NUMBER(pa.RISK_CAPIM)::NUMBER AS risk_capim,
    pa.RISK_CAPIM_SUBCLASS::TEXT AS risk_capim_subclass
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_TYPE='credit_simulation'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pa.PRE_ANALYSIS_ID
    ORDER BY pa.PRE_ANALYSIS_UPDATED_AT DESC, pa.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
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
    c.BUREAU_CHECK_INFO,
    c.KEY_PARAMETERS,
    c.KEY_PARAMETERS:campos:"CPF"::string AS crivo_cpf_raw,
    REGEXP_REPLACE(c.KEY_PARAMETERS:campos:"CPF"::string, '\\D','') AS crivo_cpf_digits
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS c
  WHERE c.ENGINEABLE_TYPE = 'CreditSimulation'
),

crivo_candidates AS (
  SELECT
    cs.credit_simulation_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'engineable' AS crivo_resolution_stage,
    DATEDIFF(
      'minute',
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cb.CRIVO_CHECK_CREATED_AT),
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cs.cs_created_at)
    ) AS crivo_minutes_from_cs
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
    DATEDIFF(
      'minute',
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cb.CRIVO_CHECK_CREATED_AT),
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cs.cs_created_at)
    ) AS crivo_minutes_from_cs
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
    DATEDIFF(
      'minute',
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cb.CRIVO_CHECK_CREATED_AT),
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cs.cs_created_at)
    ) AS crivo_minutes_from_cs
  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN crivo_base cb
    ON cb.crivo_cpf_digits = cs.cpf_effective_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('day', -p.crivo_cache_days, cs.cs_created_at)
                                    AND cs.cs_created_at
  WHERE cs.crivo_check_id IS NULL

  UNION ALL

  SELECT
    cs.credit_simulation_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'cpf_fallback_180d' AS crivo_resolution_stage,
    DATEDIFF(
      'minute',
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cb.CRIVO_CHECK_CREATED_AT),
      CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', cs.cs_created_at)
    ) AS crivo_minutes_from_cs
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
    cs.crivo_check_id AS crivo_check_id_original,
    COALESCE(cs.crivo_check_id, cr.CRIVO_CHECK_ID) AS crivo_check_id_resolved,
    IFF(cs.crivo_check_id IS NOT NULL, 'original', cr.crivo_resolution_stage) AS crivo_resolution_stage,
    COALESCE(cb_original.CRIVO_CHECK_CREATED_AT, cr.CRIVO_CHECK_CREATED_AT) AS crivo_check_created_at,
    COALESCE(
      DATEDIFF('minute', COALESCE(cb_original.CRIVO_CHECK_CREATED_AT, cr.CRIVO_CHECK_CREATED_AT), cs.cs_created_at),
      cr.crivo_minutes_from_cs
    ) AS crivo_minutes_from_cs,
    COALESCE(cb_original.POLITICA, cr.POLITICA) AS crivo_politica
  FROM cs_enriched cs
  LEFT JOIN crivo_base cb_original
    ON cb_original.CRIVO_CHECK_ID = cs.crivo_check_id
  LEFT JOIN crivo_ranked cr
    ON cr.credit_simulation_id = cs.credit_simulation_id
   AND cr.rn_best = 1
),

/* ============================
   CRIVO: features (BUREAU_CHECK_INFO:campos + KEY_PARAMETERS:campos)
   ============================ */
crivo_features_campos AS (
  SELECT
    cr.credit_simulation_id,
    MAX(IFF(f.value:nome::string = 'PEFIN Serasa',    TRY_TO_NUMBER(f.value:valor::string), NULL)) AS crivo_pefin_serasa,
    MAX(IFF(f.value:nome::string = 'REFIN Serasa',    TRY_TO_NUMBER(f.value:valor::string), NULL)) AS crivo_refin_serasa,
    MAX(IFF(f.value:nome::string = 'Protesto Serasa', TRY_TO_NUMBER(f.value:valor::string), NULL)) AS crivo_protesto_serasa,
    MAX(IFF(f.value:nome::string = 'Score Serasa',
            IFF(TRY_TO_NUMBER(f.value:valor::string) > 0, TRY_TO_NUMBER(f.value:valor::string), NULL),
            NULL)) AS crivo_score_serasa,
    /* Renda presumida (CREDILINK / SERASA) — geralmente vem como string monetária (PT-BR) */
    MAX(
      IFF(
        LOWER(TRIM(f.value:nome::string)) LIKE 'credilink%renda presumida%',
        TRY_TO_NUMBER(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(NULLIF(TRIM(f.value:valor::string), ''), '\"', ''),
                  'R$', ''
                ),
                '.',''
              ),
              ',','.'
            ),
            ' ',''
          )
        ),
        NULL
      )
    ) AS crivo_renda_presumida_credilink,
    MAX(
      IFF(
        LOWER(TRIM(f.value:nome::string)) LIKE 'serasa%renda presumida%',
        TRY_TO_NUMBER(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(NULLIF(TRIM(f.value:valor::string), ''), '\"', ''),
                  'R$', ''
                ),
                '.',''
              ),
              ',','.'
            ),
            ' ',''
          )
        ),
        NULL
      )
    ) AS crivo_renda_presumida_serasa,
    /* Cadastro: fallback de birthdate observado via bureau BVS dentro do Crivo
       Importante: restringir ao campo correto para evitar capturar datas não-nascimento. */
    MAX(IFF(f.value:nome::string = 'Data de Nascimento BVS',
            TRY_TO_DATE(NULLIF(TRIM(f.value:valor::string),''), 'DD/MM/YYYY'),
            NULL)) AS crivo_birthdate_bvs,
    MAX(IFF(f.value:nome::string = 'CEP do Proponente',     f.value:valor::string, NULL)) AS crivo_zipcode,
    MAX(IFF(f.value:nome::string = 'Telefone do proponente', f.value:valor::string, NULL)) AS crivo_phone_raw,
    MAX(IFF(f.value:nome::string = 'Telefone do proponente' AND NULLIF(TRIM(f.value:valor::string),'') IS NOT NULL, 1, 0)) AS crivo_has_phone
  FROM crivo_resolution cr
  JOIN crivo_base cb
    ON cb.CRIVO_CHECK_ID = cr.crivo_check_id_resolved
  , LATERAL FLATTEN(input => cb.BUREAU_CHECK_INFO:campos) f
  GROUP BY 1
),

crivo_features_key_params AS (
  SELECT
    cr.credit_simulation_id,
    TRY_TO_NUMBER(cb.KEY_PARAMETERS:campos:BacenScore::string) AS crivo_bacen_score,
    /* tentativa: às vezes CreditLimits é número/string; se for objeto/array, fica NULL */
    TRY_TO_NUMBER(
      REPLACE(
        REPLACE(
          REPLACE(cb.KEY_PARAMETERS:campos:CreditLimits::string, '\"', ''),
          '.',''
        ),
        ',','.'
      )
    ) AS crivo_credit_limits_value
    ,
    /* Proxies adicionais (formato monetário PT-BR como string: "1.272,15", "0,0", etc.) */
    TRY_TO_NUMBER(
      REPLACE(
        REPLACE(
          REPLACE(NULLIF(TRIM(cb.KEY_PARAMETERS:campos:OverduePortfolio::string), ''), '\"', ''),
          '.',''
        ),
        ',','.'
      )
    ) AS crivo_overdue_portfolio_value,
    TRY_TO_NUMBER(
      REPLACE(
        REPLACE(
          REPLACE(NULLIF(TRIM(cb.KEY_PARAMETERS:campos:Loss::string), ''), '\"', ''),
          '.',''
        ),
        ',','.'
      )
    ) AS crivo_loss_value
  FROM crivo_resolution cr
  JOIN crivo_base cb
    ON cb.CRIVO_CHECK_ID = cr.crivo_check_id_resolved
),

/* ============================
   CRIVO: DataBusca PF (dentro de BUREAU_CHECK_INFO.drivers[*].produtos[...])
   - Aqui encontramos um "código sexo" com bom sinal (inferido via cruzamento com SERASA):
     1 -> M, 2 -> F (0/outros -> NULL)
   ============================ */
crivo_databusca_pf_raw AS (
  SELECT
    cr.credit_simulation_id,
    MAX(
      IFF(
        /* Pode aparecer como "lista - código sexo" ou variações; pegamos qualquer nome contendo 'sexo' */
        LOWER(p.value:nome::string) LIKE '%sexo%',
        NULLIF(TRIM(p.value:valor::string), ''),
        NULL
      )
    ) AS crivo_sexo_codigo_raw
  FROM crivo_resolution cr
  JOIN crivo_base cb
    ON cb.CRIVO_CHECK_ID = cr.crivo_check_id_resolved
  , LATERAL FLATTEN(input => cb.BUREAU_CHECK_INFO:drivers) d
  , LATERAL FLATTEN(input => d.value:produtos:"api DataBusca - Consulta Dados Pessoa - PF") p
  GROUP BY 1
),

crivo_features_databusca_pf AS (
  SELECT
    credit_simulation_id,
    crivo_sexo_codigo_raw,
    CASE
      WHEN crivo_sexo_codigo_raw = '1' THEN 'M'
      WHEN crivo_sexo_codigo_raw = '2' THEN 'F'
      ELSE NULL
    END AS crivo_gender
  FROM crivo_databusca_pf_raw
),

/* ============================
   CREDIT CHECKS: associação por CPF (dígitos) + tempo (com leniência auditável)
   Preferência (menor rank = mais estrito):
     0) strict_primary_1h    : [cs-1h,  cs+1h]
     1) lenient_primary_24h  : [cs-24h, cs+24h]
     2) lenient_fallback_15d : [cs-15d, cs]
     3) lenient_fallback_180d: [cs-180d, cs]
   ============================ */
cc_matches_strict_1h AS (
  SELECT
    cs.credit_simulation_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data       AS credit_check_data,
    'strict_primary_1h' AS credit_check_match_stage,
    0 AS credit_check_match_leniency_rank,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS credit_check_minutes_from_cs
  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('hour', -p.primary_hours, cs.cs_created_at)
                        AND DATEADD('hour',  p.primary_hours, cs.cs_created_at)
),

cc_matches_lenient_24h AS (
  SELECT
    cs.credit_simulation_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data       AS credit_check_data,
    'lenient_primary_24h' AS credit_check_match_stage,
    1 AS credit_check_match_leniency_rank,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS credit_check_minutes_from_cs
  FROM cs_enriched cs
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('hour', -24, cs.cs_created_at)
                        AND DATEADD('hour',  24, cs.cs_created_at)
),

cc_matches_fallback_15d AS (
  SELECT
    cs.credit_simulation_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data       AS credit_check_data,
    'lenient_fallback_15d' AS credit_check_match_stage,
    2 AS credit_check_match_leniency_rank,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS credit_check_minutes_from_cs
  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('day', -p.cache_days, cs.cs_created_at)
                        AND cs.cs_created_at
),

cc_matches_fallback_180d AS (
  SELECT
    cs.credit_simulation_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data       AS credit_check_data,
    'lenient_fallback_180d' AS credit_check_match_stage,
    3 AS credit_check_match_leniency_rank,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS credit_check_minutes_from_cs
  FROM cs_enriched cs
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('day', -p.crivo_cap_days, cs.cs_created_at)
                        AND cs.cs_created_at
),

cc_all_matches AS (
  SELECT * FROM cc_matches_strict_1h
  UNION ALL
  SELECT * FROM cc_matches_lenient_24h
  UNION ALL
  SELECT * FROM cc_matches_fallback_15d
  UNION ALL
  SELECT * FROM cc_matches_fallback_180d
),

cc_best_per_source_kind AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id, m.source, m.kind
      ORDER BY
        m.credit_check_match_leniency_rank ASC,
        ABS(m.credit_check_minutes_from_cs) ASC,
        m.credit_check_created_at DESC,
        m.credit_check_id DESC
    ) AS rn_best_source_kind
  FROM cc_all_matches m
),

cc_best_source_kind_1 AS (
  SELECT *
  FROM cc_best_per_source_kind
  WHERE rn_best_source_kind = 1
),

/* ============================
   Seletores por “família” (serasa novo/antigo, bacen, boa vista)
   ============================ */
cc_best_serasa_new_score_without_income_1 AS (
  /* SERASA new com reports/negativeData/registration */
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'serasa'
    AND (
      COALESCE(new_data_format, FALSE) = TRUE
      /* Mitigação de “misflag”: alguns registros vêm com new_data_format=FALSE mas payload é new (OBJECT com reports) */
      OR (TYPEOF(credit_check_data) = 'OBJECT' AND credit_check_data:reports IS NOT NULL)
    )
    AND kind = 'check_score_without_income'
),

cc_best_serasa_new_income_only_1 AS (
  /* SERASA new com score/range/scoreModel no top-level (sem reports) */
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'serasa'
    AND (
      COALESCE(new_data_format, FALSE) = TRUE
      OR (TYPEOF(credit_check_data) = 'OBJECT' AND credit_check_data:reports IS NOT NULL)
    )
    AND kind = 'check_income_only'
),

cc_best_serasa_new AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id
      ORDER BY
        m.credit_check_match_leniency_rank ASC,
        ABS(m.credit_check_minutes_from_cs) ASC,
        m.credit_check_created_at DESC,
        m.credit_check_id DESC
    ) AS rn_best
  FROM cc_all_matches m
  WHERE m.source = 'serasa'
    AND (
      COALESCE(m.new_data_format, FALSE) = TRUE
      OR (TYPEOF(m.credit_check_data) = 'OBJECT' AND m.credit_check_data:reports IS NOT NULL)
    )
),

cc_best_serasa_new_1 AS (
  SELECT *
  FROM cc_best_serasa_new
  WHERE rn_best = 1
),

cc_best_serasa_old AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id
      ORDER BY
        m.credit_check_match_leniency_rank ASC,
        ABS(m.credit_check_minutes_from_cs) ASC,
        m.credit_check_created_at DESC,
        m.credit_check_id DESC
    ) AS rn_best
  FROM cc_all_matches m
  WHERE m.source = 'serasa'
    AND COALESCE(m.new_data_format, FALSE) = FALSE
    /* Evitar classificar como “old” quando o payload é claramente new (OBJECT com reports) */
    AND NOT (TYPEOF(m.credit_check_data) = 'OBJECT' AND m.credit_check_data:reports IS NOT NULL)
),

cc_best_serasa_old_1 AS (
  SELECT *
  FROM cc_best_serasa_old
  WHERE rn_best = 1
),

cc_best_bacen_internal_score AS (
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'bacen_internal_score'
),

cc_best_boa_vista_score_pf AS (
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'boa_vista_score_pf'
),

/* === NOVA FONTE === */
cc_best_boa_vista_scpc_net AS (
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'boa_vista_scpc_net'
),

/* ============================
   SERASA novo: reports[] -> report preferido -> registration + negativeData summary + score/range
   ============================ */
serasa_new_reports AS (
  SELECT
    s.credit_simulation_id,
    s.credit_check_id,
    s.credit_check_created_at,
    s.credit_check_match_stage,
    s.credit_check_minutes_from_cs,
    s.credit_check_match_leniency_rank,
    s.kind AS serasa_new_kind,
    s.credit_check_data,
    r.value AS report,
    r.index AS report_index
  FROM cc_best_serasa_new_score_without_income_1 s
  , LATERAL FLATTEN(input => s.credit_check_data:reports) r
),

serasa_new_best_report AS (
  SELECT
    sr.*,
    ROW_NUMBER() OVER (
      PARTITION BY sr.credit_simulation_id
      ORDER BY
        IFF(sr.report:reportName::string = 'COMBO_CONCESSAO', 0, 1),
        sr.report_index ASC
    ) AS rn_best_report
  FROM serasa_new_reports sr
),

serasa_new_best_report_1 AS (
  SELECT *
  FROM serasa_new_best_report
  WHERE rn_best_report = 1
),

serasa_new_features AS (
  SELECT
    s.credit_simulation_id,

    /* linhagem do check */
    s.credit_check_id         AS serasa_new_credit_check_id,
    s.credit_check_created_at AS serasa_new_credit_check_created_at,
    s.credit_check_match_stage AS serasa_new_match_stage,
    s.credit_check_minutes_from_cs   AS serasa_new_minutes_from_cs,
    s.credit_check_match_leniency_rank AS serasa_new_match_leniency_rank,
    s.kind AS serasa_new_kind,
    br.report:reportName::string AS serasa_new_report_name,

    /* registration */
    TRY_TO_DATE(br.report:registration:birthDate::string) AS serasa_new_birthdate,
    br.report:registration:consumerGender::string         AS serasa_new_gender,
    br.report:registration:address:zipCode::string        AS serasa_new_zipcode,
    br.report:registration:statusRegistration::string     AS serasa_new_status_registration,
    TRY_TO_DATE(br.report:registration:statusDate::string) AS serasa_new_status_date,
    IFF(br.report:registration:birthDate IS NOT NULL, 1, 0) AS serasa_new_has_birthdate,
    IFF(br.report:registration:consumerGender IS NOT NULL, 1, 0) AS serasa_new_has_gender,
    IFF(br.report:registration:address:zipCode IS NOT NULL, 1, 0) AS serasa_new_has_zipcode,
    IFF(br.report:registration:phone IS NOT NULL, 1, 0) AS serasa_new_has_phone,
    IFF(br.report:registration:address IS NOT NULL, 1, 0) AS serasa_new_has_address,

    /* negativeData summary (counts) */
    TRY_TO_NUMBER(br.report:negativeData:pefin:summary:count::string)  AS serasa_new_pefin_count,
    TRY_TO_NUMBER(br.report:negativeData:refin:summary:count::string)  AS serasa_new_refin_count,
    TRY_TO_NUMBER(br.report:negativeData:notary:summary:count::string) AS serasa_new_notary_count,
    TRY_TO_NUMBER(br.report:negativeData:check:summary:count::string)  AS serasa_new_check_count,

    /* balances (quando disponíveis) */
    TRY_TO_NUMBER(br.report:negativeData:pefin:summary:balance::string)  AS serasa_new_pefin_balance,
    TRY_TO_NUMBER(br.report:negativeData:refin:summary:balance::string)  AS serasa_new_refin_balance,
    TRY_TO_NUMBER(br.report:negativeData:notary:summary:balance::string) AS serasa_new_notary_balance,
    TRY_TO_NUMBER(br.report:negativeData:check:summary:balance::string)  AS serasa_new_check_balance,

    /* score/range/model (quando o payload trouxer)
       Observado com frequência em `KIND='check_income_only'` no top-level,
       mas pode existir em blocos alternativos (`data` ou dentro do report). */
    /* normalização: SERASA new (check_income_only) tende a devolver score “escalado”
       - valores como 435000 parecem representar 435.0 (÷1000)
       - valores como 8975000 parecem representar 897.5 (÷10000)
       - valores <= 0 (ex.: -1) tratamos como NULL */
    IFF(
      /* clip final para evitar implausíveis */
      (
        CASE
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 /* alguns payloads trazem report.score como OBJETO: {score,range,scoreModel,...} */
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) IS NULL THEN NULL
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) <= 0 THEN NULL
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) >= 1000000 THEN COALESCE(
                                  TRY_TO_NUMBER(s.credit_check_data:score::string),
                                  TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                                  TRY_TO_NUMBER(br.report:score:score::string),
                                  TRY_TO_NUMBER(br.report:score::string)
                                ) / 10000
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) >= 1000 THEN COALESCE(
                                TRY_TO_NUMBER(s.credit_check_data:score::string),
                                TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                                TRY_TO_NUMBER(br.report:score:score::string),
                                TRY_TO_NUMBER(br.report:score::string)
                              ) / 1000
          ELSE COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               )
        END
      ) > 1000,
      NULL,
      (
        CASE
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) IS NULL THEN NULL
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) <= 0 THEN NULL
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) >= 1000000 THEN COALESCE(
                                  TRY_TO_NUMBER(s.credit_check_data:score::string),
                                  TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                                  TRY_TO_NUMBER(br.report:score:score::string),
                                  TRY_TO_NUMBER(br.report:score::string)
                                ) / 10000
          WHEN COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               ) >= 1000 THEN COALESCE(
                                TRY_TO_NUMBER(s.credit_check_data:score::string),
                                TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                                TRY_TO_NUMBER(br.report:score:score::string),
                                TRY_TO_NUMBER(br.report:score::string)
                              ) / 1000
          ELSE COALESCE(
                 TRY_TO_NUMBER(s.credit_check_data:score::string),
                 TRY_TO_NUMBER(s.credit_check_data:data:score::string),
                 TRY_TO_NUMBER(br.report:score:score::string),
                 TRY_TO_NUMBER(br.report:score::string)
               )
        END
      )
    ) AS serasa_new_score,

    COALESCE(
      s.credit_check_data:range::string,
      s.credit_check_data:data:range::string,
      br.report:range::string,
      br.report:score:range::string
    ) AS serasa_new_score_range,

    COALESCE(
      s.credit_check_data:scoreModel::string,
      s.credit_check_data:data:scoreModel::string,
      br.report:scoreModel::string,
      br.report:score:scoreModel::string
    ) AS serasa_new_score_model,

    CASE
      WHEN TRY_TO_NUMBER(s.credit_check_data:score::string) IS NOT NULL THEN 'serasa_new_top_level'
      WHEN TRY_TO_NUMBER(s.credit_check_data:data:score::string) IS NOT NULL THEN 'serasa_new_data_block'
      WHEN TRY_TO_NUMBER(br.report:score:score::string) IS NOT NULL THEN 'serasa_new_report_score_object'
      WHEN TRY_TO_NUMBER(br.report:score::string) IS NOT NULL THEN 'serasa_new_report_score_scalar'
      ELSE NULL
    END AS serasa_new_score_source_detail,

    'credit_checks_serasa_new' AS serasa_new_source
  FROM cc_best_serasa_new_score_without_income_1 s
  LEFT JOIN serasa_new_best_report_1 br
    ON br.credit_simulation_id = s.credit_simulation_id
),

/* ============================
   SERASA new (income_only): retorno "score/range/scoreModel" no top-level (sem reports)
   IMPORTANTE (semântica):
     - Em parte relevante dos payloads (ex.: scoreModel=HRP9), `score` representa RENDA ESTIMADA em CENTAVOS (e não um score 0..1000).
     - Por isso, aqui extraímos um proxy de renda (`serasa_income_estimated`) de forma type-safe e com sanidade.
   ============================ */
serasa_new_income_only_features AS (
  SELECT
    s.credit_simulation_id,
    s.credit_check_id AS serasa_income_only_credit_check_id,
    s.credit_check_created_at AS serasa_income_only_credit_check_created_at,
    s.credit_check_match_stage AS serasa_income_only_match_stage,
    s.credit_check_minutes_from_cs AS serasa_income_only_minutes_from_cs,
    s.credit_check_match_leniency_rank AS serasa_income_only_match_leniency_rank,

    /* campos brutos (para auditoria e debug) */
    TRY_TO_NUMBER(
      COALESCE(
        s.credit_check_data:score::string,
        s.credit_check_data:data:score::string
      )
    ) AS serasa_income_only_raw_value,

    COALESCE(s.credit_check_data:range::string, s.credit_check_data:data:range::string) AS serasa_income_only_range,
    COALESCE(s.credit_check_data:scoreModel::string, s.credit_check_data:data:scoreModel::string) AS serasa_income_only_model,

    /* semântica inferida (anti-cegueira): HRP* tem forte evidência de ser "renda estimada" */
    CASE
      WHEN COALESCE(s.credit_check_data:scoreModel::string, s.credit_check_data:data:scoreModel::string) ILIKE 'HRP%' THEN 'income_cents'
      ELSE 'unknown'
    END AS serasa_income_only_semantic,

    /* renda estimada (reais): somente quando a semântica indicar income_cents e o valor for plausível */
    IFF(
      COALESCE(s.credit_check_data:scoreModel::string, s.credit_check_data:data:scoreModel::string) ILIKE 'HRP%'
      AND TRY_TO_NUMBER(COALESCE(s.credit_check_data:score::string, s.credit_check_data:data:score::string)) IS NOT NULL
      AND TRY_TO_NUMBER(COALESCE(s.credit_check_data:score::string, s.credit_check_data:data:score::string)) > 0
      /* sanidade: renda mensal em R$ raramente > 1.000.000; como está em centavos, limite ~100.000.000 */
      AND TRY_TO_NUMBER(COALESCE(s.credit_check_data:score::string, s.credit_check_data:data:score::string)) <= 100000000,
      (TRY_TO_NUMBER(COALESCE(s.credit_check_data:score::string, s.credit_check_data:data:score::string)) / 100.0)::FLOAT,
      NULL
    ) AS serasa_income_estimated
  FROM cc_best_serasa_new_income_only_1 s
),

/* ============================
   SERASA new (strict ±1h, score_without_income): registration dedicada para eixo cadastro
   Motivo: evitar que o “best SERASA new” (por simulation) seja income_only e o eixo de cadastro perca registration/negativeData.
   ============================ */
serasa_new_registration_strict AS (
  WITH strict_serasa_candidates AS (
    SELECT
      m.credit_simulation_id,
      m.credit_check_id,
      m.credit_check_created_at,
      m.credit_check_match_stage,
      m.credit_check_minutes_from_cs,
      m.credit_check_match_leniency_rank,
      m.credit_check_data,
      ROW_NUMBER() OVER (
        PARTITION BY m.credit_simulation_id
        ORDER BY
          ABS(m.credit_check_minutes_from_cs) ASC,
          m.credit_check_created_at DESC,
          m.credit_check_id DESC
      ) AS rn_best_strict
    FROM cc_matches_strict_1h m
    WHERE m.source = 'serasa'
      AND (
        COALESCE(m.new_data_format, FALSE) = TRUE
        OR (TYPEOF(m.credit_check_data) = 'OBJECT' AND m.credit_check_data:reports IS NOT NULL)
      )
      AND m.kind = 'check_score_without_income'
  ),
  strict_best AS (
    SELECT *
    FROM strict_serasa_candidates
    WHERE rn_best_strict = 1
  ),
  reports AS (
    SELECT
      s.credit_simulation_id,
      s.credit_check_id,
      s.credit_check_created_at,
      s.credit_check_match_stage,
      s.credit_check_minutes_from_cs,
      s.credit_check_match_leniency_rank,
      r.value AS report,
      r.index AS report_index
    FROM strict_best s
    , LATERAL FLATTEN(input => s.credit_check_data:reports) r
  ),
  best_report AS (
    SELECT
      r.*,
      ROW_NUMBER() OVER (
        PARTITION BY r.credit_simulation_id
        ORDER BY
          IFF(r.report:reportName::string = 'COMBO_CONCESSAO', 0, 1),
          r.report_index ASC
      ) AS rn_report
    FROM reports r
  )
  SELECT
    credit_simulation_id,
    credit_check_id AS serasa_reg_strict_credit_check_id,
    credit_check_created_at AS serasa_reg_strict_credit_check_created_at,
    credit_check_match_stage AS serasa_reg_strict_match_stage,
    credit_check_minutes_from_cs AS serasa_reg_strict_minutes_from_cs,
    credit_check_match_leniency_rank AS serasa_reg_strict_match_leniency_rank,
    report:reportName::string AS serasa_reg_strict_report_name,
    TRY_TO_DATE(report:registration:birthDate::string) AS serasa_reg_strict_birthdate,
    report:registration:consumerGender::string         AS serasa_reg_strict_gender,
    report:registration:address:zipCode::string        AS serasa_reg_strict_zipcode,
    report:registration:statusRegistration::string     AS serasa_reg_strict_status_registration,
    TRY_TO_DATE(report:registration:statusDate::string) AS serasa_reg_strict_status_date,
    IFF(report:registration:phone IS NOT NULL, 1, 0) AS serasa_reg_strict_has_phone,
    IFF(report:registration:address IS NOT NULL, 1, 0) AS serasa_reg_strict_has_address
  FROM best_report
  WHERE rn_report = 1
),

/* ============================
   SERASA antigo: B-codes em data (ARRAY) -> extrair cadastro/score/negativação (sumários)
   ============================ */
serasa_old_features_raw AS (
  SELECT
    s.credit_simulation_id,

    /* linhagem do check */
    MAX(s.credit_check_id)         AS serasa_old_credit_check_id,
    MAX(s.credit_check_created_at) AS serasa_old_credit_check_created_at,
    MAX(s.credit_check_match_stage) AS serasa_old_match_stage,
    MAX(s.credit_check_minutes_from_cs)   AS serasa_old_minutes_from_cs,
    MAX(s.credit_check_match_leniency_rank) AS serasa_old_match_leniency_rank,

    /* cadastro/demografia: agregamos por B-code para garantir 1 linha por simulation */
    MAX(TRY_TO_DATE(f.value:"B002":birth_date::string)) AS b002_birth_date,
    MAX(TRY_TO_DATE(f.value:"B001":birthdate::string))  AS b001_birthdate,
    MAX(TRY_TO_DATE(f.value:"B001":birth_date::string)) AS b001_birth_date,

    MAX(f.value:"B002":gender::string) AS b002_gender,
    MAX(f.value:"B001":gender::string) AS b001_gender,

    MAX(f.value:"B004":zip_code::string) AS b004_zip_code,
    MAX(f.value:"B004":zipcode::string)  AS b004_zipcode,
    MAX(f.value:"B004":cep::string)      AS b004_cep,

    MAX(IFF(f.value:"B003" IS NOT NULL, 1, 0)) AS serasa_old_has_phone,
    MAX(IFF(f.value:"B004" IS NOT NULL, 1, 0)) AS serasa_old_has_address,

    /* score */
    MAX(NULLIF(TRY_TO_NUMBER(f.value:"B280":score::string), 0)) AS serasa_old_score_b280,
    MAX(f.value:"B280":score_range_name::string)     AS serasa_old_score_range_name,
    MAX(TRY_TO_NUMBER(f.value:"B280":delinquency_probability_percent::string)) AS serasa_old_delinquency_prob_pct,

    /* negativação (sumários) */
    MAX(TRY_TO_NUMBER(f.value:"B357":occurrences_count::string)) AS serasa_old_b357_occurrences_count,
    MAX(TRY_TO_NUMBER(f.value:"B357":total_occurrence_value::string)) AS serasa_old_b357_total_value,
    MAX(TRY_TO_NUMBER(f.value:"B361":occurrences_count::string)) AS serasa_old_b361_occurrences_count,
    MAX(TRY_TO_NUMBER(f.value:"B361":total_occurrence_value::string)) AS serasa_old_b361_total_value,

    'credit_checks_serasa_old' AS serasa_old_source
  FROM cc_best_serasa_old_1 s
  , LATERAL FLATTEN(input => s.credit_check_data) f
  GROUP BY 1
),

serasa_old_features AS (
  SELECT
    credit_simulation_id,
    serasa_old_credit_check_id,
    serasa_old_credit_check_created_at,
    serasa_old_match_stage,
    serasa_old_minutes_from_cs,
    serasa_old_match_leniency_rank,

    COALESCE(b002_birth_date, b001_birthdate, b001_birth_date) AS serasa_old_birthdate,
    COALESCE(b002_gender, b001_gender) AS serasa_old_gender,
    COALESCE(b004_zip_code, b004_zipcode, b004_cep) AS serasa_old_zipcode,

    serasa_old_has_phone,
    serasa_old_has_address,

    serasa_old_score_b280,
    serasa_old_score_range_name,
    serasa_old_delinquency_prob_pct,

    serasa_old_b357_occurrences_count,
    (serasa_old_b357_total_value)::FLOAT AS serasa_old_b357_total_value, /* Observado (amostral): valores vêm como inteiros em R$ (ex.: "000000116" => 116) */
    serasa_old_b361_occurrences_count,
    (serasa_old_b361_total_value)::FLOAT AS serasa_old_b361_total_value,

    serasa_old_source
  FROM serasa_old_features_raw
),

/* ============================
   BOA VISTA SCPC (scpc_net): extração de negativação (bloco 141) e cadastro (249)
   ============================ */
boa_vista_scpc_net_features_raw AS (
  SELECT
    s.credit_simulation_id,
    MAX(s.credit_check_id)         AS bvs_net_credit_check_id,
    MAX(s.credit_check_created_at) AS bvs_net_credit_check_created_at,
    MAX(s.credit_check_match_stage) AS bvs_net_match_stage,
    MAX(s.credit_check_minutes_from_cs)   AS bvs_net_minutes_from_cs,
    MAX(s.credit_check_match_leniency_rank) AS bvs_net_match_leniency_rank,

    /* Bloco 249: Cadastro */
    /* formato data BVS: DDMMYYYY */
    MAX(TRY_TO_DATE(f.value:"249":birthdate::string, 'DDMMYYYY')) AS bvs_net_birthdate,
    MAX(f.value:"249":name::string) AS bvs_net_name,
    MAX(f.value:"249":status::string) AS bvs_net_status,

    /* Bloco 141: Débitos */
    MAX(TRY_TO_NUMBER(f.value:"141":debit_total_count::string)) AS bvs_net_debit_count,
    /* valor vem sem pontuação (ex: 0000000299113) mas geralmente é centavos? Na inspeção visual:
       299113 => 2991.13? ou 299.113?
       No exemplo: "0000000299113" com "currency_type": "R$"
       Vamos assumir centavos por padrão bancário, mas confirmar se possível.
       Se for R$ 299,11 então é divide por 100.
       Se for R$ 2991,13 então é divide por 100.
       Se for 2 dívidas totalizando isso, parece alto.
       OBS: O exemplo do usuário diz "2 dívidas (total R$ 299,00)". Se o raw é 299113, então não bate fácil.
       Espera: 299113 / 100 = 2991.13.
       Talvez o exemplo visual que vi tenha sido mal interpretado.
       Vou assumir divide por 100 (centavos). */
    MAX(TRY_TO_NUMBER(f.value:"141":debit_total_value::string)) AS bvs_net_debit_value_raw,

    /* Bloco 123: flag simples de existência (S/N) */
    MAX(NULLIF(TRIM(f.value:"123":exists::string), '')) AS bvs_net_exists_123_raw,

    /* Recência (quando disponível) */
    MAX(TRY_TO_DATE(NULLIF(TRIM(f.value:"141":last_debit_date::string), ''))) AS bvs_net_last_debit_date,

    'credit_checks_boa_vista_scpc_net' AS bvs_net_source
  FROM cc_best_boa_vista_scpc_net s
  , LATERAL FLATTEN(input => s.credit_check_data) f
  GROUP BY 1
),

boa_vista_scpc_net_features AS (
  SELECT
    credit_simulation_id,
    bvs_net_credit_check_id,
    bvs_net_credit_check_created_at,
    bvs_net_match_stage,
    bvs_net_minutes_from_cs,
    bvs_net_match_leniency_rank,
    bvs_net_birthdate,
    bvs_net_status,
    bvs_net_debit_count,
    (bvs_net_debit_value_raw / 100.0)::FLOAT AS bvs_net_debit_value,
    bvs_net_exists_123_raw,
    CASE
      WHEN bvs_net_exists_123_raw = 'S' THEN TRUE
      WHEN bvs_net_exists_123_raw = 'N' THEN FALSE
      ELSE NULL
    END AS bvs_net_exists_123,
    bvs_net_last_debit_date,
    bvs_net_source
  FROM boa_vista_scpc_net_features_raw
),

/* ============================
   SCR: exposição/endereçamento (core mínimo)
   Nota: evitamos “explodir” colunas; extraímos contagens e um somatório RAW (unidade a confirmar por auditoria).
   ============================ */
scr_matches_5y AS (
  /* SCR como fallback de alta leniência: lookback 5 anos.
     Motivo: em amostras, SCR não aparece no entorno (±1h/15d/180d) das simulations. */
  SELECT
    cs.credit_simulation_id,
    cc.id AS scr_credit_check_id,
    cc.created_at AS scr_credit_check_created_at,
    cc.data AS scr_credit_check_data,
    'scr_fallback_5y' AS scr_match_stage,
    4 AS scr_match_leniency_rank,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS scr_minutes_from_cs
  FROM cs_enriched cs
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.source = 'scr'
   AND cc.created_at BETWEEN DATEADD('day', -1825, cs.cs_created_at) AND cs.cs_created_at
),

cc_best_scr AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id
      ORDER BY ABS(m.scr_minutes_from_cs) ASC, m.scr_credit_check_created_at DESC, m.scr_credit_check_id DESC
    ) AS rn_best
  FROM scr_matches_5y m
),

cc_best_scr_1 AS (
  SELECT *
  FROM cc_best_scr
  WHERE rn_best = 1
),

scr_ops AS (
  SELECT
    s.credit_simulation_id,
    o.value AS op
  FROM cc_best_scr_1 s
  , LATERAL FLATTEN(
      input => COALESCE(
        /* nested (camelCase) */
        s.scr_credit_check_data:resumoDoCliente:listaDeResumoDasOperacoes,
        s.scr_credit_check_data:resumoDoCliente:ListaDeResumoDasOperacoes,
        s.scr_credit_check_data:ResumoDoCliente:listaDeResumoDasOperacoes,
        s.scr_credit_check_data:ResumoDoCliente:ListaDeResumoDasOperacoes,
        /* top-level fallbacks */
        s.scr_credit_check_data:listaDeResumoDasOperacoes,
        s.scr_credit_check_data:ListaDeResumoDasOperacoes
      )
    ) o
),

scr_vencimentos AS (
  SELECT
    so.credit_simulation_id,
    v.value AS venc
  FROM scr_ops so
  , LATERAL FLATTEN(input => COALESCE(so.op:listaDeVencimentos, so.op:ListaDeVencimentos)) v
),

scr_features AS (
  SELECT
    s.credit_simulation_id,
    s.scr_credit_check_id,
    s.scr_credit_check_created_at,
    s.scr_match_stage,
    s.scr_minutes_from_cs,
    s.scr_match_leniency_rank,
    IFF(
      s.scr_credit_check_data:resumoDoCliente IS NOT NULL
      OR s.scr_credit_check_data:ResumoDoCliente IS NOT NULL,
      TRUE, FALSE
    ) AS scr_has_resumo_do_cliente,
    /* contagens (unitless, estáveis) */
    COALESCE(opc.n_ops, 0) AS scr_operations_count,
    COALESCE(vc.n_vencimentos, 0) AS scr_vencimentos_count,
    /* somatório raw (unidade/escala a confirmar; não usamos em score canônico) */
    vc.sum_valor_raw AS scr_sum_valor_raw,
    TRY_TO_DATE(
      COALESCE(
        s.scr_credit_check_data:resumoDoCliente:dataBaseConsultada::string,
        s.scr_credit_check_data:ResumoDoCliente:DataBaseConsultada::string,
        s.scr_credit_check_data:resumoDoCliente:dataBase::string,
        s.scr_credit_check_data:ResumoDoCliente:DataBase::string
      )
    ) AS scr_data_base_consultada,
    'credit_checks_scr' AS scr_source
  FROM cc_best_scr_1 s
  LEFT JOIN (
    SELECT credit_simulation_id, COUNT(*) AS n_ops
    FROM scr_ops
    GROUP BY 1
  ) opc
    ON opc.credit_simulation_id = s.credit_simulation_id
  LEFT JOIN (
    SELECT
      credit_simulation_id,
      COUNT(*) AS n_vencimentos,
      SUM(
        TRY_TO_NUMBER(
          COALESCE(
            venc:valorVencimento::string,
            venc:ValorVencimento::string
          )
        )
      )::FLOAT AS sum_valor_raw
    FROM scr_vencimentos
    GROUP BY 1
  ) vc
    ON vc.credit_simulation_id = s.credit_simulation_id
),

/* ============================
   SERASA: linhagem enxuta (evitar explosão de colunas)
   ============================ */
serasa_lineage AS (
  SELECT
    cs.credit_simulation_id,
    CASE
      WHEN sn.serasa_new_credit_check_id IS NOT NULL OR si.serasa_income_only_credit_check_id IS NOT NULL THEN 'new'
      WHEN so.serasa_old_credit_check_id IS NOT NULL THEN 'old'
      ELSE NULL
    END AS serasa_format,
    /* semântica: score SERASA vem prioritariamente dos credit checks SERASA;
       Crivo só entra como fallback quando não há score vindo do SERASA */
    ROUND(COALESCE(sn.serasa_new_score, so.serasa_old_score_b280, cf.crivo_score_serasa), 0)::NUMBER AS serasa_score,
    CASE
      WHEN sn.serasa_new_score IS NOT NULL THEN 'serasa_new_score_without_income'
      WHEN so.serasa_old_score_b280 IS NOT NULL THEN 'serasa_old_b280'
      WHEN cf.crivo_score_serasa IS NOT NULL THEN 'crivo_bureau_campos'
      ELSE NULL
    END AS serasa_score_source
  FROM cs_enriched cs
  LEFT JOIN crivo_features_campos cf
    ON cf.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_new_features sn
    ON sn.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_new_income_only_features si
    ON si.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_old_features so
    ON so.credit_simulation_id = cs.credit_simulation_id
),

/* ============================
   BACEN internal score (proxy + score)
   ============================ */
bacen_internal_features AS (
  SELECT
    b.credit_simulation_id,
    b.credit_check_id AS bacen_credit_check_id,
    b.credit_check_created_at AS bacen_credit_check_created_at,
    b.credit_check_match_stage AS bacen_match_stage,
    b.credit_check_minutes_from_cs AS bacen_minutes_from_cs,
    TRY_TO_NUMBER(b.credit_check_data:predictions[0]:score::string) AS bacen_internal_score,
    /* Proxies adicionais (alta cobertura) vindos do motor interno Bacen */
    TRY_TO_NUMBER(b.credit_check_data:predictions[0]:limitesdecredito::string) AS bacen_credit_limits_total,
    TRY_TO_NUMBER(b.credit_check_data:predictions[0]:valorvencimento_mean_credit_limits::string) AS bacen_mean_due_value_credit_limits,
    CASE
      WHEN b.credit_check_data:predictions[0]:is_not_banked IS NULL THEN NULL
      WHEN LOWER(b.credit_check_data:predictions[0]:is_not_banked::string) IN ('true','1') THEN TRUE
      WHEN LOWER(b.credit_check_data:predictions[0]:is_not_banked::string) IN ('false','0') THEN FALSE
      ELSE NULL
    END AS bacen_is_not_banked,
    'credit_checks_bacen_internal_score' AS bacen_source
  FROM cc_best_bacen_internal_score b
),

/* ============================
   Boa Vista score PF (score)
   ============================ */
boa_vista_score_pf_features AS (
  SELECT
    b.credit_simulation_id,
    b.credit_check_id AS bvs_credit_check_id,
    b.credit_check_created_at AS bvs_credit_check_created_at,
    b.credit_check_match_stage AS bvs_match_stage,
    b.credit_check_minutes_from_cs AS bvs_minutes_from_cs,
    /* path observado no doc (pode variar; esta é a leitura mais simples) */
    TRY_TO_NUMBER(b.credit_check_data:score_positivo:score_classificacao_varios_modelos:score::string) AS bvs_score,
    'credit_checks_boa_vista_score_pf' AS bvs_source
  FROM cc_best_boa_vista_score_pf b
),

/* ============================
   Camada de decisão por eixo (prioridade de fontes)
   ============================ */
cad_candidates AS (
  /* SERASA new (registration) */
  SELECT
    cs.credit_simulation_id,
    'serasa_new_registration' AS cadastro_evidence_source,
    sn.serasa_new_match_stage AS cadastro_evidence_match_stage,
    sn.serasa_new_minutes_from_cs AS cadastro_evidence_minutes_from_cs,
    sn.serasa_new_match_leniency_rank AS cadastro_evidence_rank,
    (IFF(sn.serasa_new_birthdate IS NOT NULL, 1, 0)
     + IFF(NULLIF(TRIM(sn.serasa_new_gender),'') IS NOT NULL, 1, 0)
     + IFF(NULLIF(TRIM(sn.serasa_new_zipcode),'') IS NOT NULL, 1, 0)) AS cadastro_completeness
  FROM cs_enriched cs
  JOIN serasa_new_features sn
    ON sn.credit_simulation_id = cs.credit_simulation_id
  WHERE sn.serasa_new_credit_check_id IS NOT NULL
    AND (sn.serasa_new_birthdate IS NOT NULL OR NULLIF(TRIM(sn.serasa_new_gender),'') IS NOT NULL OR NULLIF(TRIM(sn.serasa_new_zipcode),'') IS NOT NULL)

  UNION ALL

  /* SERASA new (registration) — versão estrita dedicada (±1h) */
  SELECT
    cs.credit_simulation_id,
    'serasa_new_registration' AS cadastro_evidence_source,
    r.serasa_reg_strict_match_stage AS cadastro_evidence_match_stage,
    r.serasa_reg_strict_minutes_from_cs AS cadastro_evidence_minutes_from_cs,
    r.serasa_reg_strict_match_leniency_rank AS cadastro_evidence_rank,
    (IFF(r.serasa_reg_strict_birthdate IS NOT NULL, 1, 0)
     + IFF(NULLIF(TRIM(r.serasa_reg_strict_gender),'') IS NOT NULL, 1, 0)
     + IFF(NULLIF(TRIM(r.serasa_reg_strict_zipcode),'') IS NOT NULL, 1, 0)) AS cadastro_completeness
  FROM cs_enriched cs
  JOIN serasa_new_registration_strict r
    ON r.credit_simulation_id = cs.credit_simulation_id
  WHERE r.serasa_reg_strict_birthdate IS NOT NULL
     OR NULLIF(TRIM(r.serasa_reg_strict_gender),'') IS NOT NULL
     OR NULLIF(TRIM(r.serasa_reg_strict_zipcode),'') IS NOT NULL

  UNION ALL

  /* SERASA old (B-codes) */
  SELECT
    cs.credit_simulation_id,
    'serasa_old_bcodes' AS cadastro_evidence_source,
    so.serasa_old_match_stage AS cadastro_evidence_match_stage,
    so.serasa_old_minutes_from_cs AS cadastro_evidence_minutes_from_cs,
    so.serasa_old_match_leniency_rank AS cadastro_evidence_rank,
    (IFF(so.serasa_old_birthdate IS NOT NULL, 1, 0)
     + IFF(NULLIF(TRIM(so.serasa_old_gender),'') IS NOT NULL, 1, 0)
     + IFF(NULLIF(TRIM(so.serasa_old_zipcode),'') IS NOT NULL, 1, 0)) AS cadastro_completeness
  FROM cs_enriched cs
  JOIN serasa_old_features so
    ON so.credit_simulation_id = cs.credit_simulation_id
  WHERE so.serasa_old_credit_check_id IS NOT NULL
    AND (so.serasa_old_birthdate IS NOT NULL OR NULLIF(TRIM(so.serasa_old_gender),'') IS NOT NULL OR NULLIF(TRIM(so.serasa_old_zipcode),'') IS NOT NULL)

  UNION ALL

  /* Boa Vista SCPC (novo candidato!) */
  SELECT
    cs.credit_simulation_id,
    'boa_vista_scpc_net' AS cadastro_evidence_source,
    bvs.bvs_net_match_stage AS cadastro_evidence_match_stage,
    bvs.bvs_net_minutes_from_cs AS cadastro_evidence_minutes_from_cs,
    bvs.bvs_net_match_leniency_rank AS cadastro_evidence_rank,
    IFF(bvs.bvs_net_birthdate IS NOT NULL, 1, 0) AS cadastro_completeness
  FROM cs_enriched cs
  JOIN boa_vista_scpc_net_features bvs
    ON bvs.credit_simulation_id = cs.credit_simulation_id
  WHERE bvs.bvs_net_birthdate IS NOT NULL

  UNION ALL

  /* Crivo (campos) — útil principalmente para CEP/telefone quando SERASA não existir */
  SELECT
    cs.credit_simulation_id,
    'crivo_bureau_campos' AS cadastro_evidence_source,
    cr.crivo_resolution_stage AS cadastro_evidence_match_stage,
    cr.crivo_minutes_from_cs AS cadastro_evidence_minutes_from_cs,
    IFF(cr.crivo_resolution_stage IN ('original','engineable'), 0,
      IFF(cr.crivo_resolution_stage = 'cpf_primary', 1,
        IFF(cr.crivo_resolution_stage = 'cpf_fallback_15d', 2,
          IFF(cr.crivo_resolution_stage = 'cpf_fallback_180d', 3, 99)
        )
      )
    ) AS cadastro_evidence_rank,
    (IFF(cf.crivo_zipcode IS NOT NULL, 1, 0)
     + IFF(
         cf.crivo_birthdate_bvs IS NOT NULL
         AND DATEDIFF('year', cf.crivo_birthdate_bvs, cs.cs_created_at::DATE) BETWEEN 12 AND 120
         AND cf.crivo_birthdate_bvs <= cs.cs_created_at::DATE,
         1, 0
       )
     + IFF(cg.crivo_gender IS NOT NULL, 1, 0)
    ) AS cadastro_completeness
  FROM cs_enriched cs
  JOIN crivo_resolution cr
    ON cr.credit_simulation_id = cs.credit_simulation_id
  JOIN crivo_features_campos cf
    ON cf.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN crivo_features_databusca_pf cg
    ON cg.credit_simulation_id = cs.credit_simulation_id
  WHERE cr.crivo_check_id_resolved IS NOT NULL
    AND (
      cf.crivo_zipcode IS NOT NULL
      OR cf.crivo_has_phone = 1
      OR cg.crivo_gender IS NOT NULL
      OR (
        cf.crivo_birthdate_bvs IS NOT NULL
        AND DATEDIFF('year', cf.crivo_birthdate_bvs, cs.cs_created_at::DATE) BETWEEN 12 AND 120
        AND cf.crivo_birthdate_bvs <= cs.cs_created_at::DATE
      )
    )

  UNION ALL

  /* Sensitive last resort */
  SELECT
    cs.credit_simulation_id,
    'sensitive_last_resort' AS cadastro_evidence_source,
    NULL AS cadastro_evidence_match_stage,
    NULL AS cadastro_evidence_minutes_from_cs,
    99 AS cadastro_evidence_rank,
    (IFF(bs.birthdate IS NOT NULL, 1, 0) + IFF(bs.zipcode IS NOT NULL, 1, 0)) AS cadastro_completeness
  FROM cs_enriched cs
  JOIN borrower_sensitive bs
    ON bs.borrower_person_id = cs.borrower_person_id
  WHERE bs.birthdate IS NOT NULL OR bs.zipcode IS NOT NULL
),

cad_best AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.credit_simulation_id
      ORDER BY
        c.cadastro_evidence_rank ASC,
        IFF(c.cadastro_evidence_minutes_from_cs IS NULL, 1, 0) ASC,
        ABS(c.cadastro_evidence_minutes_from_cs) ASC,
        c.cadastro_completeness DESC
    ) AS rn_best
  FROM cad_candidates c
),

axis_cadastro AS (
  SELECT
    cs.credit_simulation_id,

    /* birthdate */
    TO_DATE(COALESCE(
      r.serasa_reg_strict_birthdate,
      sn.serasa_new_birthdate,
      so.serasa_old_birthdate,
      bvs.bvs_net_birthdate,
      IFF(
        cf.crivo_birthdate_bvs IS NOT NULL
        AND DATEDIFF('year', cf.crivo_birthdate_bvs, cs.cs_created_at::DATE) BETWEEN 12 AND 120
        AND cf.crivo_birthdate_bvs <= cs.cs_created_at::DATE,
        cf.crivo_birthdate_bvs,
        NULL
      ),
      bs.birthdate
    )) AS borrower_birthdate,
    CASE
      WHEN r.serasa_reg_strict_birthdate IS NOT NULL THEN 'serasa_new_registration'
      WHEN sn.serasa_new_birthdate IS NOT NULL THEN 'serasa_new_registration'
      WHEN so.serasa_old_birthdate IS NOT NULL THEN 'serasa_old_bcodes'
      WHEN bvs.bvs_net_birthdate IS NOT NULL THEN 'boa_vista_scpc_net'
      WHEN cf.crivo_birthdate_bvs IS NOT NULL
       AND DATEDIFF('year', cf.crivo_birthdate_bvs, cs.cs_created_at::DATE) BETWEEN 12 AND 120
       AND cf.crivo_birthdate_bvs <= cs.cs_created_at::DATE
        THEN 'crivo_bureau_campos_birthdate_bvs'
      WHEN bs.birthdate IS NOT NULL THEN 'sensitive_last_resort'
      ELSE NULL
    END AS borrower_birthdate_source,

    /* gender: serasa novo/antigo, com fallback Crivo (DataBusca PF) quando SERASA não tiver
       Observação: SERASA pode vir com string vazia; tratamos como NULL. */
    COALESCE(
      NULLIF(TRIM(r.serasa_reg_strict_gender), ''),
      NULLIF(TRIM(sn.serasa_new_gender), ''),
      NULLIF(TRIM(so.serasa_old_gender), ''),
      cg.crivo_gender
    ) AS borrower_gender,
    CASE
      WHEN NULLIF(TRIM(r.serasa_reg_strict_gender), '') IS NOT NULL THEN 'serasa_new_registration'
      WHEN NULLIF(TRIM(sn.serasa_new_gender), '') IS NOT NULL THEN 'serasa_new_registration'
      WHEN NULLIF(TRIM(so.serasa_old_gender), '') IS NOT NULL THEN 'serasa_old_bcodes'
      WHEN cg.crivo_gender IS NOT NULL THEN 'crivo_databusca_pf_sexo_codigo'
      ELSE NULL
    END AS borrower_gender_source,

    /* zipcode: serasa novo -> serasa antigo -> crivo -> sensitive */
    COALESCE(r.serasa_reg_strict_zipcode, sn.serasa_new_zipcode, so.serasa_old_zipcode, cf.crivo_zipcode, bs.zipcode) AS borrower_zipcode,
    CASE
      WHEN r.serasa_reg_strict_zipcode IS NOT NULL THEN 'serasa_new_registration'
      WHEN sn.serasa_new_zipcode IS NOT NULL THEN 'serasa_new_registration'
      WHEN so.serasa_old_zipcode IS NOT NULL THEN 'serasa_old_bcodes'
      WHEN cf.crivo_zipcode IS NOT NULL THEN 'crivo_bureau_campos'
      WHEN bs.zipcode IS NOT NULL THEN 'sensitive_last_resort'
      ELSE NULL
    END AS borrower_zipcode_source,

    /* city/state (sensitive) — útil para feature engineering */
    bs.city AS borrower_city,
    bs.state AS borrower_state,

    /* status cadastral (evitar PII): preferir SERASA new; fallback BVS */
    COALESCE(r.serasa_reg_strict_status_registration, sn.serasa_new_status_registration, bvs.bvs_net_status) AS borrower_registration_status,
    CASE
      WHEN r.serasa_reg_strict_status_registration IS NOT NULL THEN 'serasa_new_registration'
      WHEN sn.serasa_new_status_registration IS NOT NULL THEN 'serasa_new_registration'
      WHEN bvs.bvs_net_status IS NOT NULL THEN 'boa_vista_scpc_net'
      ELSE NULL
    END AS borrower_registration_status_source,
    COALESCE(r.serasa_reg_strict_status_date, sn.serasa_new_status_date) AS borrower_registration_status_date,

    /* flags de qualidade do cadastro (sem PII): tem telefone? tem endereço? */
    IFF(
      COALESCE(
        r.serasa_reg_strict_has_phone,
        sn.serasa_new_has_phone,
        so.serasa_old_has_phone,
        cf.crivo_has_phone
      ) = 1,
      TRUE, FALSE
    ) AS borrower_has_phone,
    CASE
      WHEN r.serasa_reg_strict_has_phone = 1 THEN 'serasa_new_registration'
      WHEN sn.serasa_new_has_phone = 1 THEN 'serasa_new_registration'
      WHEN so.serasa_old_has_phone = 1 THEN 'serasa_old_bcodes'
      WHEN cf.crivo_has_phone = 1 THEN 'crivo_bureau_campos'
      ELSE NULL
    END AS borrower_has_phone_source,

    IFF(
      COALESCE(
        r.serasa_reg_strict_has_address,
        sn.serasa_new_has_address,
        so.serasa_old_has_address
      ) = 1,
      TRUE, FALSE
    ) AS borrower_has_address,
    CASE
      WHEN r.serasa_reg_strict_has_address = 1 THEN 'serasa_new_registration'
      WHEN sn.serasa_new_has_address = 1 THEN 'serasa_new_registration'
      WHEN so.serasa_old_has_address = 1 THEN 'serasa_old_bcodes'
      ELSE NULL
    END AS borrower_has_address_source,

    /* evidência do eixo (para auditoria: estrito → proximidade → completude) */
    cb.cadastro_evidence_source,
    cb.cadastro_evidence_match_stage,
    cb.cadastro_evidence_minutes_from_cs
  FROM cs_enriched cs
  LEFT JOIN serasa_new_features sn
    ON sn.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_old_features so
    ON so.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN boa_vista_scpc_net_features bvs
    ON bvs.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN crivo_features_campos cf
    ON cf.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN crivo_features_databusca_pf cg
    ON cg.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN borrower_sensitive bs
    ON bs.borrower_person_id = cs.borrower_person_id
  LEFT JOIN serasa_new_registration_strict r
    ON r.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN cad_best cb
    ON cb.credit_simulation_id = cs.credit_simulation_id
   AND cb.rn_best = 1
),

/* ============================
   EIXO 2 (negativação) — seleção dinâmica (estrito primeiro)
   Regra: escolher a melhor evidência entre SERASA new / SERASA old / Crivo,
   priorizando match mais estrito, depois proximidade temporal, depois completude.
   ============================ */
neg_candidates AS (
  /* SERASA new (summary) */
  SELECT
    cs.credit_simulation_id,
    'serasa_new_negativeData_summary' AS negativacao_source,
    sn.serasa_new_match_stage AS negativacao_evidence_match_stage,
    sn.serasa_new_minutes_from_cs AS negativacao_evidence_minutes_from_cs,
    sn.serasa_new_match_leniency_rank AS negativacao_evidence_rank,
    sn.serasa_new_pefin_count  AS pefin_count,
    sn.serasa_new_refin_count  AS refin_count,
    sn.serasa_new_notary_count AS protesto_count,
    /* unificação de valores */
    (sn.serasa_new_pefin_balance)::FLOAT  AS pefin_value,
    (sn.serasa_new_refin_balance)::FLOAT  AS refin_value,
    (sn.serasa_new_notary_balance)::FLOAT AS protesto_value,
    (IFF(sn.serasa_new_pefin_count IS NOT NULL, 1, 0)
     + IFF(sn.serasa_new_refin_count IS NOT NULL, 1, 0)
     + IFF(sn.serasa_new_notary_count IS NOT NULL, 1, 0)) AS negativacao_completeness
  FROM cs_enriched cs
  JOIN serasa_new_features sn
    ON sn.credit_simulation_id = cs.credit_simulation_id
  WHERE sn.serasa_new_credit_check_id IS NOT NULL
    AND (sn.serasa_new_pefin_count IS NOT NULL OR sn.serasa_new_refin_count IS NOT NULL OR sn.serasa_new_notary_count IS NOT NULL)

  UNION ALL

  /* SERASA old (B-codes summary) */
  SELECT
    cs.credit_simulation_id,
    'serasa_old_bcodes_summary' AS negativacao_source,
    so.serasa_old_match_stage AS negativacao_evidence_match_stage,
    so.serasa_old_minutes_from_cs AS negativacao_evidence_minutes_from_cs,
    so.serasa_old_match_leniency_rank AS negativacao_evidence_rank,
    NULL AS pefin_count,
    so.serasa_old_b357_occurrences_count AS refin_count,
    so.serasa_old_b361_occurrences_count AS protesto_count,
    /* unificação de valores */
    NULL AS pefin_value,
    so.serasa_old_b357_total_value AS refin_value,     /* B357 costuma misturar PEFIN/REFIN, mapeado aqui como REFIN por convenção */
    so.serasa_old_b361_total_value AS protesto_value,
    (IFF(so.serasa_old_b357_occurrences_count IS NOT NULL, 1, 0)
     + IFF(so.serasa_old_b361_occurrences_count IS NOT NULL, 1, 0)) AS negativacao_completeness
  FROM cs_enriched cs
  JOIN serasa_old_features so
    ON so.credit_simulation_id = cs.credit_simulation_id
  WHERE so.serasa_old_credit_check_id IS NOT NULL
    AND (so.serasa_old_b357_occurrences_count IS NOT NULL OR so.serasa_old_b361_occurrences_count IS NOT NULL)

  UNION ALL

  /* Boa Vista SCPC (novo candidato!) */
  SELECT
    cs.credit_simulation_id,
    'boa_vista_scpc_net' AS negativacao_source,
    bvs.bvs_net_match_stage AS negativacao_evidence_match_stage,
    bvs.bvs_net_minutes_from_cs AS negativacao_evidence_minutes_from_cs,
    bvs.bvs_net_match_leniency_rank AS negativacao_evidence_rank,
    /* BVS retorna "debit_count", que é similar a PEFIN. Vamos mapear para pefin_count. */
    bvs.bvs_net_debit_count AS pefin_count,
    NULL AS refin_count,
    NULL AS protesto_count,
    /* unificação de valores */
    bvs.bvs_net_debit_value AS pefin_value, /* Mapeando debit_value para pefin_value (principal) */
    NULL AS refin_value,
    NULL AS protesto_value,
    (IFF(bvs.bvs_net_debit_count IS NOT NULL, 1, 0)) AS negativacao_completeness
  FROM cs_enriched cs
  JOIN boa_vista_scpc_net_features bvs
    ON bvs.credit_simulation_id = cs.credit_simulation_id
  WHERE bvs.bvs_net_debit_count IS NOT NULL

  UNION ALL

  /* Crivo (campos curados) */
  SELECT
    cs.credit_simulation_id,
    'crivo_bureau_campos' AS negativacao_source,
    cr.crivo_resolution_stage AS negativacao_evidence_match_stage,
    cr.crivo_minutes_from_cs AS negativacao_evidence_minutes_from_cs,
    IFF(cr.crivo_resolution_stage IN ('original','engineable'), 0,
      IFF(cr.crivo_resolution_stage = 'cpf_primary', 1,
        IFF(cr.crivo_resolution_stage = 'cpf_fallback_15d', 2,
          IFF(cr.crivo_resolution_stage = 'cpf_fallback_180d', 3, 99)
        )
      )
    ) AS negativacao_evidence_rank,
    cf.crivo_pefin_serasa AS pefin_count,
    cf.crivo_refin_serasa AS refin_count,
    cf.crivo_protesto_serasa AS protesto_count,
    /* unificação de valores: CRIVO NÃO POSSUI VALORES NESTA INTEGRAÇÃO (só counts) */
    NULL AS pefin_value,
    NULL AS refin_value,
    NULL AS protesto_value,
    (IFF(cf.crivo_pefin_serasa IS NOT NULL, 1, 0)
     + IFF(cf.crivo_refin_serasa IS NOT NULL, 1, 0)
     + IFF(cf.crivo_protesto_serasa IS NOT NULL, 1, 0)) AS negativacao_completeness
  FROM cs_enriched cs
  JOIN crivo_resolution cr
    ON cr.credit_simulation_id = cs.credit_simulation_id
  JOIN crivo_features_campos cf
    ON cf.credit_simulation_id = cs.credit_simulation_id
  WHERE cr.crivo_check_id_resolved IS NOT NULL
    AND (cf.crivo_pefin_serasa IS NOT NULL OR cf.crivo_refin_serasa IS NOT NULL OR cf.crivo_protesto_serasa IS NOT NULL)
),

neg_best AS (
  SELECT
    n.*,
    ROW_NUMBER() OVER (
      PARTITION BY n.credit_simulation_id
      ORDER BY
        n.negativacao_evidence_rank ASC,
        IFF(n.negativacao_evidence_minutes_from_cs IS NULL, 1, 0) ASC,
        ABS(n.negativacao_evidence_minutes_from_cs) ASC,
        n.negativacao_completeness DESC
    ) AS rn_best
  FROM neg_candidates n
),

axis_negativacao AS (
  SELECT
    cs.credit_simulation_id,

    nb.pefin_count,
    nb.refin_count,
    nb.protesto_count,

    nb.negativacao_source,
    nb.negativacao_evidence_match_stage,
    nb.negativacao_evidence_minutes_from_cs,

    /* valores monetários unificados */
    nb.pefin_value,
    nb.refin_value,
    nb.protesto_value,
    /* Semântica:
       - NULL => não há evidência monetária (ex.: Crivo só fornece counts/métricas)
       - 0    => evidência monetária existe e o somatório é 0 */
    IFF(
      nb.pefin_value IS NULL
      AND nb.refin_value IS NULL
      AND nb.protesto_value IS NULL,
      NULL,
      (COALESCE(nb.pefin_value, 0) + COALESCE(nb.refin_value, 0) + COALESCE(nb.protesto_value, 0))
    ) AS total_negative_value
  FROM cs_enriched cs
  LEFT JOIN neg_best nb
    ON nb.credit_simulation_id = cs.credit_simulation_id
   AND nb.rn_best = 1
),

axis_renda AS (
  SELECT
    cs.credit_simulation_id,

    /* proxies: prefer crivo key_parameters; senão bacen_internal; senão sensitive */
    cfk.crivo_bacen_score,
    cfk.crivo_credit_limits_value,
    cfk.crivo_overdue_portfolio_value,
    cfk.crivo_loss_value,
    bi.bacen_internal_score,
    si.serasa_income_estimated,
    /* renda presumida (Crivo) */
    COALESCE(cf.crivo_renda_presumida_credilink, cf.crivo_renda_presumida_serasa) AS crivo_renda_presumida,
    (bs.monthly_income)::FLOAT AS sensitive_monthly_income,
    /* SCR: sinais unitless + um somatório raw (não canonizado) */
    scr.scr_operations_count,
    scr.scr_vencimentos_count,
    scr.scr_sum_valor_raw,

    /* seleção dinâmica (estrito primeiro) do “melhor proxy”: Crivo vs Bacen vs Sensitive */
    CASE
      WHEN (
        cfk.crivo_credit_limits_value IS NOT NULL
        OR cfk.crivo_bacen_score IS NOT NULL
        OR cfk.crivo_overdue_portfolio_value IS NOT NULL
        OR cfk.crivo_loss_value IS NOT NULL
      )
       AND (
         /* Crivo é mais estrito (ou Bacen não existe) */
         IFF(cr.crivo_resolution_stage IN ('original','engineable'), 0,
           IFF(cr.crivo_resolution_stage = 'cpf_primary', 1,
             IFF(cr.crivo_resolution_stage = 'cpf_fallback_15d', 2,
               IFF(cr.crivo_resolution_stage = 'cpf_fallback_180d', 3, 99)
             )
           )
         )
         <=
    COALESCE(
           IFF(bi.bacen_match_stage = 'strict_primary_1h', 0,
             IFF(bi.bacen_match_stage = 'lenient_primary_24h', 1,
               IFF(bi.bacen_match_stage = 'lenient_fallback_15d', 2,
                 IFF(bi.bacen_match_stage = 'lenient_fallback_180d', 3, 99)
               )
             )
           ),
           99
         )
       )
        THEN 'crivo_key_parameters'
      WHEN bi.bacen_internal_score IS NOT NULL THEN 'bacen_internal_score'
      WHEN si.serasa_income_estimated IS NOT NULL THEN 'serasa_income_estimated'
      WHEN bs.monthly_income IS NOT NULL THEN 'sensitive_last_resort'
      ELSE NULL
    END AS renda_proxies_source,

    CASE
      WHEN (
        cfk.crivo_credit_limits_value IS NOT NULL
        OR cfk.crivo_bacen_score IS NOT NULL
        OR cfk.crivo_overdue_portfolio_value IS NOT NULL
        OR cfk.crivo_loss_value IS NOT NULL
      )
       AND (
         IFF(cr.crivo_resolution_stage IN ('original','engineable'), 0,
           IFF(cr.crivo_resolution_stage = 'cpf_primary', 1,
             IFF(cr.crivo_resolution_stage = 'cpf_fallback_15d', 2,
               IFF(cr.crivo_resolution_stage = 'cpf_fallback_180d', 3, 99)
             )
           )
         )
         <=
         COALESCE(
           IFF(bi.bacen_match_stage = 'strict_primary_1h', 0,
             IFF(bi.bacen_match_stage = 'lenient_primary_24h', 1,
               IFF(bi.bacen_match_stage = 'lenient_fallback_15d', 2,
                 IFF(bi.bacen_match_stage = 'lenient_fallback_180d', 3, 99)
               )
             )
           ),
           99
         )
       ) THEN cr.crivo_resolution_stage
      WHEN bi.bacen_internal_score IS NOT NULL THEN bi.bacen_match_stage
      WHEN si.serasa_income_estimated IS NOT NULL THEN si.serasa_income_only_match_stage
      ELSE NULL
    END AS renda_proxies_evidence_match_stage,

    CASE
      WHEN (
        cfk.crivo_credit_limits_value IS NOT NULL
        OR cfk.crivo_bacen_score IS NOT NULL
        OR cfk.crivo_overdue_portfolio_value IS NOT NULL
        OR cfk.crivo_loss_value IS NOT NULL
      )
       AND (
         IFF(cr.crivo_resolution_stage IN ('original','engineable'), 0,
           IFF(cr.crivo_resolution_stage = 'cpf_primary', 1,
             IFF(cr.crivo_resolution_stage = 'cpf_fallback_15d', 2,
               IFF(cr.crivo_resolution_stage = 'cpf_fallback_180d', 3, 99)
             )
           )
         )
         <=
         COALESCE(
           IFF(bi.bacen_match_stage = 'strict_primary_1h', 0,
             IFF(bi.bacen_match_stage = 'lenient_primary_24h', 1,
               IFF(bi.bacen_match_stage = 'lenient_fallback_15d', 2,
                 IFF(bi.bacen_match_stage = 'lenient_fallback_180d', 3, 99)
               )
             )
           ),
           99
         )
       ) THEN cr.crivo_minutes_from_cs
      WHEN bi.bacen_internal_score IS NOT NULL THEN bi.bacen_minutes_from_cs
      WHEN si.serasa_income_estimated IS NOT NULL THEN si.serasa_income_only_minutes_from_cs
      ELSE NULL
    END AS renda_proxies_evidence_minutes_from_cs
    ,
    /* ===== Renda estimada (core) — seleção explícita sem misturar semânticas ===== */
    COALESCE(
      si.serasa_income_estimated,
      cf.crivo_renda_presumida_credilink,
      cf.crivo_renda_presumida_serasa,
      (bs.monthly_income)::FLOAT
    ) AS income_estimated,
    CASE
      WHEN si.serasa_income_estimated IS NOT NULL THEN 'serasa_check_income_only_hrp_score_cents'
      WHEN cf.crivo_renda_presumida_credilink IS NOT NULL THEN 'crivo_credilink_renda_presumida'
      WHEN cf.crivo_renda_presumida_serasa IS NOT NULL THEN 'crivo_serasa_renda_presumida'
      WHEN bs.monthly_income IS NOT NULL THEN 'sensitive_last_resort'
      ELSE NULL
    END AS income_estimated_source,
    CASE
      WHEN si.serasa_income_estimated IS NOT NULL THEN si.serasa_income_only_match_stage
      WHEN cf.crivo_renda_presumida_credilink IS NOT NULL THEN cr.crivo_resolution_stage
      WHEN cf.crivo_renda_presumida_serasa IS NOT NULL THEN cr.crivo_resolution_stage
      ELSE NULL
    END AS income_estimated_evidence_match_stage,
    CASE
      WHEN si.serasa_income_estimated IS NOT NULL THEN si.serasa_income_only_minutes_from_cs
      WHEN cf.crivo_renda_presumida_credilink IS NOT NULL THEN cr.crivo_minutes_from_cs
      WHEN cf.crivo_renda_presumida_serasa IS NOT NULL THEN cr.crivo_minutes_from_cs
      ELSE NULL
    END AS income_estimated_evidence_minutes_from_cs
  FROM cs_enriched cs
  LEFT JOIN crivo_features_key_params cfk
    ON cfk.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN crivo_features_campos cf
    ON cf.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN crivo_resolution cr
    ON cr.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN bacen_internal_features bi
    ON bi.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_new_income_only_features si
    ON si.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN scr_features scr
    ON scr.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN borrower_sensitive bs
    ON bs.borrower_person_id = cs.borrower_person_id
),

axis_score AS (
  /* Mantemos apenas o que ainda é exposto no schema final (sem score canônico) */
  SELECT
    cs.credit_simulation_id,
    so.serasa_old_score_range_name,
    so.serasa_old_delinquency_prob_pct
  FROM cs_enriched cs
  LEFT JOIN serasa_old_features so
    ON so.credit_simulation_id = cs.credit_simulation_id
),

credit_check_stats AS (
  SELECT
    cs.credit_simulation_id,
    COUNT(DISTINCT cc.id) AS total_credit_checks_count
  FROM cs_enriched cs
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('hour', -1, cs.cs_created_at) AND DATEADD('hour', 1, cs.cs_created_at)
  GROUP BY 1
),

/* ============================
   Saída final (1 linha por simulation)
   ============================ */
final AS (
  SELECT
    cs.credit_simulation_id,
    cs.credit_lead_id,
    cs.clinic_id,
    cs.patient_id,
    cs.financial_responsible_id,
    cs.borrower_person_id,
    cs.borrower_role,

    /* linhagem - CPF efetivo */
    cs.patient_cpf,
    cs.financial_responsible_cpf,
    cs.cpf_effective,
    cs.cpf_effective_digits,

    /* estado da simulação */
    cs.state AS credit_simulation_state,
    cs.rejection_reason AS credit_simulation_rejection_reason,
    cs.cs_created_at,
    cs.cs_updated_at,

    /* status simplificado (política): aprovado se tiver approved_at OU (permitted_amount>0 e não for rejeitado/erro) */
    IFF(
      cs.approved_at IS NOT NULL
      OR (
        cs.permitted_amount > 0
        AND cs.state NOT IN ('rejected','error','errored')
      ),
      TRUE, FALSE
    ) AS credit_simulation_was_approved,

    /* risk (Capim) + prob. default (se existir) */
    /* risk_capim é a mesma variável que CREDIT_SIMULATIONS.SCORE (0..5,-1,9),
       mas o SCORE às vezes vem como "9,00". Preferimos o que vier preenchido via PRE_ANALYSES (type=credit_simulation),
       e caímos no SCORE normalizado se necessário. */
    COALESCE(pa_risk.risk_capim, TRY_TO_NUMBER(REPLACE(cs.risk_score_raw, ',', '.'))::NUMBER) AS risk_capim,
    pa_risk.risk_capim_subclass,
    cs.payment_default_risk,

    /* flags analíticas */
    IFF(
      (cs.approved_at IS NOT NULL OR (cs.permitted_amount > 0 AND cs.state NOT IN ('rejected','error','errored')))
      AND cs.credit_lead_requested_amount IS NOT NULL
      AND cs.permitted_amount IS NOT NULL
      AND cs.permitted_amount > 0
      AND cs.credit_lead_requested_amount > 0
      AND cs.permitted_amount + 1e-6 < cs.credit_lead_requested_amount,
      TRUE, FALSE
    ) AS c1_has_counter_proposal,

    /* financiamento (valores em reais) */
    cs.permitted_amount,
    fin.financing_term_min,
    fin.financing_term_max,
    fin.financing_installment_value_min,
    fin.financing_installment_value_max,
    fin.financing_total_debt_min,
    fin.financing_total_debt_max,

    /* lead auxiliares */
    cs.credit_lead_requested_amount,
    cs.under_age_patient_verified,

    /* retry/appeal (canônico) */
    cs.appealable AS c1_appealable,

    /* profundidade (janela ±1h) */
    COALESCE(ccs.total_credit_checks_count, 0) AS total_credit_checks_count,

    /* credit checks (ordem da política): BVS -> SERASA -> BACEN */
    bvs_pf.bvs_score AS boa_vista_score,
    sl.serasa_score,
    sl.serasa_score_source,
    sc.serasa_old_score_range_name,
    sc.serasa_old_delinquency_prob_pct,
    ren.bacen_internal_score,
    bi.bacen_is_not_banked AS bacen_is_not_banked,
    bi.bacen_credit_limits_total AS bacen_credit_limits_total,
    bi.bacen_mean_due_value_credit_limits AS bacen_mean_due_value_credit_limits,

    /* crivo (por último, conforme política) */
    cr.crivo_check_id_original,
    cr.crivo_check_id_resolved,
    cr.crivo_resolution_stage,
    ren.crivo_bacen_score,
    ren.crivo_credit_limits_value,
    ren.crivo_overdue_portfolio_value,
    ren.crivo_loss_value,

    /* ===== EIXO 1: cadastro/demografia ===== */
    cad.borrower_birthdate,
    cad.borrower_birthdate_source,
    cad.borrower_gender,
    cad.borrower_gender_source,
    cad.borrower_zipcode,
    cad.borrower_zipcode_source,
    cad.borrower_city,
    cad.borrower_state,
    cad.borrower_registration_status,
    cad.borrower_registration_status_source,
    cad.borrower_registration_status_date,
    cad.borrower_has_phone,
    cad.borrower_has_phone_source,
    cad.borrower_has_address,
    cad.borrower_has_address_source,
    cad.cadastro_evidence_source,
    cad.cadastro_evidence_match_stage,
    cad.cadastro_evidence_minutes_from_cs,

    /* ===== EIXO 2: negativação/restrições ===== */
    neg.pefin_count,
    neg.refin_count,
    neg.protesto_count,
    
    /* valores monetários unificados */
    neg.pefin_value,
    neg.refin_value,
    neg.protesto_value,
    neg.total_negative_value,
    neg.negativacao_source,
    neg.negativacao_evidence_match_stage,
    neg.negativacao_evidence_minutes_from_cs,

    /* ===== EIXO 3: renda/proxies ===== */
    ren.sensitive_monthly_income,
    ren.serasa_income_estimated,
    ren.crivo_renda_presumida,
    ren.income_estimated,
    ren.income_estimated_source,
    ren.income_estimated_evidence_match_stage,
    ren.income_estimated_evidence_minutes_from_cs,
    ren.scr_operations_count,
    ren.scr_vencimentos_count,
    ren.scr_sum_valor_raw,
    ren.renda_proxies_source,
    ren.renda_proxies_evidence_match_stage,
    ren.renda_proxies_evidence_minutes_from_cs
  FROM cs_enriched cs
  LEFT JOIN pa_cs_dedup pa_risk
    ON pa_risk.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN financing_features fin
    ON fin.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_lineage sl
    ON sl.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN crivo_resolution cr
    ON cr.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN axis_cadastro cad
    ON cad.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN axis_negativacao neg
    ON neg.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN axis_renda ren
    ON ren.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN axis_score sc
    ON sc.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN boa_vista_score_pf_features bvs_pf
    ON bvs_pf.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN bacen_internal_features bi
    ON bi.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN credit_check_stats ccs
    ON ccs.credit_simulation_id = cs.credit_simulation_id
)

SELECT *
FROM final
;
