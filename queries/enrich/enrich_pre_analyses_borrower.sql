/*
  v1 — Enriquecimento de C1 no grão "PRE_ANALYSES unificada"
  Objetivo:
    - Produzir 1 linha por C1, preservando coexistência (2025+):
        chave canônica = (pre_analysis_type, pre_analysis_id)
    - Permitir análise do topo do funil (C1) com eixos do paciente
      sem depender do ETL problemático de unificação.

  Estratégia (por tipo):
    1) PRE_ANALYSIS_TYPE='credit_simulation'
       - PRE_ANALYSIS_ID == CREDIT_SIMULATIONS.id (validado em queries/audit/audit_pre_analyses.sql [E3])
       - Reusar o enrichment canônico já implementado para credit_simulations.
    2) PRE_ANALYSIS_TYPE='pre_analysis' (legado)
       - Recuperar CPF + birthdate/zipcode/state/occupation via RESTRICTED.SOURCE_PRE_ANALYSIS_API
       - Associar credit checks por CPF+tempo (±1h, ±24h, lookback 15d/180d)
       - Associar Crivo por CPF+tempo (não há engineable_type='PreAnalysis' em SOURCE_CRIVO_CHECKS)
       - Extrair eixos (cadastro/negativação/renda/scores) de fontes já consolidadas (credit checks + crivo)

  Guardrails importantes:
    - NÃO usar PRE_ANALYSES.CRIVO_ID para join: empiricamente não casa com SOURCE_CRIVO_CHECKS.crivo_check_id.
    - Deduplicação: PRE_ANALYSES pode reprocessar o mesmo (type,id). Escolhemos o mais recente por PRE_ANALYSIS_UPDATED_AT.
    - LGPD: não persistir CPF cru na saída final; usar apenas para joins dentro da query.

  Dependências:
    - Enrichment canônico (simulações): CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
      (ajuste o schema se necessário)
    - Legacy: CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API (contém CPF e BIRTHDATE)
    - Credit checks: CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API (por CPF+tempo)
    - Crivo checks: CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS (CPF em KEY_PARAMETERS; fallback por CPF+tempo)
*/

WITH params AS (
  SELECT
    1  ::INT AS primary_hours,
    24 ::INT AS primary_hours_wide,
    15 ::INT AS cache_days,
    180::INT AS cap_days,
    1  ::INT AS crivo_primary_hours,
    15 ::INT AS crivo_cache_days,
    180::INT AS crivo_cap_days,

    /* ============================
       SAMPLING (opcional; para evitar full scan)
       - Se sample_n_per_month for NULL: roda tudo (padrão)
       - Se sample_n_per_month tiver valor: limita a N linhas por (type, mês)
       - Se sample_months estiver vazio: não restringe por mês (padrão)
       - Se sample_months tiver valores: restringe aos meses listados
       ============================ */
    NULL::INT AS sample_n_per_month
),

/* Lista opcional de meses (primeiro dia do mês). Deixe vazio para não filtrar por período. */
sample_months AS (
  SELECT NULL::DATE AS month WHERE FALSE
  /*
  UNION ALL SELECT TO_DATE('2025-12-01')
  UNION ALL SELECT TO_DATE('2025-09-01')
  UNION ALL SELECT TO_DATE('2025-03-01')
  UNION ALL SELECT TO_DATE('2024-10-01')
  */
),

/* ============================
   Base: PRE_ANALYSES deduplicada no grão (type,id)
   ============================ */
pa_dedup AS (
  SELECT
    pa.PRE_ANALYSIS_TYPE,
    pa.PRE_ANALYSIS_ID,
    pa.PRE_ANALYSIS_CREATED_AT,
    pa.PRE_ANALYSIS_UPDATED_AT,
    pa.RETAIL_ID AS clinic_id,
    pa.RETAIL_GROUP,
    pa.RETAIL_GROUP_ID,
    pa.CREDIT_LEAD_ID,
    pa.PRE_ANALYSIS_STATE,
    pa.RISK_CAPIM,
    pa.RISK_CAPIM_SUBCLASS,
    pa.REJECTION_REASON,
    pa.PRE_ANALYSIS_AMOUNT,
    pa.PRE_ANALYSIS_INSTALLMENT_AMOUNT,
    pa.COUNTER_PROPOSAL_AMOUNT,
    pa.MAXIMUM_TERM_AVAILABLE,
    pa.MINIMUM_TERM_AVAILABLE,
    pa.PROPOSAL_INTEREST,
    pa.HAS_REQUEST,
    pa.FINANCING_CONDITIONS,
    pa.INTEREST_RATES_ARRAY
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE
    (SELECT COUNT(*) FROM sample_months) = 0
    OR DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) IN (SELECT month FROM sample_months)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pa.PRE_ANALYSIS_TYPE, pa.PRE_ANALYSIS_ID
    ORDER BY
      pa.PRE_ANALYSIS_UPDATED_AT DESC,
      pa.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
),

/* Escopo final: aplica sampling por mês/tipo (opcional) */
pa_scope AS (
  SELECT pa.*
  FROM pa_dedup pa
  JOIN params p ON TRUE
  QUALIFY
    p.sample_n_per_month IS NULL
    OR ROW_NUMBER() OVER (
      PARTITION BY pa.PRE_ANALYSIS_TYPE, DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT)
      ORDER BY UNIFORM(0, 1000000, RANDOM())
    ) <= p.sample_n_per_month
),

/* ============================
   Parte A: tipo credit_simulation → reuso do enrichment de credit_simulations
   ============================ */
pa_as_credit_simulation AS (
  SELECT
    'credit_simulation' AS c1_entity_type,
    pa.PRE_ANALYSIS_ID  AS c1_entity_id,
    pa.PRE_ANALYSIS_ID  AS credit_simulation_id,
    pa.CREDIT_LEAD_ID,
    pa.clinic_id,
    pa.PRE_ANALYSIS_CREATED_AT AS c1_created_at,
    pa.PRE_ANALYSIS_STATE  AS c1_state,
    pa.RISK_CAPIM,
    pa.RISK_CAPIM_SUBCLASS,
    pa.REJECTION_REASON,
    pa.PRE_ANALYSIS_AMOUNT AS c1_amount,
    pa.COUNTER_PROPOSAL_AMOUNT,
    pa.MAXIMUM_TERM_AVAILABLE,
    pa.MINIMUM_TERM_AVAILABLE,
    pa.PROPOSAL_INTEREST,
    pa.HAS_REQUEST,
    pa.FINANCING_CONDITIONS
  FROM pa_scope pa
  WHERE pa.PRE_ANALYSIS_TYPE = 'credit_simulation'
),

cs_enriched AS (
  /* Fonte canônica já materializada no fluxo atual.
     Ajuste se o schema destino for diferente. */
  SELECT *
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER
),

part_a_reused AS (
  SELECT
    a.c1_entity_type,
    a.c1_entity_id,
    a.credit_simulation_id,
    a.credit_lead_id,
    a.clinic_id,
    a.c1_created_at,
    a.c1_state,
    a.risk_capim,
    a.risk_capim_subclass,
    a.rejection_reason,
    a.c1_amount,
    a.counter_proposal_amount,
    a.maximum_term_available,
    a.minimum_term_available,
    a.proposal_interest,
    a.has_request,
    /* comparabilidade de estado/outcome/valores */
    cs.credit_simulation_state AS c1_state_raw,
    NULL::BOOLEAN AS legacy_has_financing_signal,
    cs.credit_simulation_was_approved AS c1_was_approved,
    IFF(cs.credit_simulation_was_approved, 'approved', 'not_approved') AS c1_outcome_bucket,
    cs.credit_simulation_rejection_reason AS c1_rejection_reason,
    IFF(
      cs.credit_simulation_was_approved = FALSE
      AND cs.under_age_patient_verified = TRUE
      AND cs.borrower_role = 'patient',
      TRUE, FALSE
    ) AS c1_can_retry_with_financial_responsible,
    cs.credit_lead_requested_amount AS c1_requested_amount,
    cs.permitted_amount AS c1_approved_amount,
    cs.financing_term_min,
    cs.financing_term_max,
    cs.financing_installment_value_min,
    cs.financing_installment_value_max,
    cs.financing_total_debt_min,
    cs.financing_total_debt_max,

    /* ===== EIXO 1: cadastro/demografia (reuso) ===== */
    cs.borrower_birthdate,
    cs.borrower_birthdate_source,
    cs.borrower_gender,
    cs.borrower_gender_source,
    cs.borrower_zipcode,
    cs.borrower_zipcode_source,
    cs.borrower_city,
    cs.borrower_state,
    cs.borrower_registration_status,
    cs.borrower_registration_status_source,
    cs.borrower_registration_status_date,
    cs.borrower_has_phone,
    cs.borrower_has_phone_source,
    cs.borrower_has_address,
    cs.borrower_has_address_source,
    cs.cadastro_evidence_source,
    cs.cadastro_evidence_match_stage,
    cs.cadastro_evidence_minutes_from_cs,

    /* ===== EIXO 2: negativação ===== */
    cs.pefin_count,
    cs.refin_count,
    cs.protesto_count,
    cs.pefin_value,
    cs.refin_value,
    cs.protesto_value,
    cs.total_negative_value,
    cs.negativacao_source,
    cs.negativacao_evidence_match_stage,
    cs.negativacao_evidence_minutes_from_cs,

    /* ===== EIXO 3: renda/proxies ===== */
    cs.sensitive_monthly_income,
    cs.serasa_income_estimated,
    cs.crivo_renda_presumida,
    cs.income_estimated,
    cs.income_estimated_source,
    cs.income_estimated_evidence_match_stage,
    cs.income_estimated_evidence_minutes_from_cs,
    cs.scr_operations_count,
    cs.scr_vencimentos_count,
    cs.scr_sum_valor_raw,
    cs.renda_proxies_source,
    cs.renda_proxies_evidence_match_stage,
    cs.renda_proxies_evidence_minutes_from_cs,

    /* ===== EIXO 4: scores ===== */
    cs.boa_vista_score,
    cs.serasa_score,
    cs.serasa_score_source,
    cs.bacen_internal_score,
    cs.serasa_old_score_range_name,
    cs.serasa_old_delinquency_prob_pct,

    /* Crivo lineage (quando existir) */
    cs.crivo_check_id_resolved,
    cs.crivo_resolution_stage,
    NULL::NUMBER AS crivo_minutes_from_cs
  FROM pa_as_credit_simulation a
  LEFT JOIN cs_enriched cs
    ON cs.credit_simulation_id = a.credit_simulation_id
),

/* ============================
   Parte B: tipo pre_analysis (legado) → enrichment por CPF+tempo
   ============================ */
spa_dedup AS (
  /* SOURCE_PRE_ANALYSIS_API pode ter múltiplas linhas por PRE_ANALYSIS_ID.
     Para manter 1 linha por (type,id) no C1 unificado, deduplicamos aqui. */
  SELECT
    spa.*
  FROM CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API spa
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY spa.PRE_ANALYSIS_ID
    ORDER BY
      spa.PRE_ANALYSIS_UPDATED_AT DESC,
      spa.PRE_ANALYSIS_CREATED_AT DESC,
      spa.PRE_ANALYSIS_ID DESC
  ) = 1
),

pa_legacy AS (
  SELECT
    'pre_analysis' AS c1_entity_type,
    pa.PRE_ANALYSIS_ID AS c1_entity_id,
    NULL::NUMBER AS credit_simulation_id,
    NULL::NUMBER AS credit_lead_id,
    pa.clinic_id,
    pa.PRE_ANALYSIS_CREATED_AT AS c1_created_at,
    pa.PRE_ANALYSIS_STATE AS c1_state,
    pa.RISK_CAPIM,
    pa.RISK_CAPIM_SUBCLASS,
    pa.REJECTION_REASON,
    pa.PRE_ANALYSIS_AMOUNT AS c1_amount,
    pa.COUNTER_PROPOSAL_AMOUNT,
    pa.MAXIMUM_TERM_AVAILABLE,
    pa.MINIMUM_TERM_AVAILABLE,
    pa.PROPOSAL_INTEREST,
    pa.HAS_REQUEST,
    pa.FINANCING_CONDITIONS,
    pa.INTEREST_RATES_ARRAY,

    spa.CPF AS cpf_raw,
    REGEXP_REPLACE(spa.CPF, '\\D','') AS cpf_digits,
    SHA2(REGEXP_REPLACE(spa.CPF, '\\D',''), 256) AS hash_cpf,
    TO_DATE(spa.BIRTHDATE) AS legacy_birthdate,
    spa.ZIPCODE AS legacy_zipcode,
    spa.STATE   AS legacy_state,
    spa.OCCUPATION AS legacy_occupation
  FROM pa_scope pa
  LEFT JOIN spa_dedup spa
    ON spa.PRE_ANALYSIS_ID = pa.PRE_ANALYSIS_ID
  WHERE pa.PRE_ANALYSIS_TYPE = 'pre_analysis'
),

/* ===== Financing (legado): reconstrução via INTEREST_RATES_ARRAY + MIN/MAX_TERM_AVAILABLE =====
   Achado empírico (auditoria): FINANCING_CONDITIONS vem 0% preenchido no legado,
   mas INTEREST_RATES_ARRAY (OBJECT com chaves "3..6", "7..9", ...) tem boa cobertura.

   Interpretação prática:
     - usamos MINIMUM/MAXIMUM_TERM_AVAILABLE como termo min/max;
     - inferimos a taxa mensal para esses termos olhando o bucket correspondente;
     - estimamos parcela e dívida total usando fórmula financeira (PMT) aproximada.
*/
pa_legacy_interest_rate_buckets AS (
  SELECT
    pa.c1_entity_id,
    TRY_TO_NUMBER(SPLIT_PART(f.key::string, '..', 1)) AS term_start,
    TRY_TO_NUMBER(SPLIT_PART(f.key::string, '..', 2)) AS term_end,
    TRY_TO_NUMBER(f.value::string) AS monthly_rate
  FROM pa_legacy pa,
  LATERAL FLATTEN(input => pa.INTEREST_RATES_ARRAY) f
  WHERE pa.INTEREST_RATES_ARRAY IS NOT NULL
    AND TYPEOF(pa.INTEREST_RATES_ARRAY) = 'OBJECT'
),

pa_legacy_interest_rate_bucket_bounds AS (
  SELECT
    c1_entity_id,
    MIN(term_start) AS inferred_term_min,
    MAX(term_end) AS inferred_term_max
  FROM pa_legacy_interest_rate_buckets
  GROUP BY 1
),

pa_legacy_terms AS (
  SELECT
    pa.c1_entity_id,
    COALESCE(pa.MINIMUM_TERM_AVAILABLE::NUMBER, b.inferred_term_min)::NUMBER AS term_min,
    COALESCE(pa.MAXIMUM_TERM_AVAILABLE::NUMBER, b.inferred_term_max)::NUMBER AS term_max
  FROM pa_legacy pa
  LEFT JOIN pa_legacy_interest_rate_bucket_bounds b
    ON b.c1_entity_id = pa.c1_entity_id
),

pa_legacy_interest_rate_at_terms AS (
  SELECT
    t.c1_entity_id,
    MAX(IFF(t.term_min BETWEEN b.term_start AND b.term_end, b.monthly_rate, NULL)) AS rate_at_term_min,
    MAX(IFF(t.term_max BETWEEN b.term_start AND b.term_end, b.monthly_rate, NULL)) AS rate_at_term_max
  FROM pa_legacy_terms t
  LEFT JOIN pa_legacy_interest_rate_buckets b
    ON b.c1_entity_id = t.c1_entity_id
  GROUP BY 1
),

pa_legacy_financing_estimates AS (
  SELECT
    pa.c1_entity_id,
    t.term_min AS financing_term_min,
    t.term_max AS financing_term_max,

    /* PMT aproximado (mensal): pmt = r*PV / (1-(1+r)^-n) */
    IFF(
      r.rate_at_term_min IS NULL OR t.term_min IS NULL OR pa.c1_amount IS NULL
      OR t.term_min <= 0 OR r.rate_at_term_min <= -0.999999,
      NULL,
      IFF(
        ABS(r.rate_at_term_min) < 0.000000001,
        (pa.c1_amount::FLOAT) / t.term_min,
        (r.rate_at_term_min * (pa.c1_amount::FLOAT)) / (1 - POWER(1 + r.rate_at_term_min, -t.term_min))
      )
    ) AS installment_at_term_min,

    IFF(
      r.rate_at_term_max IS NULL OR t.term_max IS NULL OR pa.c1_amount IS NULL
      OR t.term_max <= 0 OR r.rate_at_term_max <= -0.999999,
      NULL,
      IFF(
        ABS(r.rate_at_term_max) < 0.000000001,
        (pa.c1_amount::FLOAT) / t.term_max,
        (r.rate_at_term_max * (pa.c1_amount::FLOAT)) / (1 - POWER(1 + r.rate_at_term_max, -t.term_max))
      )
    ) AS installment_at_term_max,

    IFF(
      r.rate_at_term_min IS NULL OR t.term_min IS NULL OR pa.c1_amount IS NULL
      OR t.term_min <= 0 OR r.rate_at_term_min <= -0.999999,
      NULL,
      IFF(
        ABS(r.rate_at_term_min) < 0.000000001,
        (pa.c1_amount::FLOAT),
        (
          (r.rate_at_term_min * (pa.c1_amount::FLOAT)) / (1 - POWER(1 + r.rate_at_term_min, -t.term_min))
        ) * t.term_min
      )
    ) AS total_debt_at_term_min,

    IFF(
      r.rate_at_term_max IS NULL OR t.term_max IS NULL OR pa.c1_amount IS NULL
      OR t.term_max <= 0 OR r.rate_at_term_max <= -0.999999,
      NULL,
      IFF(
        ABS(r.rate_at_term_max) < 0.000000001,
        (pa.c1_amount::FLOAT),
        (
          (r.rate_at_term_max * (pa.c1_amount::FLOAT)) / (1 - POWER(1 + r.rate_at_term_max, -t.term_max))
        ) * t.term_max
      )
    ) AS total_debt_at_term_max
  FROM pa_legacy pa
  LEFT JOIN pa_legacy_terms t
    ON t.c1_entity_id = pa.c1_entity_id
  LEFT JOIN pa_legacy_interest_rate_at_terms r
    ON r.c1_entity_id = pa.c1_entity_id
),

pa_legacy_financing_features AS (
  SELECT
    c1_entity_id,
    financing_term_min,
    financing_term_max,

    /* min/max robusto com NULLs */
    IFF(installment_at_term_min IS NULL, installment_at_term_max,
      IFF(installment_at_term_max IS NULL, installment_at_term_min,
        LEAST(installment_at_term_min, installment_at_term_max)
      )
    ) AS financing_installment_value_min,

    IFF(installment_at_term_min IS NULL, installment_at_term_max,
      IFF(installment_at_term_max IS NULL, installment_at_term_min,
        GREATEST(installment_at_term_min, installment_at_term_max)
      )
    ) AS financing_installment_value_max,

    IFF(total_debt_at_term_min IS NULL, total_debt_at_term_max,
      IFF(total_debt_at_term_max IS NULL, total_debt_at_term_min,
        LEAST(total_debt_at_term_min, total_debt_at_term_max)
      )
    ) AS financing_total_debt_min,

    IFF(total_debt_at_term_min IS NULL, total_debt_at_term_max,
      IFF(total_debt_at_term_max IS NULL, total_debt_at_term_min,
        GREATEST(total_debt_at_term_min, total_debt_at_term_max)
      )
    ) AS financing_total_debt_max
  FROM pa_legacy_financing_estimates
),

/* ===== PACC (curado) como first-choice no legado =====
   - 1 linha por PRE_ANALYSIS_ID (assumido; ainda assim deduplicamos defensivamente)
   - Observação: é curado por hash_cpf + janela/cache no dbt; pode existir mesmo sem raw recente. */
pacc_legacy AS (
  SELECT
    p.PRE_ANALYSIS_ID::NUMBER AS c1_entity_id,
    p.PRE_ANALYSIS_CREATED_AT,
    TRY_TO_NUMBER(p.SERASA_POSITIVE_SCORE::string) AS pacc_serasa_score,
    TRY_TO_NUMBER(p.BVS_POSITIVE_SCORE::string)    AS pacc_boa_vista_score,
    TRY_TO_NUMBER(p.SERASA_PRESUMED_INCOME::string) AS pacc_serasa_presumed_income,
    TRY_TO_NUMBER(p.SERASA_PEFIN::string)   AS pacc_serasa_pefin_count,
    TRY_TO_NUMBER(p.SERASA_REFIN::string)   AS pacc_serasa_refin_count,
    TRY_TO_NUMBER(p.SERASA_PROTEST::string) AS pacc_serasa_protest_count
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK p
  WHERE p.PRE_ANALYSIS_TYPE = 'pre_analysis'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.PRE_ANALYSIS_ID
    ORDER BY p.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
),

/* ===== Hash-based sources (mesma família do dbt/PACC) =====
   Estas views curadas costumam ter cobertura bem superior ao raw incremental no legado.
   A lógica de janela usa:
     days_from_consultation = ABS(DATEDIFF('days', c1_created_at, DATEADD('hours', -3, consulted_at)))
   e aceita 0..15 dias (cache window).
*/

serasa_hash_old_15d AS (
  SELECT
    pa.c1_entity_id,
    s.serasa_consulted_at,
    s.serasa_ir_status,
    s.serasa_ccf,
    s.serasa_positive_score,
    s.serasa_refin,
    s.serasa_pefin,
    s.serasa_protest,
    s.serasa_presumed_income,
    ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.serasa_consulted_at))) AS days_from_consultation,
    ROW_NUMBER() OVER (
      PARTITION BY pa.c1_entity_id
      ORDER BY ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.serasa_consulted_at))) ASC
    ) AS rn
  FROM pa_legacy pa
  JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_SERASA s
    ON s.hash_cpf = pa.hash_cpf
  WHERE DATE(pa.c1_created_at) < '2024-04-04'
    AND (s.kind IS NULL OR s.kind = 'check_score')
    AND ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.serasa_consulted_at))) BETWEEN 0 AND 15
),

serasa_hash_new_15d_aux AS (
  /* No fluxo novo (>= cutover), o dbt considera até 2 checks (income_only + score_without_income) */
  SELECT
    pa.c1_entity_id,
    s.serasa_consulted_at,
    s.serasa_ir_status,
    s.serasa_ccf,
    s.serasa_positive_score,
    s.serasa_refin,
    s.serasa_pefin,
    s.serasa_protest,
    s.serasa_presumed_income,
    ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.serasa_consulted_at))) AS days_from_consultation,
    ROW_NUMBER() OVER (
      PARTITION BY pa.c1_entity_id
      ORDER BY ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.serasa_consulted_at))) ASC
    ) AS rn
  FROM pa_legacy pa
  JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_SERASA s
    ON s.hash_cpf = pa.hash_cpf
  WHERE DATE(pa.c1_created_at) >= '2024-04-04'
    AND s.kind IN ('check_income_only', 'check_score_without_income')
    AND ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.serasa_consulted_at))) BETWEEN 0 AND 15
),

serasa_hash_new_15d AS (
  SELECT
    c1_entity_id,
    MAX(serasa_consulted_at) AS serasa_consulted_at,
    MAX(serasa_ir_status) AS serasa_ir_status,
    MAX(serasa_ccf) AS serasa_ccf,
    MAX(serasa_positive_score) AS serasa_positive_score,
    MAX(serasa_refin) AS serasa_refin,
    MAX(serasa_pefin) AS serasa_pefin,
    MAX(serasa_protest) AS serasa_protest,
    MAX(serasa_presumed_income) AS serasa_presumed_income,
    MIN(days_from_consultation) AS days_from_consultation
  FROM serasa_hash_new_15d_aux
  WHERE rn < 3
  GROUP BY 1
),

serasa_hash_15d AS (
  SELECT
    c1_entity_id,
    serasa_consulted_at,
    serasa_ir_status,
    serasa_ccf,
    serasa_positive_score,
    serasa_refin,
    serasa_pefin,
    serasa_protest,
    serasa_presumed_income,
    days_from_consultation
  FROM serasa_hash_old_15d
  WHERE rn = 1
  UNION ALL
  SELECT
    c1_entity_id,
    serasa_consulted_at,
    serasa_ir_status,
    serasa_ccf,
    serasa_positive_score,
    serasa_refin,
    serasa_pefin,
    serasa_protest,
    serasa_presumed_income,
    days_from_consultation
  FROM serasa_hash_new_15d
),

bvs_score_pf_hash_15d AS (
  SELECT
    pa.c1_entity_id,
    v.bvs_score_pf_net_consulted_at,
    v.bvs_positive_score,
    ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, v.bvs_score_pf_net_consulted_at))) AS days_from_consultation,
    ROW_NUMBER() OVER (
      PARTITION BY pa.c1_entity_id
      ORDER BY ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, v.bvs_score_pf_net_consulted_at))) ASC
    ) AS rn
  FROM pa_legacy pa
  JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_BOA_VISTA_SCORE_PF v
    ON v.hash_cpf = pa.hash_cpf
  WHERE ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, v.bvs_score_pf_net_consulted_at))) BETWEEN 0 AND 15
  QUALIFY rn = 1
),

/* BVS SCPC NET (hash+15d): traz valores agregados (dívida/protestos) com boa cobertura histórica */
bvs_scpc_hash_15d AS (
  SELECT
    pa.c1_entity_id,
    s.bvs_scpc_net_consulted_at,
    s.bvs_ccf_count,
    s.bvs_total_debt,
    s.bvs_total_protest,
    s.bvs_status_ir,
    ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.bvs_scpc_net_consulted_at))) AS days_from_consultation,
    ROW_NUMBER() OVER (
      PARTITION BY pa.c1_entity_id
      ORDER BY ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.bvs_scpc_net_consulted_at))) ASC
    ) AS rn
  FROM pa_legacy pa
  JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_BOA_VISTA_SCPC_NET s
    ON s.hash_cpf = pa.hash_cpf
  WHERE ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, s.bvs_scpc_net_consulted_at))) BETWEEN 0 AND 15
  QUALIFY rn = 1
),

scr_report_hash_15d AS (
  SELECT
    pa.c1_entity_id,
    r.scr_report_consulted_at,
    TRY_TO_NUMBER(r.scr_qtd_de_operacoes::string) AS scr_qtd_de_operacoes,
    ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, r.scr_report_consulted_at))) AS days_from_consultation,
    ROW_NUMBER() OVER (
      PARTITION BY pa.c1_entity_id
      ORDER BY ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, r.scr_report_consulted_at))) ASC
    ) AS rn
  FROM pa_legacy pa
  JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_SCR_REPORT r
    ON r.hash_cpf = pa.hash_cpf
  WHERE ABS(DATEDIFF('days', pa.c1_created_at, DATEADD('hours', -3, r.scr_report_consulted_at))) BETWEEN 0 AND 15
  QUALIFY rn = 1
),

/* ===== Credit checks: associação por CPF + tempo (mesmas janelas do core) ===== */
cc_matches_strict_1h AS (
  SELECT
    pa.c1_entity_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    COALESCE(cc.new_data_format, FALSE) AS new_data_format,
    cc.data       AS credit_check_data,
    'strict_primary_1h' AS match_stage,
    0 AS leniency_rank,
    DATEDIFF('minute', cc.created_at, pa.c1_created_at) AS minutes_from_c1
  FROM pa_legacy pa
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = pa.cpf_digits
   AND cc.created_at BETWEEN DATEADD('hour', -p.primary_hours, pa.c1_created_at)
                        AND DATEADD('hour',  p.primary_hours, pa.c1_created_at)
  WHERE pa.cpf_digits IS NOT NULL
),

cc_matches_lenient_24h AS (
  SELECT
    pa.c1_entity_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    COALESCE(cc.new_data_format, FALSE) AS new_data_format,
    cc.data       AS credit_check_data,
    'lenient_primary_24h' AS match_stage,
    1 AS leniency_rank,
    DATEDIFF('minute', cc.created_at, pa.c1_created_at) AS minutes_from_c1
  FROM pa_legacy pa
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = pa.cpf_digits
   AND cc.created_at BETWEEN DATEADD('hour', -p.primary_hours_wide, pa.c1_created_at)
                        AND DATEADD('hour',  p.primary_hours_wide, pa.c1_created_at)
  WHERE pa.cpf_digits IS NOT NULL
),

cc_matches_fallback_15d AS (
  SELECT
    pa.c1_entity_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    COALESCE(cc.new_data_format, FALSE) AS new_data_format,
    cc.data       AS credit_check_data,
    'lenient_fallback_15d' AS match_stage,
    2 AS leniency_rank,
    DATEDIFF('minute', cc.created_at, pa.c1_created_at) AS minutes_from_c1
  FROM pa_legacy pa
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = pa.cpf_digits
   AND cc.created_at BETWEEN DATEADD('day', -p.cache_days, pa.c1_created_at)
                        AND pa.c1_created_at
  WHERE pa.cpf_digits IS NOT NULL
),

cc_matches_fallback_180d AS (
  SELECT
    pa.c1_entity_id,
    cc.id         AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.source,
    cc.kind,
    COALESCE(cc.new_data_format, FALSE) AS new_data_format,
    cc.data       AS credit_check_data,
    'lenient_fallback_180d' AS match_stage,
    3 AS leniency_rank,
    DATEDIFF('minute', cc.created_at, pa.c1_created_at) AS minutes_from_c1
  FROM pa_legacy pa
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = pa.cpf_digits
   AND cc.created_at BETWEEN DATEADD('day', -p.cap_days, pa.c1_created_at)
                        AND pa.c1_created_at
  WHERE pa.cpf_digits IS NOT NULL
),

cc_all_matches AS (
  SELECT * FROM cc_matches_strict_1h
  UNION ALL SELECT * FROM cc_matches_lenient_24h
  UNION ALL SELECT * FROM cc_matches_fallback_15d
  UNION ALL SELECT * FROM cc_matches_fallback_180d
),

cc_best_per_source_kind AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.c1_entity_id, m.source, m.kind
      ORDER BY
        m.leniency_rank ASC,
        ABS(m.minutes_from_c1) ASC,
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

/* Algumas fontes têm `kind` inconsistente (às vezes NULL). Para evitar duplicação
   na montagem final, escolhemos 1 linha por (c1_entity_id, source). */
cc_best_per_source AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.c1_entity_id, m.source
      ORDER BY
        m.leniency_rank ASC,
        ABS(m.minutes_from_c1) ASC,
        m.credit_check_created_at DESC,
        m.credit_check_id DESC
    ) AS rn_best_source
  FROM cc_all_matches m
),

cc_best_source_1 AS (
  SELECT *
  FROM cc_best_per_source
  WHERE rn_best_source = 1
),

/* ===== Seletores principais (mesmos do core) ===== */
cc_best_serasa_new_score_without_income_1 AS (
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'serasa'
    AND (
      new_data_format = TRUE
      OR (TYPEOF(credit_check_data) = 'OBJECT' AND credit_check_data:reports IS NOT NULL)
    )
    AND kind = 'check_score_without_income'
),

cc_best_serasa_new_income_only_1 AS (
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'serasa'
    AND (
      new_data_format = TRUE
      OR (TYPEOF(credit_check_data) = 'OBJECT' AND credit_check_data:reports IS NOT NULL)
    )
    AND kind = 'check_income_only'
),

cc_best_serasa_old_1 AS (
  SELECT *
  FROM cc_best_source_kind_1
  WHERE source = 'serasa'
    AND new_data_format = FALSE
    AND NOT (TYPEOF(credit_check_data) = 'OBJECT' AND credit_check_data:reports IS NOT NULL)
),

cc_best_boa_vista_score_pf_1 AS (
  SELECT *
  FROM cc_best_source_1
  WHERE source = 'boa_vista_score_pf'
),

cc_best_bacen_internal_1 AS (
  SELECT *
  FROM cc_best_source_1
  WHERE source = 'bacen_internal_score'
),

/* ===== SERASA old (B-codes) ===== */
serasa_old_features AS (
  SELECT
    s.c1_entity_id,
    MAX(NULLIF(TRY_TO_NUMBER(f.value:"B280":score::string), 0)) AS serasa_old_score_b280,
    MAX(f.value:"B280":score_range_name::string) AS serasa_old_score_range_name,
    MAX(TRY_TO_NUMBER(f.value:"B280":delinquency_probability_percent::string)) AS serasa_old_delinquency_prob_pct,
    MAX(TRY_TO_NUMBER(f.value:"B357":occurrences_count::string)) AS serasa_old_b357_occurrences_count,
    MAX(TRY_TO_NUMBER(f.value:"B357":total_occurrence_value::string)) AS serasa_old_b357_total_value,
    MAX(TRY_TO_NUMBER(f.value:"B361":occurrences_count::string)) AS serasa_old_b361_occurrences_count,
    MAX(TRY_TO_NUMBER(f.value:"B361":total_occurrence_value::string)) AS serasa_old_b361_total_value
  FROM cc_best_serasa_old_1 s
  , LATERAL FLATTEN(input => s.credit_check_data) f
  GROUP BY 1
),

/* ===== SERASA new: extrair do report preferido ===== */
serasa_new_reports AS (
  SELECT
    s.c1_entity_id,
    s.credit_check_id,
    s.credit_check_created_at,
    s.match_stage,
    s.minutes_from_c1,
    s.leniency_rank,
    r.value AS report,
    r.index AS report_index
  FROM cc_best_serasa_new_score_without_income_1 s
  , LATERAL FLATTEN(input => s.credit_check_data:reports) r
),

serasa_new_best_report_1 AS (
  SELECT
    sr.*,
    ROW_NUMBER() OVER (
      PARTITION BY sr.c1_entity_id
      ORDER BY
        IFF(sr.report:reportName::string = 'COMBO_CONCESSAO', 0, 1),
        sr.report_index ASC
    ) AS rn_best_report
  FROM serasa_new_reports sr
),

serasa_new_features AS (
  SELECT
    b.c1_entity_id,
    TRY_TO_DATE(b.report:registration:birthDate::string) AS serasa_new_birthdate,
    b.report:registration:consumerGender::string         AS serasa_new_gender,
    b.report:registration:address:zipCode::string        AS serasa_new_zipcode,
    TRY_TO_NUMBER(b.report:negativeData:pefin:summary:count::string)  AS serasa_new_pefin_count,
    TRY_TO_NUMBER(b.report:negativeData:refin:summary:count::string)  AS serasa_new_refin_count,
    TRY_TO_NUMBER(b.report:negativeData:notary:summary:count::string) AS serasa_new_notary_count,
    TRY_TO_NUMBER(b.report:negativeData:pefin:summary:balance::string)  AS serasa_new_pefin_balance,
    TRY_TO_NUMBER(b.report:negativeData:refin:summary:balance::string)  AS serasa_new_refin_balance,
    TRY_TO_NUMBER(b.report:negativeData:notary:summary:balance::string) AS serasa_new_notary_balance
  FROM serasa_new_best_report_1 b
  WHERE b.rn_best_report = 1
),

serasa_income_only_features AS (
  SELECT
    s.c1_entity_id,
    COALESCE(s.credit_check_data:scoreModel::string, s.credit_check_data:data:scoreModel::string) AS score_model,
    IFF(
      COALESCE(s.credit_check_data:scoreModel::string, s.credit_check_data:data:scoreModel::string) ILIKE 'HRP%'
      AND TRY_TO_NUMBER(COALESCE(s.credit_check_data:score::string, s.credit_check_data:data:score::string)) > 0,
      (TRY_TO_NUMBER(COALESCE(s.credit_check_data:score::string, s.credit_check_data:data:score::string)) / 100.0)::FLOAT,
      NULL
    ) AS serasa_income_estimated
  FROM cc_best_serasa_new_income_only_1 s
),

boa_vista_score_pf_features AS (
  SELECT
    b.c1_entity_id,
    TRY_TO_NUMBER(b.credit_check_data:score_positivo:score_classificacao_varios_modelos:score::string) AS boa_vista_score
  FROM cc_best_boa_vista_score_pf_1 b
),

bacen_internal_features AS (
  SELECT
    b.c1_entity_id,
    TRY_TO_NUMBER(b.credit_check_data:predictions[0]:score::string) AS bacen_internal_score
  FROM cc_best_bacen_internal_1 b
),

/* ===== Crivo por CPF+tempo (fallback) ===== */
crivo_base AS (
  SELECT
    c.CRIVO_CHECK_ID,
    c.CRIVO_CHECK_CREATED_AT,
    c.POLITICA,
    c.BUREAU_CHECK_INFO,
    c.KEY_PARAMETERS,
    REGEXP_REPLACE(c.KEY_PARAMETERS:campos:"CPF"::string, '\\D','') AS crivo_cpf_digits
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS c
),

crivo_candidates AS (
  SELECT
    pa.c1_entity_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'cpf_primary' AS crivo_match_stage,
    1 AS crivo_rank,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, pa.c1_created_at) AS crivo_minutes_from_c1
  FROM pa_legacy pa
  JOIN params p ON TRUE
  JOIN crivo_base cb
    ON cb.crivo_cpf_digits = pa.cpf_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('hour', -p.crivo_primary_hours, pa.c1_created_at)
                                    AND DATEADD('hour',  p.crivo_primary_hours, pa.c1_created_at)
  WHERE pa.cpf_digits IS NOT NULL

  UNION ALL

  SELECT
    pa.c1_entity_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'cpf_fallback_15d' AS crivo_match_stage,
    2 AS crivo_rank,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, pa.c1_created_at) AS crivo_minutes_from_c1
  FROM pa_legacy pa
  JOIN params p ON TRUE
  JOIN crivo_base cb
    ON cb.crivo_cpf_digits = pa.cpf_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('day', -p.crivo_cache_days, pa.c1_created_at)
                                    AND pa.c1_created_at
  WHERE pa.cpf_digits IS NOT NULL

  UNION ALL

  SELECT
    pa.c1_entity_id,
    cb.CRIVO_CHECK_ID,
    cb.CRIVO_CHECK_CREATED_AT,
    cb.POLITICA,
    'cpf_fallback_180d' AS crivo_match_stage,
    3 AS crivo_rank,
    DATEDIFF('minute', cb.CRIVO_CHECK_CREATED_AT, pa.c1_created_at) AS crivo_minutes_from_c1
  FROM pa_legacy pa
  JOIN params p ON TRUE
  JOIN crivo_base cb
    ON cb.crivo_cpf_digits = pa.cpf_digits
   AND cb.CRIVO_CHECK_CREATED_AT BETWEEN DATEADD('day', -p.crivo_cap_days, pa.c1_created_at)
                                    AND pa.c1_created_at
  WHERE pa.cpf_digits IS NOT NULL
),

crivo_best AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.c1_entity_id
      ORDER BY c.crivo_rank ASC, ABS(c.crivo_minutes_from_c1) ASC, c.CRIVO_CHECK_CREATED_AT DESC, c.CRIVO_CHECK_ID DESC
    ) AS rn_best
  FROM crivo_candidates c
),

crivo_best_1 AS (
  SELECT *
  FROM crivo_best
  WHERE rn_best = 1
),

crivo_features_campos AS (
  SELECT
    b.c1_entity_id,
    MAX(IFF(f.value:nome::string = 'PEFIN Serasa',    TRY_TO_NUMBER(f.value:valor::string), NULL)) AS crivo_pefin_serasa,
    MAX(IFF(f.value:nome::string = 'REFIN Serasa',    TRY_TO_NUMBER(f.value:valor::string), NULL)) AS crivo_refin_serasa,
    MAX(IFF(f.value:nome::string = 'Protesto Serasa', TRY_TO_NUMBER(f.value:valor::string), NULL)) AS crivo_protesto_serasa,
    MAX(IFF(f.value:nome::string = 'Score Serasa', IFF(TRY_TO_NUMBER(f.value:valor::string) > 0, TRY_TO_NUMBER(f.value:valor::string), NULL), NULL)) AS crivo_score_serasa
  FROM crivo_best_1 b
  JOIN crivo_base cb
    ON cb.CRIVO_CHECK_ID = b.CRIVO_CHECK_ID
  , LATERAL FLATTEN(input => cb.BUREAU_CHECK_INFO:campos) f
  GROUP BY 1
),

/* Gênero via Crivo (DataBusca PF) — mesma semântica do core de credit_simulations: 1->M, 2->F */
crivo_databusca_pf_raw AS (
  SELECT
    b.c1_entity_id,
    MAX(
      IFF(
        LOWER(p.value:nome::string) LIKE '%sexo%',
        NULLIF(TRIM(p.value:valor::string), ''),
        NULL
      )
    ) AS crivo_sexo_codigo_raw
  FROM crivo_best_1 b
  JOIN crivo_base cb
    ON cb.CRIVO_CHECK_ID = b.CRIVO_CHECK_ID
  , LATERAL FLATTEN(input => cb.BUREAU_CHECK_INFO:drivers) d
  , LATERAL FLATTEN(input => d.value:produtos:"api DataBusca - Consulta Dados Pessoa - PF") p
  GROUP BY 1
),

crivo_features_databusca_pf AS (
  SELECT
    c1_entity_id,
    crivo_sexo_codigo_raw,
    CASE
      WHEN crivo_sexo_codigo_raw = '1' THEN 'M'
      WHEN crivo_sexo_codigo_raw = '2' THEN 'F'
      ELSE NULL
    END AS crivo_gender
  FROM crivo_databusca_pf_raw
),

/* ===== N8N (motor) via CEI: features agregadas recentes (2025-10+) =====
   Guardrails:
     - usar apenas como fallback (não sobrescrever PACC/Serasa/hash/raw)
     - não persistir CPF cru; só usar para checagens internas se necessário
*/
cei_n8n_latest AS (
  SELECT
    ENGINEABLE_ID AS c1_entity_id,
    CREDIT_ENGINE_CONSULTATION_CREATED_AT AS created_at,
    CREDIT_ENGINE_CONSULTATION_UPDATED_AT AS updated_at,
    TRY_PARSE_JSON(DATA) AS j
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION
  WHERE LOWER(ENGINEABLE_TYPE) IN ('pre_analysis','preanalysis','pre-analysis')
    AND SOURCE ILIKE '%n8n%'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ENGINEABLE_ID
    ORDER BY CREDIT_ENGINE_CONSULTATION_CREATED_AT DESC, CREDIT_ENGINE_CONSULTATION_UPDATED_AT DESC
  ) = 1
),

cei_n8n_features AS (
  SELECT
    n.c1_entity_id,
    n.created_at AS n8n_created_at,
    /* scores */
    TRY_TO_NUMBER(n.j:scoreSerasa::string) AS n8n_score_serasa,
    TRY_TO_NUMBER(n.j:scoreBvs::string)    AS n8n_score_bvs,

    /* negativação (counts agregados) */
    TRY_TO_NUMBER(n.j:pefinSerasa::string)    AS n8n_pefin_serasa,
    TRY_TO_NUMBER(n.j:refinSerasa::string)    AS n8n_refin_serasa,
    TRY_TO_NUMBER(n.j:protestoSerasa::string) AS n8n_protesto_serasa,
    TRY_TO_NUMBER(n.j:protestoBvs::string)    AS n8n_protesto_bvs,

    /* renda (pode vir como string PT-BR) */
    TRY_TO_NUMBER(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(NULLIF(TRIM(n.j:rendaSerasa::string), ''), '\"', ''),
              'R$', ''
            ),
            '.',''
          ),
          ',','.'
        ),
        ' ',''
      )
    ) AS n8n_renda_serasa,

    /* demografia parcial */
    NULLIF(TRIM(n.j:cepProponente::string), '') AS n8n_zipcode,
    COALESCE(
      TRY_TO_DATE(NULLIF(TRIM(n.j:dataNascimentoBvs::string), ''), 'DD/MM/YYYY'),
      TRY_TO_DATE(NULLIF(TRIM(n.j:dataNascimentoBvs::string), ''), 'YYYY-MM-DD'),
      TRY_TO_DATE(NULLIF(TRIM(n.j:dataNascimentoBvs::string), ''), 'DDMMYYYY')
    ) AS n8n_birthdate
  FROM cei_n8n_latest n
  WHERE TYPEOF(n.j) = 'OBJECT'
),

/* Cidade/UF/CEP/birthdate via INCREMENTAL_SENSITIVE_DATA_API por CPF (legado tem CPF via SOURCE_PRE_ANALYSIS_API).
   - Semântica: é “last resort” (pode estar desatualizado vs bureaus), mas aumenta interoperabilidade histórica. */
sensitive_by_cpf AS (
  SELECT
    REGEXP_REPLACE(s.cpf, '\\D','') AS cpf_digits,
    s.birthdate AS sensitive_birthdate,
    s.city      AS sensitive_city,
    s.state     AS sensitive_state,
    s.zipcode   AS sensitive_zipcode
  FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API s
  WHERE s.cpf IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY REGEXP_REPLACE(s.cpf, '\\D','')
    ORDER BY
      /* preferir registro “mais completo” para interoperabilidade */
      (IFF(s.city IS NOT NULL, 0, 1)
       + IFF(s.zipcode IS NOT NULL, 0, 1)
       + IFF(s.birthdate IS NOT NULL, 0, 1)) ASC,
      s.id DESC
  ) = 1
),

/* ===== Montagem do legado: seleção simples e transparente ===== */
part_b_enriched AS (
  SELECT
    pa.c1_entity_type,
    pa.c1_entity_id,
    pa.credit_simulation_id,
    pa.credit_lead_id,
    pa.clinic_id,
    pa.c1_created_at,
    pa.c1_state,
    pa.risk_capim,
    pa.risk_capim_subclass,
    pa.rejection_reason,
    pa.c1_amount,
    pa.counter_proposal_amount,
    pa.maximum_term_available,
    pa.minimum_term_available,
    pa.proposal_interest,
    pa.has_request,
    /* comparabilidade de estado/outcome/valores
       Política (legado) baseada em coerência observável:
         - Reprovado se houver rejection_reason (fonte de verdade do "por quê").
         - Aprovado se estado for eligible* E houver sinal de financing (termos/IR ou estimativa).
         - 'expired' no legado é majoritariamente terminal com rejection_reason e sem financing; tratar como não aprovado.
    */
    pa.c1_state AS c1_state_raw,
    IFF(
      fin.financing_total_debt_min IS NOT NULL
      OR fin.financing_term_min IS NOT NULL
      OR fin.financing_term_max IS NOT NULL,
      TRUE, FALSE
    ) AS legacy_has_financing_signal,
    CASE
      WHEN pa.rejection_reason IS NOT NULL THEN FALSE
      WHEN pa.c1_state = 'rejected' THEN FALSE
      WHEN pa.c1_state = 'expired' THEN FALSE
      WHEN pa.c1_state IN ('eligible','eligible_with_counter_proposal')
           AND (fin.financing_total_debt_min IS NOT NULL OR fin.financing_term_min IS NOT NULL OR fin.financing_term_max IS NOT NULL)
        THEN TRUE
      ELSE NULL
    END AS c1_was_approved,
    CASE
      WHEN pa.rejection_reason IS NOT NULL THEN 'not_approved'
      WHEN pa.c1_state = 'rejected' THEN 'not_approved'
      WHEN pa.c1_state = 'expired' THEN 'not_approved'
      WHEN pa.c1_state IN ('eligible','eligible_with_counter_proposal')
           AND (fin.financing_total_debt_min IS NOT NULL OR fin.financing_term_min IS NOT NULL OR fin.financing_term_max IS NOT NULL)
        THEN 'approved'
      ELSE 'unknown'
    END AS c1_outcome_bucket,
    pa.rejection_reason AS c1_rejection_reason,
    IFF(pa.c1_state='eligible' AND pa.rejection_reason IS NOT NULL, TRUE, FALSE) AS c1_can_retry_with_financial_responsible,
    pa.c1_amount AS c1_requested_amount,
    IFF(
      /* aprovado no legado: se houver contra-proposta, ela é o "permitido"; senão assume-se aprovado no valor solicitado */
      (pa.rejection_reason IS NULL)
      AND pa.c1_state IN ('eligible','eligible_with_counter_proposal')
      AND (fin.financing_total_debt_min IS NOT NULL OR fin.financing_term_min IS NOT NULL OR fin.financing_term_max IS NOT NULL),
      COALESCE(
        IFF(pa.c1_state='eligible_with_counter_proposal', pa.counter_proposal_amount, NULL),
        pa.c1_amount
      )::FLOAT,
      NULL::FLOAT
    ) AS c1_approved_amount,
    fin.financing_term_min,
    fin.financing_term_max,
    fin.financing_installment_value_min,
    fin.financing_installment_value_max,
    fin.financing_total_debt_min,
    fin.financing_total_debt_max,

    /* Eixo 1: usar legado como base; complementar com SERASA new se existir */
    COALESCE(sn.serasa_new_birthdate, pa.legacy_birthdate, n8n.n8n_birthdate, sbc.sensitive_birthdate) AS borrower_birthdate,
    CASE
      WHEN sn.serasa_new_birthdate IS NOT NULL THEN 'serasa_new_registration'
      WHEN pa.legacy_birthdate IS NOT NULL THEN 'source_pre_analysis_api'
      WHEN n8n.n8n_birthdate IS NOT NULL THEN 'cei_n8n_birthdate_bvs'
      WHEN sbc.sensitive_birthdate IS NOT NULL THEN 'sensitive_by_cpf_last_resort'
      ELSE NULL
    END AS borrower_birthdate_source,
    COALESCE(sn.serasa_new_gender, cg.crivo_gender) AS borrower_gender,
    CASE
      WHEN sn.serasa_new_gender IS NOT NULL THEN 'serasa_new_registration'
      WHEN cg.crivo_gender IS NOT NULL THEN 'crivo_databusca_pf_sexo_codigo'
      ELSE NULL
    END AS borrower_gender_source,
    COALESCE(sn.serasa_new_zipcode, pa.legacy_zipcode, n8n.n8n_zipcode, sbc.sensitive_zipcode) AS borrower_zipcode,
    CASE
      WHEN sn.serasa_new_zipcode IS NOT NULL THEN 'serasa_new_registration'
      WHEN pa.legacy_zipcode IS NOT NULL THEN 'source_pre_analysis_api'
      WHEN n8n.n8n_zipcode IS NOT NULL THEN 'cei_n8n_zipcode'
      WHEN sbc.sensitive_zipcode IS NOT NULL THEN 'sensitive_by_cpf_last_resort'
      ELSE NULL
    END AS borrower_zipcode_source,
    sbc.sensitive_city AS borrower_city,
    COALESCE(
      IFF(sbc.sensitive_state IS NOT NULL AND LENGTH(TRIM(sbc.sensitive_state)) = 2, UPPER(TRIM(sbc.sensitive_state)), NULL),
      IFF(pa.legacy_state IS NOT NULL AND LENGTH(TRIM(pa.legacy_state)) = 2, UPPER(TRIM(pa.legacy_state)), NULL)
    ) AS borrower_state,
    NULL::TEXT AS borrower_registration_status,
    NULL::TEXT AS borrower_registration_status_source,
    NULL::DATE AS borrower_registration_status_date,
    NULL::BOOLEAN AS borrower_has_phone,
    NULL::TEXT AS borrower_has_phone_source,
    NULL::BOOLEAN AS borrower_has_address,
    NULL::TEXT AS borrower_has_address_source,
    /* evidência simplificada */
    CASE
      WHEN sn.serasa_new_birthdate IS NOT NULL OR sn.serasa_new_zipcode IS NOT NULL OR sn.serasa_new_gender IS NOT NULL THEN 'serasa_new_registration'
      WHEN pa.legacy_birthdate IS NOT NULL OR pa.legacy_zipcode IS NOT NULL OR (pa.legacy_state IS NOT NULL AND LENGTH(TRIM(pa.legacy_state)) = 2) THEN 'source_pre_analysis_api'
      WHEN sbc.sensitive_city IS NOT NULL OR sbc.sensitive_zipcode IS NOT NULL OR sbc.sensitive_birthdate IS NOT NULL THEN 'sensitive_by_cpf_last_resort'
      WHEN cg.crivo_gender IS NOT NULL THEN 'crivo_databusca_pf_sexo_codigo'
      ELSE NULL
    END AS cadastro_evidence_source,
    NULL::TEXT AS cadastro_evidence_match_stage,
    NULL::NUMBER AS cadastro_evidence_minutes_from_cs,

    /* Eixo 2: preferir SERASA new (se houver) → senão Crivo (counts) → senão SERASA old */
    COALESCE(pl.pacc_serasa_pefin_count, sh.serasa_pefin, sn.serasa_new_pefin_count, n8n.n8n_pefin_serasa, cf.crivo_pefin_serasa) AS pefin_count,
    COALESCE(pl.pacc_serasa_refin_count, sh.serasa_refin, sn.serasa_new_refin_count, n8n.n8n_refin_serasa, cf.crivo_refin_serasa, so.serasa_old_b357_occurrences_count) AS refin_count,
    COALESCE(
      pl.pacc_serasa_protest_count,
      sh.serasa_protest,
      sn.serasa_new_notary_count,
      n8n.n8n_protesto_serasa,
      /* n8n também pode trazer protestos BVS; usar como último recurso antes de cair em serasa_old/crivo */
      n8n.n8n_protesto_bvs,
      cf.crivo_protesto_serasa,
      so.serasa_old_b361_occurrences_count
    ) AS protesto_count,
    /* valores monetários:
       - prioridade: SERASA new (balances)
       - fallback: BVS SCPC totals (dívida/protesto) quando não houver SERASA new balances */
    COALESCE(
      (sn.serasa_new_pefin_balance)::FLOAT,
      IFF(sn.serasa_new_pefin_balance IS NULL AND sn.serasa_new_refin_balance IS NULL AND sn.serasa_new_notary_balance IS NULL,
          (bvs_scpc.bvs_total_debt)::FLOAT,
          NULL)
    ) AS pefin_value,
    (sn.serasa_new_refin_balance)::FLOAT AS refin_value,
    COALESCE(
      (sn.serasa_new_notary_balance)::FLOAT,
      IFF(sn.serasa_new_pefin_balance IS NULL AND sn.serasa_new_refin_balance IS NULL AND sn.serasa_new_notary_balance IS NULL,
          (bvs_scpc.bvs_total_protest)::FLOAT,
          NULL)
    ) AS protesto_value,
    IFF(
      /* caso 1: temos balances SERASA new */
      sn.serasa_new_pefin_balance IS NOT NULL
      OR sn.serasa_new_refin_balance IS NOT NULL
      OR sn.serasa_new_notary_balance IS NOT NULL,
      COALESCE(sn.serasa_new_pefin_balance, 0) + COALESCE(sn.serasa_new_refin_balance, 0) + COALESCE(sn.serasa_new_notary_balance, 0),
      /* caso 2: fallback BVS SCPC totals */
      IFF(
        bvs_scpc.bvs_total_debt IS NOT NULL OR bvs_scpc.bvs_total_protest IS NOT NULL,
        COALESCE(bvs_scpc.bvs_total_debt, 0) + COALESCE(bvs_scpc.bvs_total_protest, 0),
        NULL
      )
    ) AS total_negative_value,
    CASE
      WHEN pl.c1_entity_id IS NOT NULL
           AND (pl.pacc_serasa_pefin_count IS NOT NULL OR pl.pacc_serasa_refin_count IS NOT NULL OR pl.pacc_serasa_protest_count IS NOT NULL)
        THEN 'pre_analysis_credit_check'
      WHEN sh.c1_entity_id IS NOT NULL
           AND (sh.serasa_pefin IS NOT NULL OR sh.serasa_refin IS NOT NULL OR sh.serasa_protest IS NOT NULL)
        THEN 'source_credit_checks_api_serasa'
      WHEN sn.c1_entity_id IS NOT NULL AND (sn.serasa_new_pefin_count IS NOT NULL OR sn.serasa_new_refin_count IS NOT NULL OR sn.serasa_new_notary_count IS NOT NULL) THEN 'serasa_new_negativeData_summary'
      WHEN n8n.c1_entity_id IS NOT NULL
           AND (
             n8n.n8n_pefin_serasa IS NOT NULL
             OR n8n.n8n_refin_serasa IS NOT NULL
             OR n8n.n8n_protesto_serasa IS NOT NULL
             OR n8n.n8n_protesto_bvs IS NOT NULL
           )
        THEN 'cei_n8n_counts'
      WHEN cf.c1_entity_id IS NOT NULL THEN 'crivo_bureau_campos'
      WHEN so.c1_entity_id IS NOT NULL THEN 'serasa_old_bcodes_summary'
      WHEN bvs_scpc.c1_entity_id IS NOT NULL
           AND (bvs_scpc.bvs_total_debt IS NOT NULL OR bvs_scpc.bvs_total_protest IS NOT NULL)
        THEN 'source_credit_checks_api_boa_vista_scpc_net'
      ELSE NULL
    END AS negativacao_source,
    NULL::TEXT AS negativacao_evidence_match_stage,
    NULL::NUMBER AS negativacao_evidence_minutes_from_cs,

    /* Eixo 3 */
    NULL::FLOAT AS sensitive_monthly_income,
    COALESCE(pl.pacc_serasa_presumed_income, si.serasa_income_estimated, sh.serasa_presumed_income, n8n.n8n_renda_serasa) AS serasa_income_estimated,
    NULL::FLOAT AS crivo_renda_presumida,
    COALESCE(pl.pacc_serasa_presumed_income, si.serasa_income_estimated, sh.serasa_presumed_income, n8n.n8n_renda_serasa) AS income_estimated,
    CASE
      WHEN pl.pacc_serasa_presumed_income IS NOT NULL THEN 'pre_analysis_credit_check_serasa_presumed_income'
      WHEN si.serasa_income_estimated IS NOT NULL THEN 'serasa_check_income_only_hrp_score_cents'
      WHEN sh.serasa_presumed_income IS NOT NULL THEN 'source_credit_checks_api_serasa_presumed_income'
      WHEN n8n.n8n_renda_serasa IS NOT NULL THEN 'cei_n8n_renda_serasa'
      ELSE NULL
    END AS income_estimated_source,
    NULL::TEXT AS income_estimated_evidence_match_stage,
    NULL::NUMBER AS income_estimated_evidence_minutes_from_cs,
    COALESCE(sr.scr_qtd_de_operacoes, NULL)::NUMBER AS scr_operations_count,
    NULL::NUMBER AS scr_vencimentos_count,
    NULL::FLOAT  AS scr_sum_valor_raw,
    CASE
      WHEN bi.bacen_internal_score IS NOT NULL THEN 'bacen_internal_score'
      WHEN pl.pacc_serasa_presumed_income IS NOT NULL THEN 'pre_analysis_credit_check_serasa_presumed_income'
      WHEN si.serasa_income_estimated IS NOT NULL THEN 'serasa_income_estimated'
      WHEN sh.serasa_presumed_income IS NOT NULL THEN 'source_credit_checks_api_serasa_presumed_income'
      WHEN sr.scr_qtd_de_operacoes IS NOT NULL THEN 'source_credit_checks_api_scr_report'
      WHEN n8n.n8n_renda_serasa IS NOT NULL THEN 'cei_n8n_renda_serasa'
      ELSE NULL
    END AS renda_proxies_source,
    NULL::TEXT AS renda_proxies_evidence_match_stage,
    NULL::NUMBER AS renda_proxies_evidence_minutes_from_cs,

    /* Eixo 4: scores por bureau */
    COALESCE(pl.pacc_boa_vista_score, bvsh.bvs_positive_score, n8n.n8n_score_bvs, bvs.boa_vista_score) AS boa_vista_score,
    ROUND(COALESCE(pl.pacc_serasa_score, sh.serasa_positive_score, n8n.n8n_score_serasa, so.serasa_old_score_b280, cf.crivo_score_serasa), 0)::NUMBER AS serasa_score,
    CASE
      WHEN pl.pacc_serasa_score IS NOT NULL THEN 'pre_analysis_credit_check'
      WHEN sh.serasa_positive_score IS NOT NULL THEN 'source_credit_checks_api_serasa'
      WHEN n8n.n8n_score_serasa IS NOT NULL THEN 'cei_n8n_score_serasa'
      WHEN so.serasa_old_score_b280 IS NOT NULL THEN 'serasa_old_b280'
      WHEN cf.crivo_score_serasa IS NOT NULL THEN 'crivo_bureau_campos'
      ELSE NULL
    END AS serasa_score_source,
    bi.bacen_internal_score,
    so.serasa_old_score_range_name,
    so.serasa_old_delinquency_prob_pct,

    /* Crivo lineage (legado via CPF+tempo) */
    cb.CRIVO_CHECK_ID AS crivo_check_id_resolved,
    cb.crivo_match_stage AS crivo_resolution_stage,
    cb.crivo_minutes_from_c1 AS crivo_minutes_from_cs
  FROM pa_legacy pa
  LEFT JOIN pa_legacy_financing_features fin
    ON fin.c1_entity_id = pa.c1_entity_id
  LEFT JOIN pacc_legacy pl
    ON pl.c1_entity_id = pa.c1_entity_id
  LEFT JOIN serasa_hash_15d sh
    ON sh.c1_entity_id = pa.c1_entity_id
  LEFT JOIN bvs_score_pf_hash_15d bvsh
    ON bvsh.c1_entity_id = pa.c1_entity_id
  LEFT JOIN bvs_scpc_hash_15d bvs_scpc
    ON bvs_scpc.c1_entity_id = pa.c1_entity_id
  LEFT JOIN scr_report_hash_15d sr
    ON sr.c1_entity_id = pa.c1_entity_id
  LEFT JOIN serasa_old_features so
    ON so.c1_entity_id = pa.c1_entity_id
  LEFT JOIN serasa_new_features sn
    ON sn.c1_entity_id = pa.c1_entity_id
  LEFT JOIN serasa_income_only_features si
    ON si.c1_entity_id = pa.c1_entity_id
  LEFT JOIN boa_vista_score_pf_features bvs
    ON bvs.c1_entity_id = pa.c1_entity_id
  LEFT JOIN bacen_internal_features bi
    ON bi.c1_entity_id = pa.c1_entity_id
  LEFT JOIN crivo_best_1 cb
    ON cb.c1_entity_id = pa.c1_entity_id
  LEFT JOIN crivo_features_campos cf
    ON cf.c1_entity_id = pa.c1_entity_id
  LEFT JOIN crivo_features_databusca_pf cg
    ON cg.c1_entity_id = pa.c1_entity_id
  LEFT JOIN cei_n8n_features n8n
    ON n8n.c1_entity_id = pa.c1_entity_id
  LEFT JOIN sensitive_by_cpf sbc
    ON sbc.cpf_digits = pa.cpf_digits
),

final AS (
  SELECT * FROM part_a_reused
  UNION ALL
  SELECT * FROM part_b_enriched
)

SELECT *
FROM final
;


