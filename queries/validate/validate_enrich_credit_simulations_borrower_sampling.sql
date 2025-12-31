/*
  Validação 100% SQL (Snowflake) — maximização de credit checks (match leniente auditável)

  Como usar:
    - Ajuste params_user (period_start, period_end, sample_n)
    - Rode no Snowflake Worksheet

  Saída:
    - Métricas "tidy" com quebras por state/outcome e estágios de match.
*/

WITH params_user AS (
  SELECT
    '2025-09-01'::TIMESTAMP_NTZ AS period_start,
    '2025-10-01'::TIMESTAMP_NTZ AS period_end,
    20000::INT AS sample_n,
    15::INT AS fallback_15d,
    180::INT AS fallback_180d
),

cs_base AS (
  SELECT
    cs.id AS credit_simulation_id,
    cs.state,
    cs.rejection_reason,
    cs.created_at AS cs_created_at,
    cs.approved_at,
    (cs.permitted_amount / 100.0)::FLOAT AS permitted_amount,
    cs.patient_id,
    cs.financial_responsible_id
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  JOIN params_user pu ON TRUE
  WHERE cs.created_at >= pu.period_start
    AND cs.created_at <  pu.period_end
  QUALIFY ROW_NUMBER() OVER (ORDER BY UNIFORM(0, 1000000, RANDOM())) <= (SELECT sample_n FROM params_user)
),

cs_people AS (
  SELECT
    b.*,
    IFF(
      b.financial_responsible_id IS NOT NULL AND b.financial_responsible_id <> b.patient_id,
      b.financial_responsible_id,
      b.patient_id
    ) AS borrower_person_id
  FROM cs_base b
),

cs_cpf AS (
  SELECT
    b.*,
    IFF(
      b.financial_responsible_id IS NOT NULL AND b.financial_responsible_id <> b.patient_id,
      fr.cpf,
      p.cpf
    ) AS cpf_effective,
    REGEXP_REPLACE(
      IFF(
        b.financial_responsible_id IS NOT NULL AND b.financial_responsible_id <> b.patient_id,
        fr.cpf,
        p.cpf
      ),
      '\\D',''
    ) AS cpf_effective_digits,
    IFF(
      b.approved_at IS NOT NULL
      OR (
        b.permitted_amount > 0
        AND b.state NOT IN ('rejected','error','errored')
      ),
      TRUE, FALSE
    ) AS credit_simulation_was_approved,
    IFF(
      IFF(
        b.approved_at IS NOT NULL
        OR (
          b.permitted_amount > 0
          AND b.state NOT IN ('rejected','error','errored')
        ),
        TRUE, FALSE
      ),
      'approved','not_approved'
    ) AS approval_bucket
  FROM cs_people b
  LEFT JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API p
    ON p.id = b.patient_id
  LEFT JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API fr
    ON fr.id = b.financial_responsible_id
),

cc_matches AS (
  SELECT
    cs.credit_simulation_id,
    cs.state,
    IFF(cs.credit_simulation_was_approved, 'approved', 'not_approved') AS approval_bucket,
    cs.rejection_reason,
    cc.id AS credit_check_id,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data AS credit_check_data,
    cc.created_at AS credit_check_created_at,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS minutes_from_cs,
    'strict_primary_1h' AS match_stage,
    0 AS leniency_rank
  FROM cs_cpf cs
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('hour', -1, cs.cs_created_at) AND DATEADD('hour', 1, cs.cs_created_at)

  UNION ALL

  SELECT
    cs.credit_simulation_id,
    cs.state,
    IFF(cs.credit_simulation_was_approved, 'approved', 'not_approved') AS approval_bucket,
    cs.rejection_reason,
    cc.id AS credit_check_id,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data AS credit_check_data,
    cc.created_at AS credit_check_created_at,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS minutes_from_cs,
    'lenient_primary_24h' AS match_stage,
    1 AS leniency_rank
  FROM cs_cpf cs
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('hour', -24, cs.cs_created_at) AND DATEADD('hour', 24, cs.cs_created_at)

  UNION ALL

  SELECT
    cs.credit_simulation_id,
    cs.state,
    IFF(cs.credit_simulation_was_approved, 'approved', 'not_approved') AS approval_bucket,
    cs.rejection_reason,
    cc.id AS credit_check_id,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data AS credit_check_data,
    cc.created_at AS credit_check_created_at,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS minutes_from_cs,
    'lenient_fallback_15d' AS match_stage,
    2 AS leniency_rank
  FROM cs_cpf cs
  JOIN params_user p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('day', -p.fallback_15d, cs.cs_created_at) AND cs.cs_created_at

  UNION ALL

  SELECT
    cs.credit_simulation_id,
    cs.state,
    IFF(cs.credit_simulation_was_approved, 'approved', 'not_approved') AS approval_bucket,
    cs.rejection_reason,
    cc.id AS credit_check_id,
    cc.source,
    cc.kind,
    cc.new_data_format,
    cc.data AS credit_check_data,
    cc.created_at AS credit_check_created_at,
    DATEDIFF('minute', cc.created_at, cs.cs_created_at) AS minutes_from_cs,
    'lenient_fallback_180d' AS match_stage,
    3 AS leniency_rank
  FROM cs_cpf cs
  JOIN params_user p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = cs.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('day', -p.fallback_180d, cs.cs_created_at) AND cs.cs_created_at
),

cc_best_any AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id
      ORDER BY m.leniency_rank ASC, ABS(m.minutes_from_cs) ASC, m.credit_check_created_at DESC, m.credit_check_id DESC
    ) AS rn_best_any
  FROM cc_matches m
),

cc_best_serasa AS (
  SELECT
    m.*,
    ROW_NUMBER() OVER (
      PARTITION BY m.credit_simulation_id
      ORDER BY m.leniency_rank ASC, ABS(m.minutes_from_cs) ASC, m.credit_check_created_at DESC, m.credit_check_id DESC
    ) AS rn_best_serasa
  FROM cc_matches m
  WHERE m.source = 'serasa'
),

cc_best_serasa_1 AS (
  SELECT *
  FROM cc_best_serasa
  WHERE rn_best_serasa = 1
),

/* ============================
   Diagnóstico do score SERASA via credit check (novo/antigo)
   ============================ */
serasa_new_reports AS (
  SELECT
    s.credit_simulation_id,
    s.kind AS serasa_cc_kind,
    s.credit_check_data,
    r.value AS report,
    r.index AS report_index
  FROM cc_best_serasa_1 s
  , LATERAL FLATTEN(input => s.credit_check_data:reports) r
  WHERE COALESCE(s.new_data_format, FALSE) = TRUE
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

serasa_new_score_diag AS (
  SELECT
    s.credit_simulation_id,
    s.kind AS serasa_cc_kind,
    br.report:reportName::string AS serasa_new_report_name,
    COALESCE(
      TRY_TO_NUMBER(s.credit_check_data:score::string),
      TRY_TO_NUMBER(s.credit_check_data:data:score::string),
      TRY_TO_NUMBER(br.report:score::string)
    ) AS serasa_cc_score,
    CASE
      WHEN TRY_TO_NUMBER(s.credit_check_data:score::string) IS NOT NULL THEN 'serasa_new_top_level'
      WHEN TRY_TO_NUMBER(s.credit_check_data:data:score::string) IS NOT NULL THEN 'serasa_new_data_block'
      WHEN TRY_TO_NUMBER(br.report:score::string) IS NOT NULL THEN 'serasa_new_report_block'
      ELSE NULL
    END AS serasa_cc_score_source_detail
  FROM cc_best_serasa_1 s
  LEFT JOIN (
    SELECT *
    FROM serasa_new_best_report
    WHERE rn_best_report = 1
  ) br
    ON br.credit_simulation_id = s.credit_simulation_id
  WHERE COALESCE(s.new_data_format, FALSE) = TRUE
),

serasa_old_score_diag AS (
  SELECT
    s.credit_simulation_id,
    s.kind AS serasa_cc_kind,
    MAX(TRY_TO_NUMBER(f.value:"B280":score::string)) AS serasa_cc_score
  FROM cc_best_serasa_1 s
  , LATERAL FLATTEN(input => s.credit_check_data) f
  WHERE COALESCE(s.new_data_format, FALSE) = FALSE
  GROUP BY 1,2
),

serasa_cc_score AS (
  SELECT
    cs.credit_simulation_id,
    CASE
      WHEN s.credit_check_id IS NULL THEN NULL
      WHEN COALESCE(s.new_data_format, FALSE) = TRUE THEN 'new'
      ELSE 'old'
    END AS serasa_cc_format,
    COALESCE(ns.serasa_cc_kind, os.serasa_cc_kind, s.kind) AS serasa_cc_kind,
    COALESCE(ns.serasa_new_report_name, 'NULL') AS serasa_new_report_name,
    COALESCE(ns.serasa_cc_score, os.serasa_cc_score) AS serasa_cc_score,
    CASE
      WHEN ns.serasa_cc_score IS NOT NULL THEN COALESCE(ns.serasa_cc_score_source_detail, 'serasa_new_unknown_path')
      WHEN os.serasa_cc_score IS NOT NULL THEN 'serasa_old_b280'
      ELSE NULL
    END AS serasa_cc_score_source
  FROM cs_cpf cs
  LEFT JOIN cc_best_serasa_1 s
    ON s.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_new_score_diag ns
    ON ns.credit_simulation_id = cs.credit_simulation_id
  LEFT JOIN serasa_old_score_diag os
    ON os.credit_simulation_id = cs.credit_simulation_id
),

agg_serasa_cc_score_by_kind AS (
  SELECT
    serasa_cc_format,
    serasa_cc_kind,
    serasa_new_report_name,
    COUNT(*) AS n_simulations,
    COUNT_IF(serasa_cc_format IS NOT NULL) AS n_with_serasa_cc,
    COUNT_IF(serasa_cc_score IS NOT NULL) AS n_with_serasa_cc_score,
    COUNT_IF(serasa_cc_format IS NOT NULL AND serasa_cc_score IS NULL) AS n_serasa_cc_no_score
  FROM serasa_cc_score
  GROUP BY 1,2,3
),

/* ============================
   Métricas do dataset materializado (fonte da verdade pós-CTAS)
   ============================ */
enriched_sample AS (
  SELECT
    e.*
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1 e
  JOIN params_user pu ON TRUE
  WHERE e.cs_created_at >= pu.period_start
    AND e.cs_created_at <  pu.period_end
  QUALIFY ROW_NUMBER() OVER (ORDER BY UNIFORM(0, 1000000, RANDOM())) <= (SELECT sample_n FROM params_user)
),

agg_serasa_score_source_enriched AS (
  SELECT
    COALESCE(serasa_score_source, 'NULL') AS serasa_score_source,
    COUNT(*) AS n_simulations,
    COUNT_IF(serasa_score IS NOT NULL) AS n_with_serasa_score
  FROM enriched_sample
  GROUP BY 1
),

axis_source_dist_enriched AS (
  SELECT
    'cadastro' AS axis,
    COALESCE(cadastro_evidence_source, 'NULL') AS source,
    COUNT(*) AS n
  FROM enriched_sample
  GROUP BY 1,2

  UNION ALL

  SELECT
    'negativacao' AS axis,
    COALESCE(negativacao_source, 'NULL') AS source,
    COUNT(*) AS n
  FROM enriched_sample
  GROUP BY 1,2

  UNION ALL

  SELECT
    'renda_proxies' AS axis,
    COALESCE(renda_proxies_source, 'NULL') AS source,
    COUNT(*) AS n
  FROM enriched_sample
  GROUP BY 1,2
),

axis_totals_enriched AS (
  SELECT axis, SUM(n) AS n_total
  FROM axis_source_dist_enriched
  GROUP BY 1
),

axis_source_pct_enriched AS (
  SELECT
    d.axis,
    d.source,
    d.n,
    (d.n / NULLIF(t.n_total, 0))::FLOAT AS pct_of_simulations
  FROM axis_source_dist_enriched d
  JOIN axis_totals_enriched t
    ON t.axis = d.axis
),

serasa_strict_flags AS (
  SELECT
    credit_simulation_id,
    MAX(IFF(source = 'serasa'
            AND leniency_rank = 0
            AND (
              (COALESCE(new_data_format, FALSE) = TRUE AND kind = 'check_score_without_income')
              OR COALESCE(new_data_format, FALSE) = FALSE
            ),
            1, 0)) AS has_serasa_strict_negative_payload,
    MAX(IFF(source = 'serasa'
            AND leniency_rank = 0
            AND COALESCE(new_data_format, FALSE) = TRUE
            AND kind = 'check_income_only',
            1, 0)) AS has_serasa_strict_income_only
  FROM cc_matches
  GROUP BY 1
),

masking_enriched AS (
  SELECT
    e.credit_simulation_id,
    IFF(
      e.negativacao_source = 'crivo_bureau_campos'
      AND e.negativacao_evidence_match_stage IN ('cpf_fallback_15d','cpf_fallback_180d')
      AND COALESCE(f.has_serasa_strict_negative_payload, 0) = 1,
      1, 0
    ) AS neg_masked_crivo_lenient_over_serasa_strict
  FROM enriched_sample e
  LEFT JOIN serasa_strict_flags f
    ON f.credit_simulation_id = e.credit_simulation_id
),

agg_masking AS (
  SELECT
    COUNT(*) AS n_simulations,
    COUNT_IF(neg_masked_crivo_lenient_over_serasa_strict = 1) AS n_neg_masked
  FROM masking_enriched
),

agg_serasa_score_sanity AS (
  SELECT
    COUNT(*) AS n_simulations,
    COUNT_IF(serasa_score IS NOT NULL) AS n_with_serasa_score,
    COUNT_IF(serasa_score = 0) AS n_serasa_score_zero,
    COUNT_IF(serasa_score < 0) AS n_serasa_score_negative,
    COUNT_IF(serasa_score > 1000) AS n_serasa_score_gt_1000,
    APPROX_PERCENTILE(serasa_score, 0.5) AS serasa_score_p50
  FROM enriched_sample
),

agg_serasa_income_estimated_sanity AS (
  SELECT
    COUNT(*) AS n_simulations,
    COUNT_IF(serasa_income_estimated IS NOT NULL) AS n_with_serasa_income_estimated,
    COUNT_IF(serasa_income_estimated <= 0) AS n_serasa_income_nonpositive,
    COUNT_IF(serasa_income_estimated > 100000) AS n_serasa_income_gt_100k,
    APPROX_PERCENTILE(serasa_income_estimated, 0.5) AS serasa_income_p50
  FROM enriched_sample
),

agg_bvs_score_sanity AS (
  SELECT
    COUNT(*) AS n_simulations,
    COUNT_IF(boa_vista_score IS NOT NULL) AS n_with_bvs_score,
    APPROX_PERCENTILE(boa_vista_score, 0.5) AS bvs_score_p50
  FROM enriched_sample
),

agg_cc_count_buckets AS (
  SELECT
    CASE
      WHEN total_credit_checks_count = 0 THEN '0'
      WHEN total_credit_checks_count = 1 THEN '1'
      WHEN total_credit_checks_count = 2 THEN '2'
      ELSE '3+'
    END AS bucket,
    COUNT(*) AS n
  FROM enriched_sample
  GROUP BY 1
),

agg_birthdate_source AS (
  SELECT
    COALESCE(borrower_birthdate_source, 'NULL') AS borrower_birthdate_source,
    COUNT(*) AS n_simulations,
    COUNT_IF(borrower_birthdate IS NOT NULL) AS n_with_birthdate
  FROM enriched_sample
  GROUP BY 1
),

agg AS (
  SELECT
    cs.state,
    cs.approval_bucket,
    COUNT(*) AS n_simulations,
    COUNT_IF(b.rn_best_any = 1) AS n_with_any_check,
    COUNT_IF(b.rn_best_any = 1 AND b.leniency_rank = 0) AS n_strict_any,
    COUNT_IF(b.rn_best_any = 1 AND b.leniency_rank > 0) AS n_lenient_only_any,

    COUNT_IF(s.rn_best_serasa = 1) AS n_with_serasa,
    COUNT_IF(s.rn_best_serasa = 1 AND s.leniency_rank = 0) AS n_serasa_strict,
    COUNT_IF(s.rn_best_serasa = 1 AND s.leniency_rank > 0) AS n_serasa_lenient_only
  FROM cs_cpf cs
  LEFT JOIN cc_best_any b
    ON b.credit_simulation_id = cs.credit_simulation_id
   AND b.rn_best_any = 1
  LEFT JOIN cc_best_serasa s
    ON s.credit_simulation_id = cs.credit_simulation_id
   AND s.rn_best_serasa = 1
  GROUP BY 1,2
),

agg_rejection_reason AS (
  SELECT
    cs.rejection_reason,
    COUNT(*) AS n_simulations,
    COUNT_IF(b.rn_best_any = 1) AS n_with_any_check,
    COUNT_IF(b.rn_best_any = 1 AND b.leniency_rank = 0) AS n_strict_any,
    COUNT_IF(b.rn_best_any = 1 AND b.leniency_rank > 0) AS n_lenient_only_any,
    COUNT_IF(s.rn_best_serasa = 1) AS n_with_serasa,
    COUNT_IF(s.rn_best_serasa = 1 AND s.leniency_rank = 0) AS n_serasa_strict,
    COUNT_IF(s.rn_best_serasa = 1 AND s.leniency_rank > 0) AS n_serasa_lenient_only
  FROM cs_cpf cs
  LEFT JOIN cc_best_any b
    ON b.credit_simulation_id = cs.credit_simulation_id
   AND b.rn_best_any = 1
  LEFT JOIN cc_best_serasa s
    ON s.credit_simulation_id = cs.credit_simulation_id
   AND s.rn_best_serasa = 1
  WHERE cs.state = 'rejected'
  GROUP BY 1
  QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) <= 25
)

SELECT
  'sample' AS metric_group,
  'period' AS metric_name,
  NULL AS dimension,
       NULL::FLOAT AS value_number,
       (SELECT TO_VARCHAR(period_start) || '..' || TO_VARCHAR(period_end) FROM params_user) AS value_string

UNION ALL
SELECT 'state','n_simulations', state || '|' || approval_bucket AS dimension, n_simulations::FLOAT, NULL
FROM agg

UNION ALL
SELECT 'coverage','pct_any_credit_check', state || '|' || approval_bucket AS dimension,
       (n_with_any_check / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg

UNION ALL
SELECT 'coverage','pct_any_strict_1h', state || '|' || approval_bucket AS dimension,
       (n_strict_any / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg

UNION ALL
SELECT 'coverage','pct_any_lenient_only', state || '|' || approval_bucket AS dimension,
       (n_lenient_only_any / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg

UNION ALL
SELECT 'serasa','pct_serasa_any', state || '|' || approval_bucket AS dimension,
       (n_with_serasa / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg

UNION ALL
SELECT 'serasa','pct_serasa_strict_1h', state || '|' || approval_bucket AS dimension,
       (n_serasa_strict / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg

UNION ALL
SELECT 'serasa','pct_serasa_lenient_only', state || '|' || approval_bucket AS dimension,
       (n_serasa_lenient_only / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg

UNION ALL
SELECT 'rejection_reason','n_simulations', COALESCE(rejection_reason,'NULL') AS dimension, n_simulations::FLOAT, NULL
FROM agg_rejection_reason

UNION ALL
SELECT 'rejection_reason','pct_any_credit_check', COALESCE(rejection_reason,'NULL') AS dimension,
       (n_with_any_check / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg_rejection_reason

UNION ALL
SELECT 'rejection_reason','pct_any_strict_1h', COALESCE(rejection_reason,'NULL') AS dimension,
       (n_strict_any / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg_rejection_reason

UNION ALL
SELECT 'rejection_reason','pct_any_lenient_only', COALESCE(rejection_reason,'NULL') AS dimension,
       (n_lenient_only_any / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg_rejection_reason

UNION ALL
SELECT 'rejection_reason','pct_serasa_any', COALESCE(rejection_reason,'NULL') AS dimension,
       (n_with_serasa / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg_rejection_reason

UNION ALL
SELECT 'rejection_reason','pct_serasa_strict_1h', COALESCE(rejection_reason,'NULL') AS dimension,
       (n_serasa_strict / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg_rejection_reason

UNION ALL
SELECT 'rejection_reason','pct_serasa_lenient_only', COALESCE(rejection_reason,'NULL') AS dimension,
       (n_serasa_lenient_only / NULLIF(n_simulations,0))::FLOAT, NULL
FROM agg_rejection_reason

UNION ALL
SELECT
  'serasa_score' AS metric_group,
  'pct_serasa_cc_has_score' AS metric_name,
  CONCAT(COALESCE(serasa_cc_format,'NULL'),'|',COALESCE(serasa_cc_kind,'NULL'),'|',serasa_new_report_name) AS dimension,
  (n_with_serasa_cc_score / NULLIF(n_with_serasa_cc,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_cc_score_by_kind

UNION ALL
SELECT
  'serasa_score' AS metric_group,
  'pct_serasa_cc_format_nonnull_score_null' AS metric_name,
  CONCAT(COALESCE(serasa_cc_format,'NULL'),'|',COALESCE(serasa_cc_kind,'NULL'),'|',serasa_new_report_name) AS dimension,
  (n_serasa_cc_no_score / NULLIF(n_with_serasa_cc,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_cc_score_by_kind

UNION ALL
SELECT
  'serasa_score' AS metric_group,
  'pct_serasa_score_filled_by_source_enriched' AS metric_name,
  serasa_score_source AS dimension,
  (n_with_serasa_score / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_score_source_enriched

UNION ALL
SELECT
  'axis_sources' AS metric_group,
  'pct_axis_source_distribution_enriched' AS metric_name,
  CONCAT(axis, '|', source) AS dimension,
  pct_of_simulations AS value_number,
  NULL AS value_string
FROM axis_source_pct_enriched

UNION ALL
SELECT
  'masking' AS metric_group,
  'pct_neg_masked_crivo_lenient_over_serasa_strict' AS metric_name,
  'all' AS dimension,
  (n_neg_masked / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_masking

UNION ALL
SELECT
  'sanity' AS metric_group,
  'pct_serasa_score_zero_enriched' AS metric_name,
  'all' AS dimension,
  (n_serasa_score_zero / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_score_sanity

UNION ALL
SELECT
  'bvs_score' AS metric_group,
  'pct_boa_vista_score_filled_enriched' AS metric_name,
  'all' AS dimension,
  (n_with_bvs_score / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_bvs_score_sanity

UNION ALL
SELECT
  'bvs_score' AS metric_group,
  'boa_vista_score_p50_enriched' AS metric_name,
  'all' AS dimension,
  bvs_score_p50::FLOAT AS value_number,
  NULL AS value_string
FROM agg_bvs_score_sanity

UNION ALL
SELECT
  'credit_checks_count' AS metric_group,
  'pct_total_credit_checks_count_bucket_enriched' AS metric_name,
  bucket AS dimension,
  (n / NULLIF((SELECT SUM(n) FROM agg_cc_count_buckets),0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_cc_count_buckets

UNION ALL
SELECT
  'birthdate' AS metric_group,
  'pct_birthdate_filled_by_source_enriched' AS metric_name,
  borrower_birthdate_source AS dimension,
  (n_with_birthdate / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_birthdate_source

UNION ALL
SELECT
  'sanity' AS metric_group,
  'pct_serasa_score_gt_1000_enriched' AS metric_name,
  'all' AS dimension,
  (n_serasa_score_gt_1000 / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_score_sanity

UNION ALL
SELECT
  'sanity' AS metric_group,
  'serasa_score_p50_enriched' AS metric_name,
  'all' AS dimension,
  serasa_score_p50::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_score_sanity

UNION ALL
SELECT
  'serasa_income' AS metric_group,
  'pct_serasa_income_estimated_filled_enriched' AS metric_name,
  'all' AS dimension,
  (n_with_serasa_income_estimated / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_income_estimated_sanity

UNION ALL
SELECT
  'serasa_income' AS metric_group,
  'serasa_income_estimated_p50_enriched' AS metric_name,
  'all' AS dimension,
  serasa_income_p50::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_income_estimated_sanity

UNION ALL
SELECT
  'serasa_income' AS metric_group,
  'pct_serasa_income_estimated_gt_100k_enriched' AS metric_name,
  'all' AS dimension,
  (n_serasa_income_gt_100k / NULLIF(n_simulations,0))::FLOAT AS value_number,
  NULL AS value_string
FROM agg_serasa_income_estimated_sanity
;


