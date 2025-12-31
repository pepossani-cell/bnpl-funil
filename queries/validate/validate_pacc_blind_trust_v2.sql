/*
  Validação anti-cegueira (v2): PACC vs "fontes do dbt" + raw richness condicionado.

  Diferença vs v1:
    - mede cobertura de consulta via views hash-based (SOURCE_CREDIT_CHECKS_API_*)
    - mede inconsistência "view tem consulta 15d mas PACC está NULL"
    - mede presença de campos ricos no raw (balances/gender/status; bvs debit_total_value)
      APENAS nos casos em que a view indica que houve consulta (evita falso negativo por ausência de ingestão raw).

  Escopo:
    - PRE_ANALYSIS_TYPE in ('pre_analysis','credit_simulation') via "complete_pre_analysis" (como dbt).
*/

WITH params AS (
  SELECT
    400::INT AS n_per_month,
    15::INT  AS lookback_days
),
months AS (
  SELECT TO_DATE('2022-10-01') AS month
  UNION ALL SELECT TO_DATE('2023-07-01')
  UNION ALL SELECT TO_DATE('2024-10-01')
  UNION ALL SELECT TO_DATE('2025-06-01')
  UNION ALL SELECT TO_DATE('2025-12-01')
),

/* ---------- complete_pre_analysis (dbt-like) ---------- */
spa_dedup AS (
  SELECT spa.*
  FROM CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API spa
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY spa.PRE_ANALYSIS_ID
    ORDER BY spa.PRE_ANALYSIS_UPDATED_AT DESC, spa.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
),

pa_legacy AS (
  SELECT
    pa.PRE_ANALYSIS_ID AS pre_analysis_id,
    pa.PRE_ANALYSIS_CREATED_AT AS pre_analysis_created_at,
    'pre_analysis' AS pre_analysis_type,
    SHA2(REGEXP_REPLACE(spa.CPF, '\\D',''), 256) AS hash_cpf,
    REGEXP_REPLACE(spa.CPF, '\\D','') AS cpf_digits
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  LEFT JOIN spa_dedup spa
    ON spa.PRE_ANALYSIS_ID = pa.PRE_ANALYSIS_ID
  WHERE pa.PRE_ANALYSIS_TYPE = 'pre_analysis'
),

cs_sensitive AS (
  SELECT
    cs.PRE_ANALYSIS_ID AS pre_analysis_id,
    cs.PRE_ANALYSIS_CREATED_AT AS pre_analysis_created_at,
    'credit_simulation' AS pre_analysis_type,
    SHA2(REGEXP_REPLACE(cs.CPF, '\\D',''), 256) AS hash_cpf,
    REGEXP_REPLACE(cs.CPF, '\\D','') AS cpf_digits
  FROM CAPIM_DATA.RESTRICTED.SOURCE_RESTRICTED_CREDIT_SIMULATIONS cs
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cs.PRE_ANALYSIS_ID
    ORDER BY cs.PRE_ANALYSIS_UPDATED_AT DESC NULLS LAST
  ) = 1
),

complete_pre_analysis AS (
  SELECT * FROM pa_legacy
  UNION ALL
  SELECT * FROM cs_sensitive
),

sample_data AS (
  SELECT
    cpa.*
  FROM complete_pre_analysis cpa
  JOIN params p ON TRUE
  WHERE DATE_TRUNC('month', cpa.pre_analysis_created_at) IN (SELECT month FROM months)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cpa.pre_analysis_type, DATE_TRUNC('month', cpa.pre_analysis_created_at)
    ORDER BY UNIFORM(0,1000000,RANDOM())
  ) <= p.n_per_month
),

/* ---------- PACC ---------- */
pacc AS (
  SELECT
    PRE_ANALYSIS_ID::NUMBER AS pre_analysis_id,
    PRE_ANALYSIS_TYPE AS pre_analysis_type,
    SERASA_CONSULTED_AT,
    SERASA_POSITIVE_SCORE,
    SERASA_PEFIN,
    SERASA_REFIN,
    SERASA_PROTEST,
    SERASA_PRESUMED_INCOME,
    SERASA_HAS_ERROR,
    IS_CACHE_SERASA,

    BVS_SCORE_PF_NET_CONSULTED_AT,
    BVS_POSITIVE_SCORE,
    BVS_HAS_ERROR,
    IS_CACHE_BVS_SCORE_PF,

    BVS_SCPC_NET_CONSULTED_AT,
    BVS_CCF_COUNT,
    BVS_TOTAL_DEBT,
    BVS_TOTAL_PROTEST,
    BVS_STATUS_IR,
    SCPC_HAS_ERROR,
    IS_CACHE_BVS_SCPC,

    SCR_REPORT_CONSULTED_AT,
    SCORE_SCR,
    SCR_HAS_ERROR,
    IS_CACHE_SCR
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK
),

/* ---------- Views hash-based (dbt sources) ---------- */
serasa_view_15d AS (
  SELECT
    s.pre_analysis_id,
    s.pre_analysis_type,
    COUNT_IF(
      ABS(DATEDIFF('days', s.pre_analysis_created_at, DATEADD('hours', -3, v.SERASA_CONSULTED_AT))) BETWEEN 0 AND 15
      AND (
        (DATE(s.pre_analysis_created_at) < '2024-04-04' AND (v.KIND IS NULL OR v.KIND = 'check_score'))
        OR
        (DATE(s.pre_analysis_created_at) >= '2024-04-04' AND v.KIND IN ('check_income_only', 'check_score_without_income'))
      )
    ) AS n_serasa_view_15d
  FROM sample_data s
  LEFT JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_SERASA v
    ON v.HASH_CPF = s.hash_cpf
  GROUP BY 1,2
),

bvs_score_pf_view_15d AS (
  SELECT
    s.pre_analysis_id,
    s.pre_analysis_type,
    COUNT_IF(
      ABS(DATEDIFF('days', s.pre_analysis_created_at, DATEADD('hours', -3, v.BVS_SCORE_PF_NET_CONSULTED_AT))) BETWEEN 0 AND 15
    ) AS n_bvs_score_pf_view_15d
  FROM sample_data s
  LEFT JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_BOA_VISTA_SCORE_PF v
    ON v.HASH_CPF = s.hash_cpf
  GROUP BY 1,2
),

bvs_scpc_view_15d AS (
  SELECT
    s.pre_analysis_id,
    s.pre_analysis_type,
    COUNT_IF(
      ABS(DATEDIFF('days', s.pre_analysis_created_at, DATEADD('hours', -3, v.BVS_SCPC_NET_CONSULTED_AT))) BETWEEN 0 AND 15
    ) AS n_bvs_scpc_view_15d
  FROM sample_data s
  LEFT JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_BOA_VISTA_SCPC_NET v
    ON v.HASH_CPF = s.hash_cpf
  GROUP BY 1,2
),

scr_view_15d AS (
  SELECT
    s.pre_analysis_id,
    s.pre_analysis_type,
    COUNT_IF(
      ABS(DATEDIFF('days', s.pre_analysis_created_at, DATEADD('hours', -3, v.SCR_REPORT_CONSULTED_AT))) BETWEEN 0 AND 15
    ) AS n_scr_view_15d
  FROM sample_data s
  LEFT JOIN CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_SCR_REPORT v
    ON v.HASH_CPF = s.hash_cpf
  GROUP BY 1,2
),

/* ---------- RAW richness (condicionado a existir consulta na view) ---------- */
serasa_raw_best AS (
  SELECT
    s.pre_analysis_id,
    s.pre_analysis_type,
    cc.id AS credit_check_id,
    cc.created_at,
    cc.kind,
    cc.new_data_format,
    cc.data AS data,
    ROW_NUMBER() OVER (
      PARTITION BY s.pre_analysis_id, s.pre_analysis_type
      ORDER BY ABS(DATEDIFF('minute', cc.created_at, s.pre_analysis_created_at)) ASC, cc.created_at DESC, cc.id DESC
    ) AS rn
  FROM sample_data s
  JOIN params p ON TRUE
  JOIN serasa_view_15d sv
    ON sv.pre_analysis_id = s.pre_analysis_id
   AND sv.pre_analysis_type = s.pre_analysis_type
   AND sv.n_serasa_view_15d > 0
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = s.cpf_digits
   AND cc.source = 'serasa'
   AND cc.created_at BETWEEN DATEADD('day', -p.lookback_days, s.pre_analysis_created_at) AND s.pre_analysis_created_at
),

serasa_raw_rich AS (
  SELECT
    s.pre_analysis_id,
    s.pre_analysis_type,
    IFF(TYPEOF(s.data)='OBJECT' AND s.data:reports IS NOT NULL, TRUE, FALSE) AS is_serasa_new_like,
    MAX(NULLIF(TRIM(r.value:registration:consumerGender::string), '')) AS raw_serasa_gender,
    MAX(NULLIF(TRIM(r.value:registration:statusRegistration::string), '')) AS raw_serasa_status_registration,
    MAX(TRY_TO_NUMBER(r.value:negativeData:pefin:summary:balance::string))  AS raw_pefin_balance,
    MAX(TRY_TO_NUMBER(r.value:negativeData:refin:summary:balance::string))  AS raw_refin_balance,
    MAX(TRY_TO_NUMBER(r.value:negativeData:notary:summary:balance::string)) AS raw_notary_balance
  FROM serasa_raw_best s
  , LATERAL FLATTEN(input => s.data:reports) r
  WHERE s.rn = 1
  GROUP BY 1,2,3
),

bvs_scpc_raw_best AS (
  SELECT
    s.pre_analysis_id,
    s.pre_analysis_type,
    cc.id AS credit_check_id,
    cc.created_at,
    cc.data AS data,
    ROW_NUMBER() OVER (
      PARTITION BY s.pre_analysis_id, s.pre_analysis_type
      ORDER BY ABS(DATEDIFF('minute', cc.created_at, s.pre_analysis_created_at)) ASC, cc.created_at DESC, cc.id DESC
    ) AS rn
  FROM sample_data s
  JOIN params p ON TRUE
  JOIN bvs_scpc_view_15d bv
    ON bv.pre_analysis_id = s.pre_analysis_id
   AND bv.pre_analysis_type = s.pre_analysis_type
   AND bv.n_bvs_scpc_view_15d > 0
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = s.cpf_digits
   AND cc.source = 'boa_vista_scpc_net'
   AND cc.created_at BETWEEN DATEADD('day', -p.lookback_days, s.pre_analysis_created_at) AND s.pre_analysis_created_at
),

bvs_scpc_raw_rich AS (
  SELECT
    b.pre_analysis_id,
    b.pre_analysis_type,
    MAX(TRY_TO_NUMBER(f.value:"141":debit_total_value::string)) AS raw_bvs_debit_total_value
  FROM bvs_scpc_raw_best b
  , LATERAL FLATTEN(input => b.data) f
  WHERE b.rn = 1
  GROUP BY 1,2
),

joined AS (
  SELECT
    DATE_TRUNC('month', s.pre_analysis_created_at) AS month,
    s.pre_analysis_type,
    s.pre_analysis_id,

    /* view coverage */
    (sv.n_serasa_view_15d > 0) AS has_serasa_view_15d,
    (bv.n_bvs_score_pf_view_15d > 0) AS has_bvs_score_pf_view_15d,
    (sc.n_bvs_scpc_view_15d > 0) AS has_bvs_scpc_view_15d,
    (rv.n_scr_view_15d > 0) AS has_scr_view_15d,

    /* PACC coverage */
    (p.pre_analysis_id IS NOT NULL) AS has_pacc_row,
    (p.SERASA_CONSULTED_AT IS NOT NULL) AS pacc_has_serasa_consulted_at,
    (p.BVS_SCPC_NET_CONSULTED_AT IS NOT NULL) AS pacc_has_bvs_scpc_consulted_at,
    (p.SCR_REPORT_CONSULTED_AT IS NOT NULL) AS pacc_has_scr_consulted_at,

    (p.SERASA_POSITIVE_SCORE IS NOT NULL OR p.SERASA_PRESUMED_INCOME IS NOT NULL OR p.SERASA_PEFIN IS NOT NULL OR p.SERASA_REFIN IS NOT NULL OR p.SERASA_PROTEST IS NOT NULL) AS pacc_has_any_serasa_signal,
    (p.BVS_POSITIVE_SCORE IS NOT NULL OR p.BVS_TOTAL_DEBT IS NOT NULL OR p.BVS_TOTAL_PROTEST IS NOT NULL OR p.BVS_CCF_COUNT IS NOT NULL) AS pacc_has_any_bvs_signal,
    (p.SCORE_SCR IS NOT NULL OR p.SCR_REPORT_CONSULTED_AT IS NOT NULL) AS pacc_has_any_scr_signal,

    /* RAW richness (somente se houve consulta na view) */
    sr.is_serasa_new_like,
    (sr.raw_pefin_balance IS NOT NULL OR sr.raw_refin_balance IS NOT NULL OR sr.raw_notary_balance IS NOT NULL) AS raw_serasa_has_any_balance,
    (sr.raw_serasa_gender IS NOT NULL) AS raw_serasa_has_gender,
    (sr.raw_serasa_status_registration IS NOT NULL) AS raw_serasa_has_status_registration,
    (br.raw_bvs_debit_total_value IS NOT NULL) AS raw_bvs_has_debit_total_value,

    /* inconsistências: view indica consulta, mas PACC não traz nada (potencial bug de materialização) */
    IFF((sv.n_serasa_view_15d > 0) AND NOT (p.SERASA_CONSULTED_AT IS NOT NULL OR pacc_has_any_serasa_signal), TRUE, FALSE) AS view_serasa_but_pacc_serasa_missing,
    IFF((sc.n_bvs_scpc_view_15d > 0) AND NOT (p.BVS_SCPC_NET_CONSULTED_AT IS NOT NULL OR p.BVS_TOTAL_DEBT IS NOT NULL OR p.BVS_TOTAL_PROTEST IS NOT NULL), TRUE, FALSE) AS view_bvs_scpc_but_pacc_missing,
    IFF((rv.n_scr_view_15d > 0) AND NOT (p.SCR_REPORT_CONSULTED_AT IS NOT NULL OR p.SCORE_SCR IS NOT NULL), TRUE, FALSE) AS view_scr_but_pacc_missing
  FROM sample_data s
  LEFT JOIN pacc p
    ON p.pre_analysis_id = s.pre_analysis_id
   AND p.pre_analysis_type = s.pre_analysis_type
  LEFT JOIN serasa_view_15d sv
    ON sv.pre_analysis_id = s.pre_analysis_id
   AND sv.pre_analysis_type = s.pre_analysis_type
  LEFT JOIN bvs_score_pf_view_15d bv
    ON bv.pre_analysis_id = s.pre_analysis_id
   AND bv.pre_analysis_type = s.pre_analysis_type
  LEFT JOIN bvs_scpc_view_15d sc
    ON sc.pre_analysis_id = s.pre_analysis_id
   AND sc.pre_analysis_type = s.pre_analysis_type
  LEFT JOIN scr_view_15d rv
    ON rv.pre_analysis_id = s.pre_analysis_id
   AND rv.pre_analysis_type = s.pre_analysis_type
  LEFT JOIN serasa_raw_rich sr
    ON sr.pre_analysis_id = s.pre_analysis_id
   AND sr.pre_analysis_type = s.pre_analysis_type
  LEFT JOIN bvs_scpc_raw_rich br
    ON br.pre_analysis_id = s.pre_analysis_id
   AND br.pre_analysis_type = s.pre_analysis_type
),

agg AS (
  SELECT
    j.month,
    j.pre_analysis_type,
    COUNT(*) AS n_sample,

    AVG(IFF(j.has_pacc_row, 1, 0)) AS pct_has_pacc_row,

    AVG(IFF(j.has_serasa_view_15d, 1, 0)) AS pct_has_serasa_view_15d,
    AVG(IFF(j.pacc_has_any_serasa_signal, 1, 0)) AS pct_pacc_has_any_serasa_signal,
    AVG(IFF(j.view_serasa_but_pacc_serasa_missing, 1, 0)) AS pct_view_serasa_but_pacc_missing,

    AVG(IFF(j.has_bvs_scpc_view_15d, 1, 0)) AS pct_has_bvs_scpc_view_15d,
    AVG(IFF(p.BVS_TOTAL_DEBT IS NOT NULL OR p.BVS_TOTAL_PROTEST IS NOT NULL OR p.BVS_CCF_COUNT IS NOT NULL, 1, 0)) AS pct_pacc_has_any_bvs_scpc_fields,
    AVG(IFF(j.view_bvs_scpc_but_pacc_missing, 1, 0)) AS pct_view_bvs_scpc_but_pacc_missing,

    AVG(IFF(j.has_scr_view_15d, 1, 0)) AS pct_has_scr_view_15d,
    AVG(IFF(j.pacc_has_any_scr_signal, 1, 0)) AS pct_pacc_has_any_scr_signal,
    AVG(IFF(j.view_scr_but_pacc_missing, 1, 0)) AS pct_view_scr_but_pacc_missing,

    /* RAW richness (condicionado) */
    AVG(IFF(j.has_serasa_view_15d, IFF(j.raw_serasa_has_any_balance, 1, 0), NULL)) AS pct_raw_serasa_has_balance_given_view,
    AVG(IFF(j.has_serasa_view_15d, IFF(j.raw_serasa_has_gender, 1, 0), NULL)) AS pct_raw_serasa_has_gender_given_view,
    AVG(IFF(j.has_serasa_view_15d, IFF(j.raw_serasa_has_status_registration, 1, 0), NULL)) AS pct_raw_serasa_has_status_given_view,
    AVG(IFF(j.has_bvs_scpc_view_15d, IFF(j.raw_bvs_has_debit_total_value, 1, 0), NULL)) AS pct_raw_bvs_debit_total_value_given_view
  FROM joined j
  LEFT JOIN pacc p
    ON p.pre_analysis_id = j.pre_analysis_id
   AND p.pre_analysis_type = j.pre_analysis_type
  GROUP BY 1,2
)

SELECT *
FROM agg
ORDER BY month, pre_analysis_type
;

