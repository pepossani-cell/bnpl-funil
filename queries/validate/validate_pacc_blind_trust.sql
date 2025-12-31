/*
  Validação anti-cegueira: dá para "confiar cegamente" na PACC?

  Objetivo:
    - Para C1 legado (PRE_ANALYSIS_TYPE='pre_analysis'), comparar:
        (A) o que a PACC materializa
        (B) o que existe no raw payload (INCREMENTAL_CREDIT_CHECKS_API) no mesmo intervalo (15d)
      e medir "ganho potencial" (raw tem valor e PACC está NULL).

  Foco desta versão:
    - SERASA new: balances em negativeData.summary.balance + demografia (gender/statusRegistration) (quando existirem no raw)
    - BOA VISTA SCPC NET: debit_total_value (raw) vs totals (view/PACC)

  Notas:
    - PACC é curada por hash_cpf+janela; não existe credit_check_id.
    - Esta query não tenta reproduzir 100% a seleção de report do SERASA (COMBO_CONCESSAO etc.),
      apenas mede existência de campos ricos no raw.

  Como usar:
    - Rode no Snowflake (ou via src.run_sql_file.py). Resulta em métricas agregadas por mês.
*/

WITH params AS (
  SELECT
    400::INT AS n_per_month,
    15::INT AS lookback_days
),
months AS (
  SELECT TO_DATE('2022-10-01') AS month
  UNION ALL SELECT TO_DATE('2023-07-01')
  UNION ALL SELECT TO_DATE('2024-10-01')
  UNION ALL SELECT TO_DATE('2025-06-01')
  UNION ALL SELECT TO_DATE('2025-12-01')
),

spa_dedup AS (
  SELECT spa.*
  FROM CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API spa
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY spa.PRE_ANALYSIS_ID
    ORDER BY spa.PRE_ANALYSIS_UPDATED_AT DESC, spa.PRE_ANALYSIS_CREATED_AT DESC
  ) = 1
),

base AS (
  SELECT
    pa.PRE_ANALYSIS_ID AS pre_analysis_id,
    pa.PRE_ANALYSIS_CREATED_AT AS c1_created_at,
    DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) AS month,
    REGEXP_REPLACE(spa.CPF, '\\D','') AS cpf_digits,
    SHA2(REGEXP_REPLACE(spa.CPF, '\\D',''), 256) AS hash_cpf
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  JOIN months m ON DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) = m.month
  LEFT JOIN spa_dedup spa ON spa.PRE_ANALYSIS_ID = pa.PRE_ANALYSIS_ID
  JOIN params p ON TRUE
  WHERE pa.PRE_ANALYSIS_TYPE = 'pre_analysis'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT)
    ORDER BY UNIFORM(0,1000000,RANDOM())
  ) <= p.n_per_month
),

pacc AS (
  SELECT
    PRE_ANALYSIS_ID::NUMBER AS pre_analysis_id,
    /* principais campos que usamos no enrichment */
    SERASA_POSITIVE_SCORE,
    SERASA_PRESUMED_INCOME,
    SERASA_PEFIN,
    SERASA_REFIN,
    SERASA_PROTEST,
    BVS_POSITIVE_SCORE,
    BVS_TOTAL_DEBT,
    BVS_TOTAL_PROTEST
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK
  WHERE PRE_ANALYSIS_TYPE = 'pre_analysis'
),

/* ---------------------------
   RAW: SERASA new (preferimos kind check_score_without_income, mas medimos presença de campos)
   --------------------------- */
serasa_raw_candidates AS (
  SELECT
    b.pre_analysis_id,
    cc.id AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.kind,
    cc.new_data_format,
    cc.data AS data,
    ROW_NUMBER() OVER (
      PARTITION BY b.pre_analysis_id
      ORDER BY ABS(DATEDIFF('minute', cc.created_at, b.c1_created_at)) ASC, cc.created_at DESC, cc.id DESC
    ) AS rn_best
  FROM base b
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = b.cpf_digits
   AND cc.source = 'serasa'
   AND cc.created_at BETWEEN DATEADD('day', -p.lookback_days, b.c1_created_at) AND b.c1_created_at
  WHERE b.cpf_digits IS NOT NULL AND b.cpf_digits <> ''
),

serasa_raw_best AS (
  SELECT *
  FROM serasa_raw_candidates
  WHERE rn_best = 1
),

/* extrair sinais ricos (quando o payload for SERASA new com reports) */
serasa_raw_rich AS (
  SELECT
    s.pre_analysis_id,
    /* flags de formato */
    IFF(TYPEOF(s.data)='OBJECT' AND s.data:reports IS NOT NULL, TRUE, FALSE) AS is_serasa_new_like,

    /* demografia no report */
    MAX(NULLIF(TRIM(r.value:registration:consumerGender::string), '')) AS raw_serasa_gender,
    MAX(NULLIF(TRIM(r.value:registration:statusRegistration::string), '')) AS raw_serasa_status_registration,

    /* negativação - balances (rico) */
    MAX(TRY_TO_NUMBER(r.value:negativeData:pefin:summary:balance::string))  AS raw_pefin_balance,
    MAX(TRY_TO_NUMBER(r.value:negativeData:refin:summary:balance::string))  AS raw_refin_balance,
    MAX(TRY_TO_NUMBER(r.value:negativeData:notary:summary:balance::string)) AS raw_notary_balance
  FROM serasa_raw_best s
  , LATERAL FLATTEN(input => s.data:reports) r
  GROUP BY 1,2
),

/* ---------------------------
   RAW: BOA VISTA SCPC NET (valor total de débito no payload raw)
   --------------------------- */
bvs_scpc_raw_candidates AS (
  SELECT
    b.pre_analysis_id,
    cc.id AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.data AS data,
    ROW_NUMBER() OVER (
      PARTITION BY b.pre_analysis_id
      ORDER BY ABS(DATEDIFF('minute', cc.created_at, b.c1_created_at)) ASC, cc.created_at DESC, cc.id DESC
    ) AS rn_best
  FROM base b
  JOIN params p ON TRUE
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = b.cpf_digits
   AND cc.source = 'boa_vista_scpc_net'
   AND cc.created_at BETWEEN DATEADD('day', -p.lookback_days, b.c1_created_at) AND b.c1_created_at
  WHERE b.cpf_digits IS NOT NULL AND b.cpf_digits <> ''
),

bvs_scpc_raw_best AS (
  SELECT *
  FROM bvs_scpc_raw_candidates
  WHERE rn_best = 1
),

bvs_scpc_raw_rich AS (
  SELECT
    b.pre_analysis_id,
    MAX(TRY_TO_NUMBER(f.value:"141":debit_total_value::string)) AS raw_bvs_debit_total_value,
    MAX(TRY_TO_NUMBER(f.value:"141":debit_total_count::string)) AS raw_bvs_debit_total_count
  FROM bvs_scpc_raw_best b
  , LATERAL FLATTEN(input => b.data) f
  GROUP BY 1
),

joined AS (
  SELECT
    b.month,
    b.pre_analysis_id,
    p.SERASA_POSITIVE_SCORE,
    p.SERASA_PRESUMED_INCOME,
    p.SERASA_PEFIN,
    p.SERASA_REFIN,
    p.SERASA_PROTEST,
    p.BVS_POSITIVE_SCORE,
    p.BVS_TOTAL_DEBT,
    p.BVS_TOTAL_PROTEST,
    sr.is_serasa_new_like,
    sr.raw_serasa_gender,
    sr.raw_serasa_status_registration,
    sr.raw_pefin_balance,
    sr.raw_refin_balance,
    sr.raw_notary_balance,
    br.raw_bvs_debit_total_value,
    br.raw_bvs_debit_total_count
  FROM base b
  LEFT JOIN pacc p ON p.pre_analysis_id = b.pre_analysis_id
  LEFT JOIN serasa_raw_rich sr ON sr.pre_analysis_id = b.pre_analysis_id
  LEFT JOIN bvs_scpc_raw_rich br ON br.pre_analysis_id = b.pre_analysis_id
),

agg AS (
  SELECT
    month,
    COUNT(*) AS n_sample,
    AVG(IFF(SERASA_POSITIVE_SCORE IS NOT NULL OR SERASA_PRESUMED_INCOME IS NOT NULL OR SERASA_PEFIN IS NOT NULL OR SERASA_REFIN IS NOT NULL OR SERASA_PROTEST IS NOT NULL, 1, 0)) AS pct_pacc_has_any_serasa_signal,
    AVG(IFF(BVS_POSITIVE_SCORE IS NOT NULL OR BVS_TOTAL_DEBT IS NOT NULL OR BVS_TOTAL_PROTEST IS NOT NULL, 1, 0)) AS pct_pacc_has_any_bvs_signal,

    /* existência de campos ricos no raw */
    AVG(IFF(is_serasa_new_like, 1, 0)) AS pct_raw_serasa_new_like_present,
    AVG(IFF(raw_pefin_balance IS NOT NULL OR raw_refin_balance IS NOT NULL OR raw_notary_balance IS NOT NULL, 1, 0)) AS pct_raw_serasa_has_any_balance,
    AVG(IFF(raw_serasa_gender IS NOT NULL, 1, 0)) AS pct_raw_serasa_has_gender,
    AVG(IFF(raw_serasa_status_registration IS NOT NULL, 1, 0)) AS pct_raw_serasa_has_status_registration,

    AVG(IFF(raw_bvs_debit_total_value IS NOT NULL, 1, 0)) AS pct_raw_bvs_scpc_has_debit_total_value,

    /* ganho potencial: raw tem mas PACC não materializou (PACC não tem balances/gênero/status) */
    AVG(IFF((raw_pefin_balance IS NOT NULL OR raw_refin_balance IS NOT NULL OR raw_notary_balance IS NOT NULL), 1, 0)) AS pct_potential_balance_features_not_in_pacc,
    AVG(IFF(raw_serasa_gender IS NOT NULL, 1, 0)) AS pct_potential_gender_not_in_pacc,
    AVG(IFF(raw_serasa_status_registration IS NOT NULL, 1, 0)) AS pct_potential_status_registration_not_in_pacc,

    /* comparação: BVS raw debit_total_value vs PACC bvs_total_debt (não é 1:1, mas indica possível feature rica) */
    AVG(IFF(raw_bvs_debit_total_value IS NOT NULL AND BVS_TOTAL_DEBT IS NULL, 1, 0)) AS pct_raw_bvs_value_but_pacc_bvs_total_debt_null
  FROM joined
  GROUP BY 1
)

SELECT *
FROM agg
ORDER BY month
;

