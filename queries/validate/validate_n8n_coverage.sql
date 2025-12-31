/*
  Validação: cobertura e POTENCIAL do `n8n` (CEI) para C1 legado (`pre_analysis`).

  Por que "potencial" e não "uso"?
    - Sem materializar `PRE_ANALYSES_ENRICHED_BORROWER_V1`, é caro reproduzir toda a cascata.
    - Aqui medimos:
        (1) presença de payload n8n por mês
        (2) % de casos onde n8n teria valor incremental simples:
            PACC está NULL para um sinal e n8n tem esse sinal.

  Observação:
    - n8n observado a partir de 2025-10.
*/

WITH params AS (
  SELECT 1500::INT AS n_per_month
),
sample_months AS (
  SELECT TO_DATE('2025-10-01') AS month
  UNION ALL SELECT TO_DATE('2025-11-01')
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

sample_pa AS (
  SELECT
    pa.PRE_ANALYSIS_ID AS pre_analysis_id,
    pa.PRE_ANALYSIS_CREATED_AT AS c1_created_at,
    DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) AS month,
    SHA2(REGEXP_REPLACE(spa.CPF, '\\D',''), 256) AS hash_cpf
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  JOIN sample_months m
    ON DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) = m.month
  LEFT JOIN spa_dedup spa
    ON spa.PRE_ANALYSIS_ID = pa.PRE_ANALYSIS_ID
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
    SERASA_POSITIVE_SCORE,
    SERASA_PRESUMED_INCOME,
    SERASA_PEFIN,
    SERASA_REFIN,
    SERASA_PROTEST,
    BVS_POSITIVE_SCORE,
    BVS_TOTAL_DEBT,
    BVS_TOTAL_PROTEST,
    SCORE_SCR
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK
  WHERE PRE_ANALYSIS_TYPE = 'pre_analysis'
),

cei_n8n_latest AS (
  SELECT
    ENGINEABLE_ID AS pre_analysis_id,
    TRY_PARSE_JSON(DATA) AS j,
    CREDIT_ENGINE_CONSULTATION_CREATED_AT AS created_at
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION
  WHERE LOWER(ENGINEABLE_TYPE) IN ('pre_analysis','preanalysis','pre-analysis')
    AND SOURCE ILIKE '%n8n%'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ENGINEABLE_ID
    ORDER BY CREDIT_ENGINE_CONSULTATION_CREATED_AT DESC
  ) = 1
),

n8n AS (
  SELECT
    n.pre_analysis_id,
    TRY_TO_NUMBER(n.j:scoreSerasa::string) AS n8n_score_serasa,
    TRY_TO_NUMBER(n.j:scoreBvs::string)    AS n8n_score_bvs,
    TRY_TO_NUMBER(n.j:pefinSerasa::string) AS n8n_pefin_serasa,
    TRY_TO_NUMBER(n.j:refinSerasa::string) AS n8n_refin_serasa,
    TRY_TO_NUMBER(n.j:protestoSerasa::string) AS n8n_protesto_serasa,
    TRY_TO_NUMBER(n.j:protestoBvs::string) AS n8n_protesto_bvs,
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
    ) AS n8n_renda_serasa
  FROM cei_n8n_latest n
  WHERE TYPEOF(n.j)='OBJECT'
),

joined AS (
  SELECT
    s.month,
    s.pre_analysis_id,
    IFF(n.pre_analysis_id IS NOT NULL, TRUE, FALSE) AS has_n8n,
    p.SERASA_POSITIVE_SCORE,
    p.BVS_POSITIVE_SCORE,
    p.SERASA_PRESUMED_INCOME,
    p.SERASA_PEFIN,
    p.SERASA_REFIN,
    p.SERASA_PROTEST,
    p.BVS_TOTAL_DEBT,
    p.BVS_TOTAL_PROTEST,
    p.SCORE_SCR,
    n.n8n_score_serasa,
    n.n8n_score_bvs,
    n.n8n_renda_serasa,
    n.n8n_pefin_serasa,
    n.n8n_refin_serasa,
    n.n8n_protesto_serasa,
    n.n8n_protesto_bvs
  FROM sample_pa s
  LEFT JOIN pacc p
    ON p.pre_analysis_id = s.pre_analysis_id
  LEFT JOIN n8n n
    ON n.pre_analysis_id = s.pre_analysis_id
),

agg AS (
  SELECT
    month,
    COUNT(*) AS n_sample,
    AVG(IFF(has_n8n, 1, 0)) AS pct_has_n8n,

    /* presença de sinais no n8n */
    AVG(IFF(has_n8n AND n8n_score_serasa IS NOT NULL, 1, 0)) AS pct_n8n_has_score_serasa,
    AVG(IFF(has_n8n AND n8n_score_bvs IS NOT NULL, 1, 0)) AS pct_n8n_has_score_bvs,
    AVG(IFF(has_n8n AND n8n_renda_serasa IS NOT NULL, 1, 0)) AS pct_n8n_has_renda_serasa,
    AVG(IFF(has_n8n AND (n8n_pefin_serasa IS NOT NULL OR n8n_refin_serasa IS NOT NULL OR n8n_protesto_serasa IS NOT NULL OR n8n_protesto_bvs IS NOT NULL), 1, 0)) AS pct_n8n_has_any_neg_counts,

    /* potencial incremental simples: PACC nulo e n8n tem */
    AVG(IFF(has_n8n AND SERASA_POSITIVE_SCORE IS NULL AND n8n_score_serasa IS NOT NULL, 1, 0)) AS pct_pacc_missing_serasa_score_but_n8n_has,
    AVG(IFF(has_n8n AND BVS_POSITIVE_SCORE IS NULL AND n8n_score_bvs IS NOT NULL, 1, 0)) AS pct_pacc_missing_bvs_score_but_n8n_has,
    AVG(IFF(has_n8n AND SERASA_PRESUMED_INCOME IS NULL AND n8n_renda_serasa IS NOT NULL, 1, 0)) AS pct_pacc_missing_income_but_n8n_has,
    AVG(IFF(has_n8n AND SERASA_PEFIN IS NULL AND n8n_pefin_serasa IS NOT NULL, 1, 0)) AS pct_pacc_missing_pefin_but_n8n_has
  FROM joined
  GROUP BY 1
)
SELECT *
FROM agg
ORDER BY month
;

