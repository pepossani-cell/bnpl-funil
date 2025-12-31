/*
  Auditoria de CORRETUDE do enrichment (amostral, anti-cegueira)

  Ideia:
    - Amostrar linhas da tabela enriched (fonte da verdade materializada)
    - Re-extrair do payload bruto (credit_checks/crivo) para um subconjunto de features core
    - Medir mismatches / divergências / outliers por eixo e por fonte

  Importante:
    - Esta query é propositalmente “opiniosa”: valida apenas quando existe evidência estrita (±1h) suficiente
      para evitar falsos positivos causados por cache/fallback de tempo.
    - Ajuste o FROM de `enriched` para sua tabela materializada.

  Como usar:
    - Ajuste params_user.period_start/period_end, sample_n
    - Substitua a tabela em `enriched` (abaixo)
    - Rode no Snowflake Worksheet
*/

WITH params_user AS (
  SELECT
    '2025-09-01'::TIMESTAMP_NTZ AS period_start,
    '2025-10-01'::TIMESTAMP_NTZ AS period_end,
    20000::INT AS sample_n
),

/* ===== Fonte: tabela enriquecida (SUBSTITUA AQUI) ===== */
enriched AS (
  SELECT
    e.*
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1 e
  JOIN params_user p ON TRUE
  WHERE e.cs_created_at >= p.period_start
    AND e.cs_created_at <  p.period_end
  QUALIFY ROW_NUMBER() OVER (ORDER BY UNIFORM(0, 1000000, RANDOM())) <= (SELECT sample_n FROM params_user)
),

/* ============================
   Re-match estrito (±1h) — SERASA new (score_without_income)
   Usado para validar cadastro/negativação/score quando a fonte enriched aponta SERASA new.
   ============================ */
serasa_new_strict_candidates AS (
  SELECT
    e.credit_simulation_id,
    e.cs_created_at,
    e.cpf_effective_digits,
    cc.id AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.data AS data,
    DATEDIFF('minute', cc.created_at, e.cs_created_at) AS minutes_from_cs,
    ROW_NUMBER() OVER (
      PARTITION BY e.credit_simulation_id
      ORDER BY ABS(DATEDIFF('minute', cc.created_at, e.cs_created_at)) ASC, cc.created_at DESC, cc.id DESC
    ) AS rn_best
  FROM enriched e
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = e.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('hour', -1, e.cs_created_at) AND DATEADD('hour', 1, e.cs_created_at)
  WHERE cc.source = 'serasa'
    AND (
      COALESCE(cc.new_data_format, FALSE) = TRUE
      OR (TYPEOF(cc.data)='OBJECT' AND cc.data:reports IS NOT NULL)
    )
    AND cc.kind = 'check_score_without_income'
),

serasa_new_strict_best AS (
  SELECT *
  FROM serasa_new_strict_candidates
  WHERE rn_best = 1
),

serasa_new_reports AS (
  SELECT
    s.credit_simulation_id,
    s.credit_check_id,
    s.credit_check_created_at,
    s.minutes_from_cs,
    r.value AS report,
    r.index AS report_index
  FROM serasa_new_strict_best s,
  LATERAL FLATTEN(input => s.data:reports) r
),

serasa_new_best_report AS (
  SELECT
    sr.*,
    ROW_NUMBER() OVER (
      PARTITION BY sr.credit_simulation_id
      ORDER BY
        IFF(sr.report:reportName::string = 'COMBO_CONCESSAO', 0, 1),
        sr.report_index ASC
    ) AS rn_report
  FROM serasa_new_reports sr
),

serasa_new_extracted AS (
  SELECT
    s.credit_simulation_id,
    s.credit_check_id,
    s.credit_check_created_at,
    s.minutes_from_cs,
    br.report:reportName::string AS report_name,
    TRY_TO_DATE(br.report:registration:birthDate::string) AS birthdate,
    NULLIF(TRIM(br.report:registration:consumerGender::string), '') AS gender,
    NULLIF(TRIM(br.report:registration:address:zipCode::string), '') AS zipcode,
    TRY_TO_NUMBER(br.report:negativeData:pefin:summary:count::string) AS pefin_count,
    TRY_TO_NUMBER(br.report:negativeData:refin:summary:count::string) AS refin_count,
    TRY_TO_NUMBER(br.report:negativeData:notary:summary:count::string) AS protesto_count
  FROM serasa_new_strict_best s
  LEFT JOIN (
    SELECT * FROM serasa_new_best_report WHERE rn_report = 1
  ) br
    ON br.credit_simulation_id = s.credit_simulation_id
),

/* ============================
   Re-match estrito (±1h) — SERASA new (income_only)
   Usado para validar serasa_income_estimated.
   ============================ */
serasa_income_only_strict AS (
  SELECT
    e.credit_simulation_id,
    cc.id AS credit_check_id,
    cc.created_at AS credit_check_created_at,
    cc.data AS data,
    DATEDIFF('minute', cc.created_at, e.cs_created_at) AS minutes_from_cs,
    ROW_NUMBER() OVER (
      PARTITION BY e.credit_simulation_id
      ORDER BY ABS(DATEDIFF('minute', cc.created_at, e.cs_created_at)) ASC, cc.created_at DESC, cc.id DESC
    ) AS rn_best
  FROM enriched e
  JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    ON REGEXP_REPLACE(cc.cpf, '\\D','') = e.cpf_effective_digits
   AND cc.created_at BETWEEN DATEADD('hour', -1, e.cs_created_at) AND DATEADD('hour', 1, e.cs_created_at)
  WHERE cc.source = 'serasa'
    AND COALESCE(cc.new_data_format, FALSE) = TRUE
    AND cc.kind = 'check_income_only'
),

serasa_income_only_best AS (
  SELECT * FROM serasa_income_only_strict WHERE rn_best = 1
),

serasa_income_only_extracted AS (
  SELECT
    s.credit_simulation_id,
    s.credit_check_id,
    COALESCE(s.data:scoreModel::string, s.data:data:scoreModel::string) AS score_model,
    TRY_TO_NUMBER(COALESCE(s.data:score::string, s.data:data:score::string)) AS raw_value,
    IFF(
      COALESCE(s.data:scoreModel::string, s.data:data:scoreModel::string) ILIKE 'HRP%'
      AND TRY_TO_NUMBER(COALESCE(s.data:score::string, s.data:data:score::string)) > 0
      AND TRY_TO_NUMBER(COALESCE(s.data:score::string, s.data:data:score::string)) <= 100000000,
      (TRY_TO_NUMBER(COALESCE(s.data:score::string, s.data:data:score::string)) / 100.0)::FLOAT,
      NULL
    ) AS income_estimated
  FROM serasa_income_only_best s
),

/* ============================
   Comparações (por fonte)
   ============================ */
comparisons AS (
  SELECT
    e.credit_simulation_id,

    /* cadastro */
    e.borrower_birthdate_source,
    e.borrower_birthdate AS enriched_birthdate,
    sn.birthdate AS extracted_birthdate,
    IFF(
      e.borrower_birthdate_source = 'serasa_new_registration'
      AND sn.birthdate IS NOT NULL,
      IFF(e.borrower_birthdate = sn.birthdate, 0, 1),
      NULL
    ) AS birthdate_mismatch_flag,

    e.borrower_gender_source,
    e.borrower_gender AS enriched_gender,
    sn.gender AS extracted_gender,
    IFF(
      e.borrower_gender_source = 'serasa_new_registration'
      AND sn.gender IS NOT NULL,
      IFF(e.borrower_gender = sn.gender, 0, 1),
      NULL
    ) AS gender_mismatch_flag,

    e.borrower_zipcode_source,
    e.borrower_zipcode AS enriched_zipcode,
    sn.zipcode AS extracted_zipcode,
    IFF(
      e.borrower_zipcode_source = 'serasa_new_registration'
      AND sn.zipcode IS NOT NULL,
      IFF(e.borrower_zipcode = sn.zipcode, 0, 1),
      NULL
    ) AS zipcode_mismatch_flag,

    /* negativação */
    e.negativacao_source,
    e.pefin_count AS enriched_pefin_count,
    sn.pefin_count AS extracted_pefin_count,
    e.refin_count AS enriched_refin_count,
    sn.refin_count AS extracted_refin_count,
    e.protesto_count AS enriched_protesto_count,
    sn.protesto_count AS extracted_protesto_count,
    IFF(
      e.negativacao_source = 'serasa_new_negativeData_summary'
      AND (sn.pefin_count IS NOT NULL OR sn.refin_count IS NOT NULL OR sn.protesto_count IS NOT NULL),
      IFF(
        COALESCE(e.pefin_count, -999999) = COALESCE(sn.pefin_count, -999999)
        AND COALESCE(e.refin_count, -999999) = COALESCE(sn.refin_count, -999999)
        AND COALESCE(e.protesto_count, -999999) = COALESCE(sn.protesto_count, -999999),
        0, 1
      ),
      NULL
    ) AS negativacao_mismatch_flag,

    /* renda (serasa_income_estimated) */
    e.serasa_income_estimated AS enriched_serasa_income_estimated,
    sio.score_model AS extracted_income_score_model,
    sio.raw_value AS extracted_income_raw_value,
    sio.income_estimated AS extracted_serasa_income_estimated,
    IFF(
      e.serasa_income_estimated IS NOT NULL
      AND sio.income_estimated IS NOT NULL,
      IFF(ABS(e.serasa_income_estimated - sio.income_estimated) <= 0.01, 0, 1),
      NULL
    ) AS serasa_income_estimated_mismatch_flag
  FROM enriched e
  LEFT JOIN serasa_new_extracted sn
    ON sn.credit_simulation_id = e.credit_simulation_id
  LEFT JOIN serasa_income_only_extracted sio
    ON sio.credit_simulation_id = e.credit_simulation_id
),

agg AS (
  SELECT
    'correctness' AS metric_group,
    'pct_birthdate_mismatch_when_serasa_new' AS metric_name,
    'all' AS dimension,
    (COUNT_IF(birthdate_mismatch_flag = 1)::FLOAT / NULLIF(COUNT_IF(birthdate_mismatch_flag IS NOT NULL)::FLOAT, 0))::FLOAT AS value_number,
    NULL AS value_string
  FROM comparisons

  UNION ALL

  SELECT
    'correctness','pct_gender_mismatch_when_serasa_new','all',
    (COUNT_IF(gender_mismatch_flag = 1)::FLOAT / NULLIF(COUNT_IF(gender_mismatch_flag IS NOT NULL)::FLOAT, 0))::FLOAT,
    NULL
  FROM comparisons

  UNION ALL

  SELECT
    'correctness','pct_zipcode_mismatch_when_serasa_new','all',
    (COUNT_IF(zipcode_mismatch_flag = 1)::FLOAT / NULLIF(COUNT_IF(zipcode_mismatch_flag IS NOT NULL)::FLOAT, 0))::FLOAT,
    NULL
  FROM comparisons

  UNION ALL

  SELECT
    'correctness','pct_negativacao_mismatch_when_serasa_new_negativeData','all',
    (COUNT_IF(negativacao_mismatch_flag = 1)::FLOAT / NULLIF(COUNT_IF(negativacao_mismatch_flag IS NOT NULL)::FLOAT, 0))::FLOAT,
    NULL
  FROM comparisons

  UNION ALL

  SELECT
    'correctness','pct_serasa_income_estimated_mismatch','all',
    (COUNT_IF(serasa_income_estimated_mismatch_flag = 1)::FLOAT / NULLIF(COUNT_IF(serasa_income_estimated_mismatch_flag IS NOT NULL)::FLOAT, 0))::FLOAT,
    NULL
  FROM comparisons
)

SELECT * FROM agg
UNION ALL
/* amostra de casos problemáticos (debug) */
SELECT
  'debug' AS metric_group,
  'mismatch_row' AS metric_name,
  CAST(credit_simulation_id AS STRING) AS dimension,
  NULL::FLOAT AS value_number,
  OBJECT_CONSTRUCT(
    'birthdate_source', borrower_birthdate_source,
    'enriched_birthdate', enriched_birthdate,
    'extracted_birthdate', extracted_birthdate,
    'gender_source', borrower_gender_source,
    'enriched_gender', enriched_gender,
    'extracted_gender', extracted_gender,
    'zip_source', borrower_zipcode_source,
    'enriched_zip', enriched_zipcode,
    'extracted_zip', extracted_zipcode,
    'neg_source', negativacao_source,
    'enriched_pefin', enriched_pefin_count,
    'extracted_pefin', extracted_pefin_count,
    'enriched_refin', enriched_refin_count,
    'extracted_refin', extracted_refin_count,
    'enriched_protesto', enriched_protesto_count,
    'extracted_protesto', extracted_protesto_count,
    'enriched_serasa_income_estimated', enriched_serasa_income_estimated,
    'income_score_model', extracted_income_score_model,
    'income_raw_value', extracted_income_raw_value,
    'extracted_serasa_income_estimated', extracted_serasa_income_estimated
  )::string AS value_string
FROM comparisons
WHERE COALESCE(birthdate_mismatch_flag, 0) = 1
   OR COALESCE(gender_mismatch_flag, 0) = 1
   OR COALESCE(zipcode_mismatch_flag, 0) = 1
   OR COALESCE(negativacao_mismatch_flag, 0) = 1
   OR COALESCE(serasa_income_estimated_mismatch_flag, 0) = 1
QUALIFY ROW_NUMBER() OVER (ORDER BY UNIFORM(0, 1000000, RANDOM())) <= 50
;


