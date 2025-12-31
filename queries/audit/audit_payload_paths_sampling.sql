/*
  Auditoria amostral de "contratos" de payload (credit checks + crivo)
  Objetivo: mitigar cegueiras por mudanças de formato/path em JSONs.

  Resultado: tabela "tidy" com métricas por source/kind/formato e por path.
  - metric_group: credit_checks | crivo
  - metric_name: ex. pct_score_path_filled, pct_registration_birthdate_filled
  - dimension: ex. "serasa|new|check_score_without_income|report_score_object"

  Como usar:
    - Ajuste params_user.period_start/period_end e sample_n por período
    - Rode no Snowflake Worksheet
*/

WITH params_periods AS (
  /* 3 janelas para capturar formatos antigos/novos */
  SELECT 'p2022_07' AS period_label, '2022-07-01'::TIMESTAMP_NTZ AS period_start, '2022-08-01'::TIMESTAMP_NTZ AS period_end, 20000::INT AS sample_n
  UNION ALL
  SELECT 'p2023_03', '2023-03-01'::TIMESTAMP_NTZ, '2023-04-01'::TIMESTAMP_NTZ, 20000::INT
  UNION ALL
  SELECT 'p2025_09', '2025-09-01'::TIMESTAMP_NTZ, '2025-10-01'::TIMESTAMP_NTZ, 20000::INT
),

/* =========================
   CREDIT CHECKS: amostra
   ========================= */
cc_sample AS (
  SELECT
    p.period_label,
    cc.id,
    cc.source,
    cc.kind,
    COALESCE(cc.new_data_format, FALSE) AS new_data_format,
    cc.created_at,
    cc.data
  FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
  JOIN params_periods p ON TRUE
  WHERE cc.created_at >= p.period_start
    AND cc.created_at <  p.period_end
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.period_label ORDER BY UNIFORM(0, 1000000, RANDOM())) <= p.sample_n
),

cc_counts AS (
  SELECT
    'credit_checks' AS metric_group,
    'n_sample' AS metric_name,
    CONCAT(period_label, '|', source, '|', IFF(new_data_format,'new','old'), '|', COALESCE(kind,'NULL')) AS dimension,
    COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  GROUP BY 1,2,3
),

cc_typeof AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_typeof_data' AS metric_name,
    CONCAT(period_label, '|', source, '|', IFF(new_data_format,'new','old'), '|', COALESCE(kind,'NULL'), '|', TYPEOF(data)) AS dimension,
    (COUNT(*)::FLOAT / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY period_label, source, new_data_format, kind), 0))::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  GROUP BY 1,2,3, period_label, source, new_data_format, kind, TYPEOF(data)
),

/* Exemplo de chaves top-level por combinação (anti-cegueira: mudanças de contrato) */
cc_keys_example AS (
  SELECT
    'credit_checks' AS metric_group,
    'top_level_keys_example' AS metric_name,
    CONCAT(period_label, '|', source, '|', IFF(new_data_format,'new','old'), '|', COALESCE(kind,'NULL')) AS dimension,
    NULL AS value_number,
    CASE
      WHEN TYPEOF(data) = 'OBJECT' THEN ARRAY_TO_STRING(OBJECT_KEYS(data), ',')
      WHEN TYPEOF(data) = 'ARRAY' AND TYPEOF(data[0]) = 'OBJECT' THEN ARRAY_TO_STRING(OBJECT_KEYS(data[0]), ',')
      ELSE CONCAT('TYPEOF=', TYPEOF(data))
    END AS value_string
  FROM cc_sample
  QUALIFY ROW_NUMBER() OVER (PARTITION BY period_label, source, new_data_format, kind ORDER BY UNIFORM(0, 1000000, RANDOM())) = 1
),

/* ===== SERASA (novo): score paths ===== */
serasa_new_score_paths AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_score_path_filled' AS metric_name,
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|top_level') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(data:score::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind

  UNION ALL
  SELECT
    'credit_checks','pct_score_path_filled',
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|data_block') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(data:data:score::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM cc_sample
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind

  UNION ALL
  /* report.score como escalar */
  SELECT
    'credit_checks','pct_score_path_filled',
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|report_score_scalar') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(r.value:score::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind

  UNION ALL
  /* report.score como objeto {score, range, scoreModel,...} */
  SELECT
    'credit_checks','pct_score_path_filled',
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|report_score_object') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(r.value:score:score::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind
),

/* ===== SERASA (novo): check_income_only — scoreModel + semântica (score vs renda) ===== */
serasa_income_only_models AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_serasa_income_only_model' AS metric_name,
    CONCAT(
      period_label,
      '|serasa|new|check_income_only|scoreModel=',
      COALESCE(data:scoreModel::string, data:data:scoreModel::string, 'NULL')
    ) AS dimension,
    (COUNT(*)::FLOAT / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY period_label), 0))::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'serasa'
    AND new_data_format = TRUE
    AND kind = 'check_income_only'
  GROUP BY period_label, COALESCE(data:scoreModel::string, data:data:scoreModel::string, 'NULL')
),

serasa_income_only_semantics AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_serasa_income_only_hrp_like' AS metric_name,
    CONCAT(period_label, '|serasa|new|check_income_only|scoreModel_ILIKE_HRP%') AS dimension,
    (COUNT_IF(COALESCE(data:scoreModel::string, data:data:scoreModel::string) ILIKE 'HRP%')::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'serasa'
    AND new_data_format = TRUE
    AND kind = 'check_income_only'
  GROUP BY period_label

  UNION ALL

  SELECT
    'credit_checks' AS metric_group,
    'pct_serasa_income_only_score_present' AS metric_name,
    CONCAT(period_label, '|serasa|new|check_income_only|score_present') AS dimension,
    (COUNT_IF(TRY_TO_NUMBER(COALESCE(data:score::string, data:data:score::string)) IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'serasa'
    AND new_data_format = TRUE
    AND kind = 'check_income_only'
  GROUP BY period_label

  UNION ALL

  SELECT
    'credit_checks' AS metric_group,
    'serasa_income_only_score_raw_p50' AS metric_name,
    CONCAT(period_label, '|serasa|new|check_income_only|score_raw') AS dimension,
    APPROX_PERCENTILE(TRY_TO_NUMBER(COALESCE(data:score::string, data:data:score::string)), 0.5)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'serasa'
    AND new_data_format = TRUE
    AND kind = 'check_income_only'
  GROUP BY period_label
),

/* ===== SERASA (novo): registration birthDate ===== */
serasa_new_registration_paths AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_registration_birthdate_filled' AS metric_name,
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|registration_birthDate') AS dimension,
    COUNT_IF(TRY_TO_DATE(r.value:registration:birthDate::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_registration_gender_filled' AS metric_name,
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|registration_consumerGender') AS dimension,
    COUNT_IF(NULLIF(r.value:registration:consumerGender::string,'') IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_registration_zipcode_filled' AS metric_name,
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|registration_address_zipCode') AS dimension,
    COUNT_IF(NULLIF(r.value:registration:address:zipCode::string,'') IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind
),

/* ===== SERASA (novo): negativeData summary ===== */
serasa_new_negative_paths AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_negative_summary_any_count_filled' AS metric_name,
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|negativeData_summary_any') AS dimension,
    COUNT_IF(
      TRY_TO_NUMBER(r.value:negativeData:pefin:summary:count::string) IS NOT NULL
      OR TRY_TO_NUMBER(r.value:negativeData:refin:summary:count::string) IS NOT NULL
      OR TRY_TO_NUMBER(r.value:negativeData:notary:summary:count::string) IS NOT NULL
      OR TRY_TO_NUMBER(r.value:negativeData:check:summary:count::string) IS NOT NULL
    )::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_negative_summary_any_balance_filled' AS metric_name,
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|negativeData_summary_any_balance') AS dimension,
    COUNT_IF(
      TRY_TO_NUMBER(r.value:negativeData:pefin:summary:balance::string) IS NOT NULL
      OR TRY_TO_NUMBER(r.value:negativeData:refin:summary:balance::string) IS NOT NULL
      OR TRY_TO_NUMBER(r.value:negativeData:notary:summary:balance::string) IS NOT NULL
      OR TRY_TO_NUMBER(r.value:negativeData:check:summary:balance::string) IS NOT NULL
    )::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_facts_inquiry_present' AS metric_name,
    CONCAT(period_label, '|serasa|new|', COALESCE(kind,'NULL'), '|facts_inquiry') AS dimension,
    COUNT_IF(r.value:facts:inquiry IS NOT NULL OR r.value:facts:inquirySummary IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data:reports) r
  WHERE source = 'serasa' AND new_data_format = TRUE
  GROUP BY period_label, kind
),

/* ===== SERASA (antigo): B280 score + B357/B361 value ===== */
serasa_old_paths AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_serasa_old_b280_score_filled' AS metric_name,
    CONCAT(period_label, '|serasa|old|', COALESCE(kind,'NULL'), '|b280_score') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(f.value:"B280":score::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data) f
  WHERE source = 'serasa' AND new_data_format = FALSE
  GROUP BY period_label, kind

  UNION ALL
  SELECT
    'credit_checks','pct_serasa_old_b357_total_value_filled',
    CONCAT(period_label, '|serasa|old|', COALESCE(kind,'NULL'), '|b357_total_occurrence_value') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(f.value:"B357":total_occurrence_value::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data) f
  WHERE source = 'serasa' AND new_data_format = FALSE
  GROUP BY period_label, kind

  UNION ALL
  SELECT
    'credit_checks','pct_serasa_old_b361_total_value_filled',
    CONCAT(period_label, '|serasa|old|', COALESCE(kind,'NULL'), '|b361_total_occurrence_value') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(f.value:"B361":total_occurrence_value::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM cc_sample
  , LATERAL FLATTEN(input => cc_sample.data) f
  WHERE source = 'serasa' AND new_data_format = FALSE
  GROUP BY period_label, kind
),

/* ===== BOA VISTA score_pf: score path ===== */
bvs_score_pf_paths AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_bvs_score_pf_score_filled' AS metric_name,
    CONCAT(period_label, '|boa_vista_score_pf|', COALESCE(kind,'NULL'), '|score_classificacao_varios_modelos.score') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(data:score_positivo:score_classificacao_varios_modelos:score::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'boa_vista_score_pf'
  GROUP BY period_label, kind
),

/* ===== BOA VISTA scpc_net: birthdate + debit_total_value ===== */
bvs_scpc_net_paths AS (
  /* Importante:
     - `data` é ARRAY com vários blocos por check.
     - Medir fill-rate dividindo por COUNT(*) após FLATTEN distorce (denominador vira “n_blocos”).
     - Aqui medimos por CHECK (1 linha = 1 cc.id). */
  WITH per_check AS (
    SELECT
      s.period_label,
      s.id,
      MAX(IFF(TRY_TO_DATE(f.value:"249":birthdate::string, 'DDMMYYYY') IS NOT NULL, 1, 0)) AS has_birthdate,
      MAX(IFF(TRY_TO_NUMBER(f.value:"141":debit_total_value::string) IS NOT NULL, 1, 0)) AS has_debit_total_value,
      MAX(IFF(NULLIF(f.value:"123":exists::string,'') IS NOT NULL, 1, 0)) AS has_exists_123,
      MAX(IFF(NULLIF(f.value:"141":last_debit_date::string,'') IS NOT NULL, 1, 0)) AS has_last_debit_date
    FROM cc_sample s
    , LATERAL FLATTEN(input => s.data) f
    WHERE s.source = 'boa_vista_scpc_net'
    GROUP BY 1,2
  )
  SELECT
    'credit_checks' AS metric_group,
    'pct_bvs_scpc_net_birthdate_filled' AS metric_name,
    CONCAT(period_label, '|boa_vista_scpc_net|block_249.birthdate') AS dimension,
    AVG(has_birthdate)::FLOAT AS value_number,
    NULL AS value_string
  FROM per_check
  GROUP BY period_label

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_bvs_scpc_net_debit_total_value_filled' AS metric_name,
    CONCAT(period_label, '|boa_vista_scpc_net|block_141.debit_total_value') AS dimension,
    AVG(has_debit_total_value)::FLOAT AS value_number,
    NULL AS value_string
  FROM per_check
  GROUP BY period_label

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_bvs_scpc_net_flag_exists_123_present' AS metric_name,
    CONCAT(period_label, '|boa_vista_scpc_net|block_123.exists') AS dimension,
    AVG(has_exists_123)::FLOAT AS value_number,
    NULL AS value_string
  FROM per_check
  GROUP BY period_label

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_bvs_scpc_net_last_debit_date_present' AS metric_name,
    CONCAT(period_label, '|boa_vista_scpc_net|block_141.last_debit_date') AS dimension,
    AVG(has_last_debit_date)::FLOAT AS value_number,
    NULL AS value_string
  FROM per_check
  GROUP BY period_label
),

/* ===== BACEN internal: predictions[0].score ===== */
bacen_paths AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_bacen_internal_score_filled' AS metric_name,
    CONCAT(period_label, '|bacen_internal_score|predictions[0].score') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(data:predictions[0]:score::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'bacen_internal_score'
  GROUP BY period_label

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_bacen_is_not_banked_present' AS metric_name,
    CONCAT(period_label, '|bacen_internal_score|predictions[0].is_not_banked') AS dimension,
    COUNT_IF(data:predictions[0]:is_not_banked IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM cc_sample
  WHERE source = 'bacen_internal_score'
  GROUP BY period_label
),

/* ===== SCR: presença de blocos chaves ===== */
scr_paths AS (
  SELECT
    'credit_checks' AS metric_group,
    'pct_scr_has_lista_resumo_operacoes' AS metric_name,
    CONCAT(period_label, '|scr|data.listaDeResumoDasOperacoes') AS dimension,
    COUNT_IF(data:listaDeResumoDasOperacoes IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0) AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'scr'
  GROUP BY period_label

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_scr_has_resumo_do_cliente' AS metric_name,
    CONCAT(period_label, '|scr|data.resumoDoCliente') AS dimension,
    COUNT_IF(data:resumoDoCliente IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0) AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'scr'
  GROUP BY period_label

  UNION ALL
  /* alguns períodos antigos usam chaves com inicial maiúscula (ex.: ResumoDoCliente) */
  SELECT
    'credit_checks' AS metric_group,
    'pct_scr_has_resumo_do_cliente' AS metric_name,
    CONCAT(period_label, '|scr|data.ResumoDoCliente') AS dimension,
    COUNT_IF(data:ResumoDoCliente IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0) AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'scr'
  GROUP BY period_label

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_scr_has_lista_resumo_operacoes_nested' AS metric_name,
    CONCAT(period_label, '|scr|data.resumoDoCliente.listaDeResumoDasOperacoes') AS dimension,
    COUNT_IF(data:resumoDoCliente:listaDeResumoDasOperacoes IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0) AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'scr'
  GROUP BY period_label

  UNION ALL
  SELECT
    'credit_checks' AS metric_group,
    'pct_scr_has_lista_resumo_operacoes_nested' AS metric_name,
    CONCAT(period_label, '|scr|data.ResumoDoCliente.ListaDeResumoDasOperacoes') AS dimension,
    COUNT_IF(data:ResumoDoCliente:ListaDeResumoDasOperacoes IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0) AS value_number,
    NULL AS value_string
  FROM cc_sample
  WHERE source = 'scr'
  GROUP BY period_label
),

/* =========================
   CRIVO: amostra + campos relevantes
   ========================= */
crivo_sample AS (
  SELECT
    p.period_label,
    c.crivo_check_id,
    c.crivo_check_created_at,
    c.key_parameters,
    c.bureau_check_info
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS c
  JOIN params_periods p ON TRUE
  WHERE c.crivo_check_created_at >= p.period_start
    AND c.crivo_check_created_at <  p.period_end
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.period_label ORDER BY UNIFORM(0, 1000000, RANDOM())) <= p.sample_n
),

crivo_paths AS (
  SELECT
    'crivo' AS metric_group,
    'pct_key_parameters_bacen_score_filled' AS metric_name,
    CONCAT(period_label, '|crivo|key_parameters.campos.BacenScore') AS dimension,
    COUNT_IF(TRY_TO_NUMBER(key_parameters:campos:BacenScore::string) IS NOT NULL)::FLOAT / COUNT(*)::FLOAT AS value_number,
    NULL AS value_string
  FROM crivo_sample
  GROUP BY period_label

  UNION ALL
  SELECT
    'crivo','pct_key_parameters_credit_limits_filled',
    CONCAT(period_label, '|crivo|key_parameters.campos.CreditLimits') AS dimension,
    COUNT_IF(key_parameters:campos:CreditLimits IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM crivo_sample
  GROUP BY period_label

  UNION ALL
  SELECT
    'crivo','pct_bureau_campos_birthdate_bvs_filled',
    CONCAT(period_label, '|crivo|bureau_check_info.campos.Data_de_Nascimento_BVS') AS dimension,
    COUNT_IF(
      TRY_TO_DATE(
        NULLIF(TRIM(f.value:valor::string),''),
        'DD/MM/YYYY'
      ) IS NOT NULL
    )::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM crivo_sample
  , LATERAL FLATTEN(input => crivo_sample.bureau_check_info:campos) f
  WHERE f.value:nome::string = 'Data de Nascimento BVS'
  GROUP BY period_label

  UNION ALL
  SELECT
    'crivo' AS metric_group,
    'pct_key_parameters_overdue_portfolio_present' AS metric_name,
    CONCAT(period_label, '|crivo|key_parameters.campos.OverduePortfolio') AS dimension,
    COUNT_IF(key_parameters:campos:OverduePortfolio IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM crivo_sample
  GROUP BY period_label

  UNION ALL
  SELECT
    'crivo' AS metric_group,
    'pct_key_parameters_loss_present' AS metric_name,
    CONCAT(period_label, '|crivo|key_parameters.campos.Loss') AS dimension,
    COUNT_IF(key_parameters:campos:Loss IS NOT NULL)::FLOAT / COUNT(*)::FLOAT,
    NULL
  FROM crivo_sample
  GROUP BY period_label
)

SELECT * FROM cc_counts
UNION ALL SELECT * FROM cc_typeof
UNION ALL SELECT * FROM cc_keys_example
UNION ALL SELECT * FROM serasa_new_score_paths
UNION ALL SELECT * FROM serasa_income_only_models
UNION ALL SELECT * FROM serasa_income_only_semantics
UNION ALL SELECT * FROM serasa_new_registration_paths
UNION ALL SELECT * FROM serasa_new_negative_paths
UNION ALL SELECT * FROM serasa_old_paths
UNION ALL SELECT * FROM bvs_score_pf_paths
UNION ALL SELECT * FROM bvs_scpc_net_paths
UNION ALL SELECT * FROM bacen_paths
UNION ALL SELECT * FROM scr_paths
UNION ALL SELECT * FROM crivo_paths
;


