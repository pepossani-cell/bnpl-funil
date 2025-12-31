/*
  Inventário de tipos/formatos (credit_checks + crivo_checks) por período
  Objetivo: responder “quais tipos existem, como variam e quais paths/keys mudaram”, com anti-cegueira.

  Como usar:
    - Ajuste params.period_start/period_end
    - Rode no Snowflake Worksheet

  Saída: dataset único (linhas) com seções:
    - cc_volume / cc_typeof / cc_top_keys
    - crivo_volume / crivo_typeof / crivo_key_parameters_top_keys / crivo_bureau_campos_top_names
*/

WITH params AS (
  SELECT
    '2025-01-01'::TIMESTAMP_NTZ AS period_start,
    '2025-12-01'::TIMESTAMP_NTZ AS period_end,
    50::INT AS top_n_keys,
    8000::INT AS sample_n_for_flatten
),

/* =========================
   CREDIT CHECKS
   ========================= */
cc_base AS (
  SELECT
    DATE_TRUNC('month', created_at) AS month,
    source,
    COALESCE(kind, 'NULL') AS kind,
    COALESCE(new_data_format, FALSE) AS new_data_format,
    data
  FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
  JOIN params p ON TRUE
  WHERE created_at >= p.period_start
    AND created_at <  p.period_end
),

cc_volume AS (
  SELECT
    'cc_volume' AS section,
    month,
    source,
    kind,
    IFF(new_data_format, 'new', 'old') AS format_flag,
    NULL AS data_typeof,
    NULL AS key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM cc_base
  GROUP BY 1,2,3,4,5
),

cc_typeof AS (
  SELECT
    'cc_typeof' AS section,
    month,
    source,
    kind,
    IFF(new_data_format, 'new', 'old') AS format_flag,
    TYPEOF(data) AS data_typeof,
    NULL AS key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM cc_base
  GROUP BY 1,2,3,4,5,6
),

/* Amostra controlada para reduzir custo de FLATTEN (keys/B-codes/blocos).
   Mantemos cc_volume/cc_typeof em full scan (mais confiável),
   e amostramos apenas o inventário de chaves/estruturas. */
cc_base_flatten_sample AS (
  SELECT *
  FROM cc_base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month, source, kind, new_data_format
    ORDER BY UNIFORM(0, 1000000, RANDOM())
  ) <= (SELECT sample_n_for_flatten FROM params)
),

cc_keys_flat AS (
  SELECT
    b.month,
    b.source,
    b.kind,
    b.new_data_format,
    k.value::string AS key_name
  FROM cc_base_flatten_sample b,
  LATERAL FLATTEN(
    input => IFF(TYPEOF(b.data)='OBJECT', OBJECT_KEYS(b.data), NULL)
  ) k
  WHERE TYPEOF(b.data) = 'OBJECT'
),

cc_top_keys AS (
  SELECT
    'cc_top_keys' AS section,
    month,
    source,
    kind,
    IFF(new_data_format, 'new', 'old') AS format_flag,
    'OBJECT' AS data_typeof,
    key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM cc_keys_flat
  GROUP BY 1,2,3,4,5,6,7
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month, source, kind, format_flag
    ORDER BY COUNT(*) DESC, key_name ASC
  ) <= (SELECT top_n_keys FROM params)
),

/* =========================
   CREDIT CHECKS: SERASA old — inventário de B-codes por mês
   (anti-cegueira: B280/B357 etc podem aparecer/desaparecer ao longo do tempo)
   ========================= */
cc_serasa_old_bcode_keys_flat AS (
  SELECT
    b.month,
    b.source,
    b.kind,
    b.new_data_format,
    k.value::string AS key_name
  FROM cc_base_flatten_sample b,
  LATERAL FLATTEN(input => b.data) elem,
  LATERAL FLATTEN(
    input => IFF(TYPEOF(elem.value)='OBJECT', OBJECT_KEYS(elem.value), NULL)
  ) k
  WHERE b.source = 'serasa'
    AND b.new_data_format = FALSE
    AND TYPEOF(b.data) = 'ARRAY'
    AND TYPEOF(elem.value) = 'OBJECT'
),

cc_serasa_old_bcodes AS (
  SELECT
    'cc_serasa_old_bcodes' AS section,
    month,
    source,
    kind,
    'old' AS format_flag,
    'ARRAY' AS data_typeof,
    key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM cc_serasa_old_bcode_keys_flat
  GROUP BY 1,2,3,4,5,6,7
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month, source, kind
    ORDER BY COUNT(*) DESC, key_name ASC
  ) <= (SELECT top_n_keys FROM params)
),

/* =========================
   CREDIT CHECKS: BOA VISTA scpc_net — inventário de “blocos” por mês (141/249/...)
   ========================= */
cc_bvs_scpc_net_block_keys_flat AS (
  SELECT
    b.month,
    b.source,
    b.kind,
    b.new_data_format,
    k.value::string AS key_name
  FROM cc_base_flatten_sample b,
  LATERAL FLATTEN(input => b.data) elem,
  LATERAL FLATTEN(
    input => IFF(TYPEOF(elem.value)='OBJECT', OBJECT_KEYS(elem.value), NULL)
  ) k
  WHERE b.source = 'boa_vista_scpc_net'
    AND TYPEOF(b.data) = 'ARRAY'
    AND TYPEOF(elem.value) = 'OBJECT'
),

cc_bvs_scpc_net_blocks AS (
  SELECT
    'cc_bvs_scpc_net_blocks' AS section,
    month,
    source,
    kind,
    IFF(new_data_format, 'new', 'old') AS format_flag,
    'ARRAY' AS data_typeof,
    key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM cc_bvs_scpc_net_block_keys_flat
  GROUP BY 1,2,3,4,5,6,7
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month, source, kind
    ORDER BY COUNT(*) DESC, key_name ASC
  ) <= (SELECT top_n_keys FROM params)
),

/* =========================
   CRIVO CHECKS (staging)
   ========================= */
crivo_base AS (
  SELECT
    DATE_TRUNC('month', crivo_check_created_at) AS month,
    engineable_type,
    key_parameters,
    bureau_check_info
  FROM CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS
  JOIN params p ON TRUE
  WHERE crivo_check_created_at >= p.period_start
    AND crivo_check_created_at <  p.period_end
),

crivo_volume AS (
  SELECT
    'crivo_volume' AS section,
    month,
    engineable_type AS source,
    NULL AS kind,
    NULL AS format_flag,
    NULL AS data_typeof,
    NULL AS key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM crivo_base
  GROUP BY 1,2,3
),

crivo_typeof AS (
  SELECT
    'crivo_typeof' AS section,
    month,
    engineable_type AS source,
    'key_parameters' AS kind,
    NULL AS format_flag,
    TYPEOF(key_parameters) AS data_typeof,
    NULL AS key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM crivo_base
  GROUP BY 1,2,3,4,5,6

  UNION ALL

  SELECT
    'crivo_typeof' AS section,
    month,
    engineable_type AS source,
    'bureau_check_info' AS kind,
    NULL AS format_flag,
    TYPEOF(bureau_check_info) AS data_typeof,
    NULL AS key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM crivo_base
  GROUP BY 1,2,3,4,5,6
),

crivo_base_flatten_sample AS (
  SELECT *
  FROM crivo_base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month, engineable_type
    ORDER BY UNIFORM(0, 1000000, RANDOM())
  ) <= (SELECT sample_n_for_flatten FROM params)
),

crivo_key_parameters_keys_flat AS (
  SELECT
    b.month,
    b.engineable_type,
    k.value::string AS key_name
  FROM crivo_base_flatten_sample b,
  LATERAL FLATTEN(
    input => IFF(TYPEOF(b.key_parameters:campos)='OBJECT', OBJECT_KEYS(b.key_parameters:campos), NULL)
  ) k
  WHERE TYPEOF(b.key_parameters:campos) = 'OBJECT'
),

crivo_key_parameters_top_keys AS (
  SELECT
    'crivo_key_parameters_top_keys' AS section,
    month,
    engineable_type AS source,
    'key_parameters.campos' AS kind,
    NULL AS format_flag,
    'OBJECT' AS data_typeof,
    key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM crivo_key_parameters_keys_flat
  GROUP BY 1,2,3,4,5,6,7
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month, engineable_type
    ORDER BY COUNT(*) DESC, key_name ASC
  ) <= (SELECT top_n_keys FROM params)
),

crivo_bureau_campos_names_flat AS (
  SELECT
    b.month,
    b.engineable_type,
    NULLIF(TRIM(f.value:nome::string), '') AS key_name
  FROM crivo_base_flatten_sample b,
  LATERAL FLATTEN(input => b.bureau_check_info:campos) f
  WHERE b.bureau_check_info:campos IS NOT NULL
),

crivo_bureau_campos_top_names AS (
  SELECT
    'crivo_bureau_campos_top_names' AS section,
    month,
    engineable_type AS source,
    'bureau_check_info.campos.nome' AS kind,
    NULL AS format_flag,
    'ARRAY' AS data_typeof,
    key_name,
    COUNT(*)::NUMBER AS n_rows,
    NULL AS example
  FROM crivo_bureau_campos_names_flat
  WHERE key_name IS NOT NULL
  GROUP BY 1,2,3,4,5,6,7
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month, engineable_type
    ORDER BY COUNT(*) DESC, key_name ASC
  ) <= (SELECT top_n_keys FROM params)
)

SELECT * FROM cc_volume
UNION ALL SELECT * FROM cc_typeof
UNION ALL SELECT * FROM cc_top_keys
UNION ALL SELECT * FROM cc_serasa_old_bcodes
UNION ALL SELECT * FROM cc_bvs_scpc_net_blocks
UNION ALL SELECT * FROM crivo_volume
UNION ALL SELECT * FROM crivo_typeof
UNION ALL SELECT * FROM crivo_key_parameters_top_keys
UNION ALL SELECT * FROM crivo_bureau_campos_top_names
;


