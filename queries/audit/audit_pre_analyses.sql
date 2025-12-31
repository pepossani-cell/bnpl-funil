/*
  Auditoria Snowflake-first da tabela CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES
  Objetivo: responder empiricamente (cobertura, grão, duplicidade, semântica por eixo)
  se PRE_ANALYSES pode ser usada como:
    (A) canônica de C1 (simulações),
    (B) auxiliar/bridge,
    (C) descartada para análises finas.

  Regras/guardrails:
    - Não assumir "fonte da verdade": validar por cobertura e consistência temporal.
    - Null vs zero: NULL = não observado; 0 = observado e zero.
    - LGPD: evitar persistir PII crua; aqui só auditamos no Worksheet (sem materialização).
    - Anti-cegueira: medir TYPEOF + top keys para colunas JSON/VARIANT por mês.

  Como usar (recomendado):
    1) Rode a seção [A0] (INFORMATION_SCHEMA) e ajuste os SETs abaixo (nomes de colunas).
    2) Rode [A1..A4] para volume + estrutura.
    3) Rode [B1..B4] para grão/duplicidade/reprocessamento.
    4) Rode [C1..C3] para cobertura vs CREDIT_SIMULATIONS (por mês/clinic/state).
    5) Rode [D1..D4] para mapear sinais por eixo e gerar queries de fill-rate/sanidade.
    6) (Opcional) Rode [Z] periodicamente (anti-cegueira) e monitore quedas de fill-rate/TYPEOF.

  Importante:
    - Este arquivo é "SQL-first": múltiplas queries independentes (vários result sets).
    - Ajuste os parâmetros via SET (variáveis de sessão).
*/

/* =========================================================
   PARAMS (ajuste após rodar a seção A0)
   ========================================================= */
SET pre_analyses_table = 'CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES';
SET cs_table           = 'CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS';

/* Período "recente" para cobertura (últimos N meses completos) */
SET months_back = 12;

/* EXPRESSÕES (com alias) para evitar ambiguidade com IDENTIFIER().
   Ajuste para as colunas reais descobertas em A0.

   Exemplos comuns:
     - pre ts: pa.CREATED_AT
     - cs  ts: cs.CREATED_AT
     - join por simulation: cs.ID = pa.CREDIT_SIMULATION_ID
     - join por lead      : cs.CREDIT_LEAD_ID = pa.CREDIT_LEAD_ID
*/
/* PRE_ANALYSES (schema real observado): PRE_ANALYSIS_CREATED_AT / RETAIL_ID / CREDIT_LEAD_ID / PRE_ANALYSIS_ID */
SET pa_ts_expr      = 'pa.PRE_ANALYSIS_CREATED_AT';
SET cs_ts_expr      = 'cs.CREATED_AT';
SET cs_clinic_expr  = 'cs.RETAIL_ID';   -- clinic_id em CREDIT_SIMULATIONS
SET pa_clinic_expr  = 'pa.RETAIL_ID';   -- clinic_id em PRE_ANALYSES (mesma semântica do cs.RETAIL_ID)

/* Cobertura vs CREDIT_SIMULATIONS: PRE_ANALYSES não tem credit_simulation_id; join mais direto é via CREDIT_LEAD_ID */
SET cs_join_expr    = 'cs.CREDIT_LEAD_ID';
SET pa_join_expr    = 'pa.CREDIT_LEAD_ID';

/* Key principal hipotética da PRE_ANALYSES (para grão). Ajuste/ignore. */
SET pa_pk_expr      = 'pa.PRE_ANALYSIS_ID';

/* Um ou mais JSONs/VARIANT relevantes (ajuste/ignore).
   Exemplos: pa.PAYLOAD, pa.REQUEST, pa.RESPONSE, pa.DATA, pa.METADATA
*/
/* Únicas colunas VARIANT na PRE_ANALYSES (observadas): FINANCING_CONDITIONS, INTEREST_RATES_ARRAY */
SET pa_json_expr    = 'pa.FINANCING_CONDITIONS';


/* =========================================================
   [A0] INVENTÁRIO ESTRUTURAL (INFORMATION_SCHEMA)
   ========================================================= */
SELECT
  c.ordinal_position,
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema = 'CAPIM_ANALYTICS'
  AND c.table_name   = 'PRE_ANALYSES'
ORDER BY c.ordinal_position
;

/* Sugestão de colunas “candidatas” por nome (IDs / timestamps / clinic etc.) */
SELECT
  c.column_name,
  c.data_type,
  c.is_nullable,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema = 'CAPIM_ANALYTICS'
  AND c.table_name   = 'PRE_ANALYSES'
  AND (
    c.column_name ILIKE '%ID%'
    OR c.column_name ILIKE '%SIMULATION%'
    OR c.column_name ILIKE '%LEAD%'
    OR c.column_name ILIKE '%PATIENT%'
    OR c.column_name ILIKE '%RESPONS%'
    OR c.column_name ILIKE '%CPF%'
    OR c.column_name ILIKE '%DOCUMENT%'
    OR c.column_name ILIKE '%CREATED%'
    OR c.column_name ILIKE '%UPDATED%'
    OR c.column_name ILIKE '%INSERT%'
    OR c.column_name ILIKE '%TIMESTAMP%'
    OR c.column_name ILIKE '%CLINIC%'
    OR c.column_name ILIKE '%RETAIL%'
  )
ORDER BY c.column_name
;

/* Colunas VARIANT/OBJECT/ARRAY (potenciais JSONs) */
SELECT
  c.column_name,
  c.data_type,
  c.comment
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema = 'CAPIM_ANALYTICS'
  AND c.table_name   = 'PRE_ANALYSES'
  AND c.data_type IN ('VARIANT','OBJECT','ARRAY')
ORDER BY c.column_name
;


/* =========================================================
   [A1] VOLUME TOTAL + RANGE TEMPORAL
   ========================================================= */
SELECT
  COUNT(*)::NUMBER AS n_rows,
  MIN(IDENTIFIER($pa_ts_expr)) AS min_ts,
  MAX(IDENTIFIER($pa_ts_expr)) AS max_ts
FROM IDENTIFIER($pre_analyses_table) pa
;

/* =========================================================
   [A2] VOLUME POR MÊS (recorte barato e essencial)
   ========================================================= */
SELECT
  DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month,
  COUNT(*)::NUMBER AS n_rows
FROM IDENTIFIER($pre_analyses_table) pa
GROUP BY 1
ORDER BY 1
;

/* Volume por mês + “fatias” por clinic (se existir na PRE_ANALYSES) */
SELECT
  DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month,
  IDENTIFIER($pa_clinic_expr) AS clinic_id,
  COUNT(*)::NUMBER AS n_rows
FROM IDENTIFIER($pre_analyses_table) pa
GROUP BY 1,2
ORDER BY 1,3 DESC
;


/* =========================================================
   [A3] ESTRUTURA DE JSON (TYPEOF + top keys) — amostrado por mês
   - Anti-cegueira: detectar mudanças de contrato.
   ========================================================= */
WITH params AS (
  SELECT
    /* amostra por mês para reduzir custo de FLATTEN */
    5000::INT AS sample_n_per_month,
    50::INT   AS top_n_keys
),
base AS (
  SELECT
    DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month,
    IDENTIFIER($pa_json_expr) AS j
  FROM IDENTIFIER($pre_analyses_table) pa
  WHERE IDENTIFIER($pa_ts_expr) >= DATEADD('month', -TO_NUMBER($months_back), DATE_TRUNC('month', CURRENT_DATE()))
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DATE_TRUNC('month', IDENTIFIER($pa_ts_expr))
    ORDER BY UNIFORM(0, 1000000, RANDOM())
  ) <= (SELECT sample_n_per_month FROM params)
),
typeof_dist AS (
  SELECT
    'typeof' AS section,
    month,
    TYPEOF(j) AS j_typeof,
    NULL::STRING AS key_name,
    COUNT(*)::NUMBER AS n_rows
  FROM base
  GROUP BY 1,2,3,4
),
keys_flat AS (
  SELECT
    b.month,
    k.value::string AS key_name
  FROM base b,
  LATERAL FLATTEN(
    input => IFF(TYPEOF(b.j)='OBJECT', OBJECT_KEYS(b.j), NULL)
  ) k
  WHERE TYPEOF(b.j) = 'OBJECT'
),
top_keys AS (
  SELECT
    'top_keys' AS section,
    month,
    'OBJECT' AS j_typeof,
    key_name,
    COUNT(*)::NUMBER AS n_rows
  FROM keys_flat
  GROUP BY 1,2,3,4
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY month
    ORDER BY COUNT(*) DESC, key_name ASC
  ) <= (SELECT top_n_keys FROM params)
)
SELECT * FROM typeof_dist
UNION ALL
SELECT * FROM top_keys
ORDER BY month, section, n_rows DESC
;


/* =========================================================
   [B1] GRÃO (chave primária “provável”) + duplicidade
   ========================================================= */
SELECT
  COUNT(*)::NUMBER AS n_rows,
  COUNT(DISTINCT IDENTIFIER($pa_pk_expr))::NUMBER AS n_distinct_pa_pk,
  (COUNT(*) - COUNT(DISTINCT IDENTIFIER($pa_pk_expr)))::NUMBER AS n_duplicate_rows_over_pk,
  (COUNT(*)::FLOAT / NULLIF(COUNT(DISTINCT IDENTIFIER($pa_pk_expr))::FLOAT, 0))::FLOAT AS rows_per_pk
FROM IDENTIFIER($pre_analyses_table) pa
;

/* =========================================================
   [B2] DISTRIBUIÇÃO DE DUPLICIDADE (quantas vezes o mesmo pk aparece)
   ========================================================= */
WITH dup AS (
  SELECT
    IDENTIFIER($pa_pk_expr) AS pk,
    COUNT(*)::NUMBER AS n_rows_for_pk,
    MIN(IDENTIFIER($pa_ts_expr)) AS min_ts,
    MAX(IDENTIFIER($pa_ts_expr)) AS max_ts
  FROM IDENTIFIER($pre_analyses_table) pa
  GROUP BY 1
  HAVING COUNT(*) > 1
)
SELECT
  COUNT(*)::NUMBER AS n_pk_with_duplicates,
  MAX(n_rows_for_pk)::NUMBER AS max_rows_for_same_pk,
  APPROX_PERCENTILE(n_rows_for_pk, 0.50)::FLOAT AS p50_rows_for_same_pk,
  APPROX_PERCENTILE(n_rows_for_pk, 0.90)::FLOAT AS p90_rows_for_same_pk,
  APPROX_PERCENTILE(n_rows_for_pk, 0.99)::FLOAT AS p99_rows_for_same_pk,
  /* “reprocessamento”: mesma chave em timestamps bem diferentes */
  COUNT_IF(DATEDIFF('day', min_ts, max_ts) >= 1)::NUMBER AS n_pk_with_ts_span_ge_1d,
  COUNT_IF(DATEDIFF('day', min_ts, max_ts) >= 7)::NUMBER AS n_pk_with_ts_span_ge_7d
FROM dup
;

/* =========================================================
   [B3] DUPLICIDADE POR MÊS E CLÍNICA (padrões de ingestão)
   ========================================================= */
WITH per_pk_month AS (
  SELECT
    DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month,
    IDENTIFIER($pa_clinic_expr) AS clinic_id,
    IDENTIFIER($pa_pk_expr) AS pk,
    COUNT(*)::NUMBER AS n_rows
  FROM IDENTIFIER($pre_analyses_table) pa
  GROUP BY 1,2,3
)
SELECT
  month,
  clinic_id,
  COUNT(*)::NUMBER AS n_pk,
  COUNT_IF(n_rows > 1)::NUMBER AS n_pk_with_dups,
  (COUNT_IF(n_rows > 1)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_pk_with_dups,
  MAX(n_rows)::NUMBER AS max_rows_for_pk
FROM per_pk_month
GROUP BY 1,2
ORDER BY month, pct_pk_with_dups DESC, max_rows_for_pk DESC
;


/* =========================================================
   [C1] COBERTURA vs CREDIT_SIMULATIONS (últimos N meses)
   - % de credit_simulations que aparecem em pre_analyses
   - % de pre_analyses que mapeiam para credit_simulations
   ========================================================= */
WITH params AS (
  SELECT DATEADD('month', -TO_NUMBER($months_back), DATE_TRUNC('month', CURRENT_DATE())) AS start_month
),
cs_recent AS (
  SELECT
    cs.*,
    DATE_TRUNC('month', IDENTIFIER($cs_ts_expr)) AS month,
    IDENTIFIER($cs_clinic_expr) AS clinic_id
  FROM IDENTIFIER($cs_table) cs
  WHERE IDENTIFIER($cs_ts_expr) >= (SELECT start_month FROM params)
),
pa_recent AS (
  SELECT
    pa.*,
    DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month,
    IDENTIFIER($pa_clinic_expr) AS clinic_id
  FROM IDENTIFIER($pre_analyses_table) pa
  WHERE IDENTIFIER($pa_ts_expr) >= (SELECT start_month FROM params)
),
joined AS (
  SELECT
    cs.month AS cs_month,
    cs.clinic_id AS cs_clinic_id,
    cs.state AS cs_state,
    /* chaves (presença) */
    IFF(IDENTIFIER($pa_join_expr) IS NULL, 0, 1) AS has_pa
  FROM cs_recent cs
  LEFT JOIN pa_recent pa
    ON IDENTIFIER($cs_join_expr) = IDENTIFIER($pa_join_expr)
)
SELECT
  cs_month AS month,
  cs_clinic_id AS clinic_id,
  cs_state AS credit_simulation_state,
  COUNT(*)::NUMBER AS n_credit_simulations,
  SUM(has_pa)::NUMBER AS n_credit_simulations_with_pre_analysis,
  (SUM(has_pa)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_cs_with_pa
FROM joined
GROUP BY 1,2,3
ORDER BY 1,2,4 DESC
;

/* =========================================================
   [C1a] RESUMO (CS -> PA): por mês (sem clinic/state) + total do período
   - Mais barato e mais fácil de interpretar.
   ========================================================= */
WITH params AS (
  SELECT DATEADD('month', -TO_NUMBER($months_back), DATE_TRUNC('month', CURRENT_DATE())) AS start_month
),
cs_recent AS (
  SELECT
    DATE_TRUNC('month', IDENTIFIER($cs_ts_expr)) AS month,
    IDENTIFIER($cs_join_expr) AS join_key
  FROM IDENTIFIER($cs_table) cs
  WHERE IDENTIFIER($cs_ts_expr) >= (SELECT start_month FROM params)
),
pa_recent AS (
  SELECT DISTINCT
    IDENTIFIER($pa_join_expr) AS join_key
  FROM IDENTIFIER($pre_analyses_table) pa
  WHERE IDENTIFIER($pa_ts_expr) >= (SELECT start_month FROM params)
    AND IDENTIFIER($pa_join_expr) IS NOT NULL
),
joined AS (
  SELECT
    cs.month,
    IFF(pa.join_key IS NULL, 0, 1) AS has_pa
  FROM cs_recent cs
  LEFT JOIN pa_recent pa
    ON cs.join_key = pa.join_key
)
SELECT
  month,
  COUNT(*)::NUMBER AS n_credit_simulations,
  SUM(has_pa)::NUMBER AS n_credit_simulations_with_pre_analysis,
  (SUM(has_pa)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_cs_with_pa
FROM joined
GROUP BY 1
ORDER BY 1
;

WITH params AS (
  SELECT DATEADD('month', -TO_NUMBER($months_back), DATE_TRUNC('month', CURRENT_DATE())) AS start_month
),
cs_recent AS (
  SELECT IDENTIFIER($cs_join_expr) AS join_key
  FROM IDENTIFIER($cs_table) cs
  WHERE IDENTIFIER($cs_ts_expr) >= (SELECT start_month FROM params)
),
pa_recent AS (
  SELECT DISTINCT IDENTIFIER($pa_join_expr) AS join_key
  FROM IDENTIFIER($pre_analyses_table) pa
  WHERE IDENTIFIER($pa_ts_expr) >= (SELECT start_month FROM params)
    AND IDENTIFIER($pa_join_expr) IS NOT NULL
),
joined AS (
  SELECT IFF(pa.join_key IS NULL, 0, 1) AS has_pa
  FROM cs_recent cs
  LEFT JOIN pa_recent pa
    ON cs.join_key = pa.join_key
)
SELECT
  COUNT(*)::NUMBER AS n_credit_simulations,
  SUM(has_pa)::NUMBER AS n_credit_simulations_with_pre_analysis,
  (SUM(has_pa)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_cs_with_pa
FROM joined
;

/* =========================================================
   [C2] DIREÇÃO INVERSA: PRE_ANALYSES -> CREDIT_SIMULATIONS (recente)
   ========================================================= */
WITH params AS (
  SELECT DATEADD('month', -TO_NUMBER($months_back), DATE_TRUNC('month', CURRENT_DATE())) AS start_month
),
cs_recent AS (
  SELECT
    cs.*,
    DATE_TRUNC('month', IDENTIFIER($cs_ts_expr)) AS month
  FROM IDENTIFIER($cs_table) cs
  WHERE IDENTIFIER($cs_ts_expr) >= (SELECT start_month FROM params)
),
pa_recent AS (
  SELECT
    pa.*,
    DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month
  FROM IDENTIFIER($pre_analyses_table) pa
  WHERE IDENTIFIER($pa_ts_expr) >= (SELECT start_month FROM params)
),
joined AS (
  SELECT
    pa.month AS pa_month,
    IDENTIFIER($pa_clinic_expr) AS clinic_id,
    IFF(IDENTIFIER($cs_join_expr) IS NULL, 0, 1) AS has_cs
  FROM pa_recent pa
  LEFT JOIN cs_recent cs
    ON IDENTIFIER($cs_join_expr) = IDENTIFIER($pa_join_expr)
)
SELECT
  pa_month AS month,
  clinic_id,
  COUNT(*)::NUMBER AS n_pre_analyses,
  SUM(has_cs)::NUMBER AS n_pre_analyses_with_credit_simulation,
  (SUM(has_cs)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_pa_with_cs
FROM joined
GROUP BY 1,2
ORDER BY 1,3 DESC
;

/* =========================================================
   [C2a] RESUMO (PA -> CS): por mês + total do período
   ========================================================= */
WITH params AS (
  SELECT DATEADD('month', -TO_NUMBER($months_back), DATE_TRUNC('month', CURRENT_DATE())) AS start_month
),
pa_recent AS (
  SELECT
    DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month,
    IDENTIFIER($pa_join_expr) AS join_key
  FROM IDENTIFIER($pre_analyses_table) pa
  WHERE IDENTIFIER($pa_ts_expr) >= (SELECT start_month FROM params)
),
cs_recent AS (
  SELECT DISTINCT
    IDENTIFIER($cs_join_expr) AS join_key
  FROM IDENTIFIER($cs_table) cs
  WHERE IDENTIFIER($cs_ts_expr) >= (SELECT start_month FROM params)
    AND IDENTIFIER($cs_join_expr) IS NOT NULL
),
joined AS (
  SELECT
    pa.month,
    IFF(cs.join_key IS NULL, 0, 1) AS has_cs
  FROM pa_recent pa
  LEFT JOIN cs_recent cs
    ON pa.join_key = cs.join_key
)
SELECT
  month,
  COUNT(*)::NUMBER AS n_pre_analyses,
  SUM(has_cs)::NUMBER AS n_pre_analyses_with_credit_simulation,
  (SUM(has_cs)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_pa_with_cs
FROM joined
GROUP BY 1
ORDER BY 1
;

WITH params AS (
  SELECT DATEADD('month', -TO_NUMBER($months_back), DATE_TRUNC('month', CURRENT_DATE())) AS start_month
),
pa_recent AS (
  SELECT IDENTIFIER($pa_join_expr) AS join_key
  FROM IDENTIFIER($pre_analyses_table) pa
  WHERE IDENTIFIER($pa_ts_expr) >= (SELECT start_month FROM params)
),
cs_recent AS (
  SELECT DISTINCT IDENTIFIER($cs_join_expr) AS join_key
  FROM IDENTIFIER($cs_table) cs
  WHERE IDENTIFIER($cs_ts_expr) >= (SELECT start_month FROM params)
    AND IDENTIFIER($cs_join_expr) IS NOT NULL
),
joined AS (
  SELECT IFF(cs.join_key IS NULL, 0, 1) AS has_cs
  FROM pa_recent pa
  LEFT JOIN cs_recent cs
    ON pa.join_key = cs.join_key
)
SELECT
  COUNT(*)::NUMBER AS n_pre_analyses,
  SUM(has_cs)::NUMBER AS n_pre_analyses_with_credit_simulation,
  (SUM(has_cs)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_pa_with_cs
FROM joined
;


/* =========================================================
   [D1] CANDIDATOS DE COLUNAS POR EIXO (heurística por nome)
   - Resultado: lista para você mapear rapidamente “o que já existe” na PRE_ANALYSES.
   ========================================================= */
SELECT
  c.column_name,
  c.data_type,
  CASE
    WHEN c.column_name ILIKE '%BIRTH%' OR c.column_name ILIKE '%DOB%' OR c.column_name ILIKE '%NASC%' THEN 'Eixo 1: cadastro/demografia'
    WHEN c.column_name ILIKE '%GENDER%' OR c.column_name ILIKE '%SEX%' THEN 'Eixo 1: cadastro/demografia'
    WHEN c.column_name ILIKE '%ZIP%' OR c.column_name ILIKE '%CEP%' THEN 'Eixo 1: cadastro/demografia'
    WHEN c.column_name ILIKE '%PHONE%' OR c.column_name ILIKE '%TEL%' OR c.column_name ILIKE '%ADDRESS%' OR c.column_name ILIKE '%ENDERE%' THEN 'Eixo 1: cadastro/demografia'
    WHEN c.column_name ILIKE '%NEGAT%' OR c.column_name ILIKE '%PEFIN%' OR c.column_name ILIKE '%REFIN%' OR c.column_name ILIKE '%PROTEST%' OR c.column_name ILIKE '%RESTRI%' OR c.column_name ILIKE '%CCF%' THEN 'Eixo 2: negativação/restrições'
    WHEN c.column_name ILIKE '%INCOME%' OR c.column_name ILIKE '%RENDA%' OR c.column_name ILIKE '%SALARY%' OR c.column_name ILIKE '%LIMIT%' OR c.column_name ILIKE '%SCR%' OR c.column_name ILIKE '%BACEN%' OR c.column_name ILIKE '%CREDIT_LIMIT%' THEN 'Eixo 3: renda/proxies'
    WHEN c.column_name ILIKE '%SCORE%' OR c.column_name ILIKE '%SERASA%' OR c.column_name ILIKE '%BOA_VISTA%' OR c.column_name ILIKE '%BVS%' THEN 'Eixo 4: scores'
    ELSE 'outros'
  END AS axis_guess
FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS c
WHERE c.table_schema = 'CAPIM_ANALYTICS'
  AND c.table_name   = 'PRE_ANALYSES'
  AND (
    c.column_name ILIKE '%BIRTH%' OR c.column_name ILIKE '%DOB%' OR c.column_name ILIKE '%NASC%'
    OR c.column_name ILIKE '%GENDER%' OR c.column_name ILIKE '%SEX%'
    OR c.column_name ILIKE '%ZIP%' OR c.column_name ILIKE '%CEP%'
    OR c.column_name ILIKE '%PHONE%' OR c.column_name ILIKE '%TEL%'
    OR c.column_name ILIKE '%ADDRESS%' OR c.column_name ILIKE '%ENDERE%'
    OR c.column_name ILIKE '%NEGAT%' OR c.column_name ILIKE '%PEFIN%' OR c.column_name ILIKE '%REFIN%' OR c.column_name ILIKE '%PROTEST%' OR c.column_name ILIKE '%RESTRI%' OR c.column_name ILIKE '%CCF%'
    OR c.column_name ILIKE '%INCOME%' OR c.column_name ILIKE '%RENDA%' OR c.column_name ILIKE '%SALARY%'
    OR c.column_name ILIKE '%LIMIT%' OR c.column_name ILIKE '%SCR%' OR c.column_name ILIKE '%BACEN%' OR c.column_name ILIKE '%CREDIT_LIMIT%'
    OR c.column_name ILIKE '%SCORE%' OR c.column_name ILIKE '%SERASA%' OR c.column_name ILIKE '%BOA_VISTA%' OR c.column_name ILIKE '%BVS%'
  )
ORDER BY axis_guess, c.column_name
;

/* =========================================================
   [D2] GERADOR: fill-rate total (por coluna candidata)
   - Copie/cole o SQL gerado e execute (é barato).
   ========================================================= */
WITH cols AS (
  SELECT column_name
  FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema='CAPIM_ANALYTICS'
    AND table_name='PRE_ANALYSES'
    AND data_type NOT IN ('VARIANT','OBJECT','ARRAY')
    AND (
      column_name ILIKE '%BIRTH%' OR column_name ILIKE '%DOB%' OR column_name ILIKE '%NASC%'
      OR column_name ILIKE '%GENDER%' OR column_name ILIKE '%SEX%'
      OR column_name ILIKE '%ZIP%' OR column_name ILIKE '%CEP%'
      OR column_name ILIKE '%NEGAT%' OR column_name ILIKE '%PEFIN%' OR column_name ILIKE '%REFIN%' OR column_name ILIKE '%PROTEST%' OR column_name ILIKE '%RESTRI%' OR column_name ILIKE '%CCF%'
      OR column_name ILIKE '%INCOME%' OR column_name ILIKE '%RENDA%' OR column_name ILIKE '%SALARY%'
      OR column_name ILIKE '%LIMIT%' OR column_name ILIKE '%SCR%' OR column_name ILIKE '%BACEN%' OR column_name ILIKE '%CREDIT_LIMIT%'
      OR column_name ILIKE '%SCORE%' OR column_name ILIKE '%SERASA%' OR column_name ILIKE '%BOA_VISTA%' OR column_name ILIKE '%BVS%'
    )
)
SELECT
  LISTAGG(
    'SELECT ''' || column_name || ''' AS column_name, ' ||
    'COUNT(*)::NUMBER AS n_rows, ' ||
    'COUNT_IF(pa."' || column_name || '" IS NOT NULL)::NUMBER AS n_filled, ' ||
    '(COUNT_IF(pa."' || column_name || '" IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS fill_rate ' ||
    'FROM ' || $pre_analyses_table || ' pa',
    '\nUNION ALL\n'
  ) AS generated_sql
FROM cols
;

/* =========================================================
   [D3] GERADOR: fill-rate por mês (por coluna candidata)
   - Copie/cole o SQL gerado e execute.
   ========================================================= */
WITH cols AS (
  SELECT column_name
  FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema='CAPIM_ANALYTICS'
    AND table_name='PRE_ANALYSES'
    AND data_type NOT IN ('VARIANT','OBJECT','ARRAY')
    AND (
      column_name ILIKE '%BIRTH%' OR column_name ILIKE '%DOB%' OR column_name ILIKE '%NASC%'
      OR column_name ILIKE '%GENDER%' OR column_name ILIKE '%SEX%'
      OR column_name ILIKE '%ZIP%' OR column_name ILIKE '%CEP%'
      OR column_name ILIKE '%NEGAT%' OR column_name ILIKE '%PEFIN%' OR column_name ILIKE '%REFIN%' OR column_name ILIKE '%PROTEST%' OR column_name ILIKE '%RESTRI%' OR column_name ILIKE '%CCF%'
      OR column_name ILIKE '%INCOME%' OR column_name ILIKE '%RENDA%' OR column_name ILIKE '%SALARY%'
      OR column_name ILIKE '%LIMIT%' OR column_name ILIKE '%SCR%' OR column_name ILIKE '%BACEN%' OR column_name ILIKE '%CREDIT_LIMIT%'
      OR column_name ILIKE '%SCORE%' OR column_name ILIKE '%SERASA%' OR column_name ILIKE '%BOA_VISTA%' OR column_name ILIKE '%BVS%'
    )
)
SELECT
  LISTAGG(
    'SELECT ''' || column_name || ''' AS column_name, ' ||
    'DATE_TRUNC(''month'', IDENTIFIER($pa_ts_expr)) AS month, ' ||
    'COUNT(*)::NUMBER AS n_rows, ' ||
    'COUNT_IF(pa."' || column_name || '" IS NOT NULL)::NUMBER AS n_filled, ' ||
    '(COUNT_IF(pa."' || column_name || '" IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS fill_rate ' ||
    'FROM ' || $pre_analyses_table || ' pa ' ||
    'WHERE IDENTIFIER($pa_ts_expr) >= DATEADD(''month'', -TO_NUMBER($months_back), DATE_TRUNC(''month'', CURRENT_DATE())) ' ||
    'GROUP BY 2',
    '\nUNION ALL\n'
  ) AS generated_sql
FROM cols
;

/* =========================================================
   [D4] GERADOR: sanidade para colunas numéricas (min/max/p50/p90/p99)
   - Útil para detectar escalas misturadas (centavos vs reais; score vs renda).
   ========================================================= */
WITH cols AS (
  SELECT column_name
  FROM CAPIM_DATA.INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema='CAPIM_ANALYTICS'
    AND table_name='PRE_ANALYSES'
    AND data_type IN ('NUMBER','FLOAT','DOUBLE','DECIMAL','INTEGER','BIGINT')
    AND (
      column_name ILIKE '%SCORE%'
      OR column_name ILIKE '%INCOME%' OR column_name ILIKE '%RENDA%'
      OR column_name ILIKE '%VALUE%' OR column_name ILIKE '%AMOUNT%' OR column_name ILIKE '%BALANCE%'
      OR column_name ILIKE '%LIMIT%'
      OR column_name ILIKE '%PEFIN%' OR column_name ILIKE '%REFIN%' OR column_name ILIKE '%PROTEST%'
      OR column_name ILIKE '%SCR%'
    )
)
SELECT
  LISTAGG(
    'SELECT ''' || column_name || ''' AS column_name, ' ||
    'COUNT_IF(pa."' || column_name || '" IS NOT NULL)::NUMBER AS n_filled, ' ||
    'MIN(pa."' || column_name || '")::FLOAT AS min_value, ' ||
    'MAX(pa."' || column_name || '")::FLOAT AS max_value, ' ||
    'APPROX_PERCENTILE(pa."' || column_name || '", 0.50)::FLOAT AS p50, ' ||
    'APPROX_PERCENTILE(pa."' || column_name || '", 0.90)::FLOAT AS p90, ' ||
    'APPROX_PERCENTILE(pa."' || column_name || '", 0.99)::FLOAT AS p99 ' ||
    'FROM ' || $pre_analyses_table || ' pa',
    '\nUNION ALL\n'
  ) AS generated_sql
FROM cols
;

/* =========================================================
   [D5] RESULTADOS DIRETOS (sem gerador): fill-rate dos poucos campos úteis por eixo
   - Eixo 1 (cadastro leve): ZIPCODE, OCCUPATION
   - Eixo 4 (scores por bureau): SERASA_POSITIVE_SCORE, BVS_POSITIVE_SCORE
   - Bridge/metadata: CREDIT_LEAD_ID, CRIVO_ID, HAS_REQUEST
   ========================================================= */
SELECT
  COUNT(*)::NUMBER AS n_rows,
  COUNT_IF(CREDIT_LEAD_ID IS NOT NULL)::NUMBER AS n_with_credit_lead_id,
  (COUNT_IF(CREDIT_LEAD_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_credit_lead_id,
  COUNT_IF(CRIVO_ID IS NOT NULL)::NUMBER AS n_with_crivo_id,
  (COUNT_IF(CRIVO_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_crivo_id,
  COUNT_IF(ZIPCODE IS NOT NULL)::NUMBER AS n_with_zipcode,
  (COUNT_IF(ZIPCODE IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_zipcode,
  COUNT_IF(OCCUPATION IS NOT NULL)::NUMBER AS n_with_occupation,
  (COUNT_IF(OCCUPATION IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_occupation,
  COUNT_IF(SERASA_POSITIVE_SCORE IS NOT NULL)::NUMBER AS n_with_serasa_positive_score,
  (COUNT_IF(SERASA_POSITIVE_SCORE IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_serasa_positive_score,
  COUNT_IF(BVS_POSITIVE_SCORE IS NOT NULL)::NUMBER AS n_with_bvs_positive_score,
  (COUNT_IF(BVS_POSITIVE_SCORE IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_bvs_positive_score,
  /* Snowflake: usar COUNT_IF(boolean_expr); 'IS TRUE' não compila dentro do COUNT_IF */
  COUNT_IF(COALESCE(HAS_REQUEST, FALSE))::NUMBER AS n_has_request_true,
  (COUNT_IF(COALESCE(HAS_REQUEST, FALSE))::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_has_request_true
FROM IDENTIFIER($pre_analyses_table) pa
;

SELECT
  DATE_TRUNC('month', IDENTIFIER($pa_ts_expr)) AS month,
  COUNT(*)::NUMBER AS n_rows,
  (COUNT_IF(CREDIT_LEAD_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_credit_lead_id,
  (COUNT_IF(CRIVO_ID IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_crivo_id,
  (COUNT_IF(ZIPCODE IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_zipcode,
  (COUNT_IF(OCCUPATION IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_occupation,
  (COUNT_IF(SERASA_POSITIVE_SCORE IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_serasa_positive_score,
  (COUNT_IF(BVS_POSITIVE_SCORE IS NOT NULL)::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_with_bvs_positive_score,
  (COUNT_IF(COALESCE(HAS_REQUEST, FALSE))::FLOAT / NULLIF(COUNT(*)::FLOAT,0))::FLOAT AS pct_has_request_true
FROM IDENTIFIER($pre_analyses_table) pa
GROUP BY 1
ORDER BY 1
;

/* =========================================================
   [D6] SANIDADE: distribuição de scores e valores monetários (para checar escala)
   ========================================================= */
SELECT
  'SERASA_POSITIVE_SCORE' AS metric,
  COUNT_IF(SERASA_POSITIVE_SCORE IS NOT NULL)::NUMBER AS n_filled,
  MIN(SERASA_POSITIVE_SCORE)::FLOAT AS min_value,
  MAX(SERASA_POSITIVE_SCORE)::FLOAT AS max_value,
  APPROX_PERCENTILE(SERASA_POSITIVE_SCORE, 0.50)::FLOAT AS p50,
  APPROX_PERCENTILE(SERASA_POSITIVE_SCORE, 0.90)::FLOAT AS p90,
  APPROX_PERCENTILE(SERASA_POSITIVE_SCORE, 0.99)::FLOAT AS p99
FROM IDENTIFIER($pre_analyses_table) pa
UNION ALL
SELECT
  'BVS_POSITIVE_SCORE' AS metric,
  COUNT_IF(BVS_POSITIVE_SCORE IS NOT NULL)::NUMBER,
  MIN(BVS_POSITIVE_SCORE)::FLOAT,
  MAX(BVS_POSITIVE_SCORE)::FLOAT,
  APPROX_PERCENTILE(BVS_POSITIVE_SCORE, 0.50)::FLOAT,
  APPROX_PERCENTILE(BVS_POSITIVE_SCORE, 0.90)::FLOAT,
  APPROX_PERCENTILE(BVS_POSITIVE_SCORE, 0.99)::FLOAT
FROM IDENTIFIER($pre_analyses_table) pa
UNION ALL
SELECT
  'PRE_ANALYSIS_AMOUNT' AS metric,
  COUNT_IF(PRE_ANALYSIS_AMOUNT IS NOT NULL)::NUMBER,
  MIN(PRE_ANALYSIS_AMOUNT)::FLOAT,
  MAX(PRE_ANALYSIS_AMOUNT)::FLOAT,
  APPROX_PERCENTILE(PRE_ANALYSIS_AMOUNT, 0.50)::FLOAT,
  APPROX_PERCENTILE(PRE_ANALYSIS_AMOUNT, 0.90)::FLOAT,
  APPROX_PERCENTILE(PRE_ANALYSIS_AMOUNT, 0.99)::FLOAT
FROM IDENTIFIER($pre_analyses_table) pa
UNION ALL
SELECT
  'PRE_ANALYSIS_INSTALLMENT_AMOUNT' AS metric,
  COUNT_IF(PRE_ANALYSIS_INSTALLMENT_AMOUNT IS NOT NULL)::NUMBER,
  MIN(PRE_ANALYSIS_INSTALLMENT_AMOUNT)::FLOAT,
  MAX(PRE_ANALYSIS_INSTALLMENT_AMOUNT)::FLOAT,
  APPROX_PERCENTILE(PRE_ANALYSIS_INSTALLMENT_AMOUNT, 0.50)::FLOAT,
  APPROX_PERCENTILE(PRE_ANALYSIS_INSTALLMENT_AMOUNT, 0.90)::FLOAT,
  APPROX_PERCENTILE(PRE_ANALYSIS_INSTALLMENT_AMOUNT, 0.99)::FLOAT
FROM IDENTIFIER($pre_analyses_table) pa
;


/* =========================================================
   [Z] ANTI-CEGUEIRA “PERIÓDICA” (template)
   - Rode mensalmente/semana: compara TYPEOF + top keys do JSON em 3 janelas.
   - Ajuste `pa_json_expr` para cada JSON relevante (rode 1x por JSON).
   ========================================================= */
WITH params_periods AS (
  SELECT 'p_old'  AS period_label, DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE())) AS period_start, DATEADD('month', -11, DATE_TRUNC('month', CURRENT_DATE())) AS period_end, 5000::INT AS sample_n
  UNION ALL
  SELECT 'p_mid', DATEADD('month', -6,  DATE_TRUNC('month', CURRENT_DATE())), DATEADD('month', -5,  DATE_TRUNC('month', CURRENT_DATE())), 5000::INT
  UNION ALL
  SELECT 'p_new', DATEADD('month', -1,  DATE_TRUNC('month', CURRENT_DATE())), DATE_TRUNC('month', CURRENT_DATE()), 5000::INT
),
pa_sample AS (
  SELECT
    p.period_label,
    IDENTIFIER($pa_json_expr) AS j
  FROM IDENTIFIER($pre_analyses_table) pa
  JOIN params_periods p ON TRUE
  WHERE IDENTIFIER($pa_ts_expr) >= p.period_start
    AND IDENTIFIER($pa_ts_expr) <  p.period_end
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.period_label ORDER BY UNIFORM(0, 1000000, RANDOM())) <= p.sample_n
),
typeof_dist AS (
  SELECT
    'pct_typeof' AS metric_name,
    CONCAT(period_label, '|', TYPEOF(j)) AS dimension,
    (COUNT(*)::FLOAT / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY period_label), 0))::FLOAT AS value_number
  FROM pa_sample
  GROUP BY period_label, TYPEOF(j)
),
keys_flat AS (
  SELECT
    s.period_label,
    k.value::string AS key_name
  FROM pa_sample s,
  LATERAL FLATTEN(input => IFF(TYPEOF(s.j)='OBJECT', OBJECT_KEYS(s.j), NULL)) k
  WHERE TYPEOF(s.j) = 'OBJECT'
),
top_keys AS (
  SELECT
    'top_key_pct' AS metric_name,
    CONCAT(period_label, '|', key_name) AS dimension,
    (COUNT(*)::FLOAT / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY period_label), 0))::FLOAT AS value_number
  FROM keys_flat
  GROUP BY period_label, key_name
  QUALIFY ROW_NUMBER() OVER (PARTITION BY period_label ORDER BY COUNT(*) DESC, key_name ASC) <= 30
)
SELECT * FROM typeof_dist
UNION ALL
SELECT * FROM top_keys
ORDER BY metric_name, dimension
;


/* =========================================================
   [E] Fluxos coexistentes (2025+): PRE_ANALYSIS_ID polimórfico
   Objetivo: reconstruir empiricamente (sem datas “de cabeça”) a migração:
     - Legado: entidade = pre_analysis_id
     - Novo: entidade = credit_simulation_id (ID pode aparecer dentro de PRE_ANALYSES)
   Guardrail: ID pode colidir entre tipos diferentes → chave canônica = (PRE_ANALYSIS_TYPE, PRE_ANALYSIS_ID)
   ========================================================= */

/* [E0] Checagem rápida do dicionário de segmentação (is_independent_clinic) */
SELECT
  column_name,
  data_type
FROM CAPIM_DATA_DEV.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'POSSANI_SANDBOX'
  AND table_name = 'CLINIC_MOST_RELEVANT_INFO'
  AND column_name ILIKE '%INDEPENDENT%'
ORDER BY column_name
;

/* [E1] Quais valores existem em PRE_ANALYSIS_TYPE? (amplo, barato) */
SELECT
  PRE_ANALYSIS_TYPE,
  COUNT(*)::NUMBER AS n_rows
FROM IDENTIFIER($pre_analyses_table) pa
GROUP BY 1
ORDER BY n_rows DESC
;

/* [E2] PRE_ANALYSIS_TYPE por mês e segmento (is_independent_clinic), focado em 2024-01+ */
WITH base AS (
  SELECT
    DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) AS month,
    pa.RETAIL_ID AS clinic_id,
    pa.PRE_ANALYSIS_TYPE,
    COUNT(*)::NUMBER AS n_rows
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_CREATED_AT >= '2024-01-01'::TIMESTAMP_NTZ
  GROUP BY 1,2,3
),
seg AS (
  SELECT
    clinic_id,
    is_independent_clinic
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CLINIC_MOST_RELEVANT_INFO
)
SELECT
  b.month,
  s.is_independent_clinic,
  b.PRE_ANALYSIS_TYPE,
  SUM(b.n_rows)::NUMBER AS n_rows
FROM base b
LEFT JOIN seg s
  ON s.clinic_id = b.clinic_id
GROUP BY 1,2,3
ORDER BY 1,2,4 DESC
;

/* [E3] Teste direto da hipótese: quando tipo indica “credit_simulation”, PRE_ANALYSIS_ID == CREDIT_SIMULATIONS.ID?
   - resultado por mês e segmento (is_independent_clinic)
   - também mede % com credit_lead_id preenchido nesses registros
*/
WITH pa_cs_typed AS (
  SELECT
    DATE_TRUNC('month', pa.PRE_ANALYSIS_CREATED_AT) AS month,
    pa.RETAIL_ID AS clinic_id,
    pa.PRE_ANALYSIS_TYPE,
    pa.PRE_ANALYSIS_ID,
    pa.CREDIT_LEAD_ID
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_CREATED_AT >= '2024-01-01'::TIMESTAMP_NTZ
    AND pa.PRE_ANALYSIS_TYPE IS NOT NULL
    AND LOWER(pa.PRE_ANALYSIS_TYPE) LIKE '%credit%'
),
seg AS (
  SELECT clinic_id, is_independent_clinic
  FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CLINIC_MOST_RELEVANT_INFO
),
cs AS (
  SELECT
    cs.id AS credit_simulation_id,
    cs.credit_lead_id,
    cs.retail_id AS clinic_id
  FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs
  WHERE cs.created_at >= '2024-01-01'::TIMESTAMP_NTZ
),
joined AS (
  SELECT
    pa.month,
    pa.clinic_id,
    s.is_independent_clinic,
    pa.PRE_ANALYSIS_TYPE,
    IFF(cs.credit_simulation_id IS NULL, 0, 1) AS matches_cs_by_id,
    IFF(pa.CREDIT_LEAD_ID IS NOT NULL, 1, 0) AS has_credit_lead_id
  FROM pa_cs_typed pa
  LEFT JOIN seg s
    ON s.clinic_id = pa.clinic_id
  LEFT JOIN cs
    ON cs.credit_simulation_id = pa.PRE_ANALYSIS_ID
)
SELECT
  month,
  is_independent_clinic,
  PRE_ANALYSIS_TYPE,
  COUNT(*)::NUMBER AS n_rows,
  SUM(matches_cs_by_id)::NUMBER AS n_match_cs_by_id,
  (SUM(matches_cs_by_id)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_match_cs_by_id,
  AVG(has_credit_lead_id)::FLOAT AS pct_has_credit_lead_id
FROM joined
GROUP BY 1,2,3
ORDER BY 1,2,4 DESC
;

/* [E4] Colisão de ID entre tipos (o mesmo PRE_ANALYSIS_ID em 2+ PRE_ANALYSIS_TYPE) — recorte 2024-01+ */
WITH per_id AS (
  SELECT
    pa.PRE_ANALYSIS_ID,
    COUNT(DISTINCT pa.PRE_ANALYSIS_TYPE)::NUMBER AS n_types,
    MIN(pa.PRE_ANALYSIS_CREATED_AT) AS min_ts,
    MAX(pa.PRE_ANALYSIS_CREATED_AT) AS max_ts
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_CREATED_AT >= '2024-01-01'::TIMESTAMP_NTZ
    AND pa.PRE_ANALYSIS_ID IS NOT NULL
    AND pa.PRE_ANALYSIS_TYPE IS NOT NULL
  GROUP BY 1
)
SELECT
  COUNT(*)::NUMBER AS n_ids_total,
  COUNT_IF(n_types >= 2)::NUMBER AS n_ids_with_type_collision,
  (COUNT_IF(n_types >= 2)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_ids_with_type_collision
FROM per_id
;

/* Exemplos (amostrados) de colisão para inspeção */
WITH colliding AS (
  SELECT
    pa.PRE_ANALYSIS_ID
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_CREATED_AT >= '2024-01-01'::TIMESTAMP_NTZ
    AND pa.PRE_ANALYSIS_ID IS NOT NULL
    AND pa.PRE_ANALYSIS_TYPE IS NOT NULL
  GROUP BY 1
  HAVING COUNT(DISTINCT pa.PRE_ANALYSIS_TYPE) >= 2
  QUALIFY ROW_NUMBER() OVER (ORDER BY pa.PRE_ANALYSIS_ID) <= 50
)
SELECT
  pa.PRE_ANALYSIS_ID,
  pa.PRE_ANALYSIS_TYPE,
  MIN(pa.PRE_ANALYSIS_CREATED_AT) AS min_created_at,
  MAX(pa.PRE_ANALYSIS_CREATED_AT) AS max_created_at,
  COUNT(*)::NUMBER AS n_rows,
  COUNT_IF(pa.CREDIT_LEAD_ID IS NOT NULL)::NUMBER AS n_with_credit_lead_id
FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
JOIN colliding c
  ON c.PRE_ANALYSIS_ID = pa.PRE_ANALYSIS_ID
GROUP BY 1,2
ORDER BY 1,2
;

/* [E4b] Colisão de ID entre tipos — HISTÓRICO COMPLETO (desde o início da PRE_ANALYSES)
   Motivo: sabemos que já houve investigações onde o mesmo ID existia em entidades diferentes.
*/
WITH per_id AS (
  SELECT
    pa.PRE_ANALYSIS_ID,
    COUNT(DISTINCT pa.PRE_ANALYSIS_TYPE)::NUMBER AS n_types,
    MIN(pa.PRE_ANALYSIS_CREATED_AT) AS min_ts,
    MAX(pa.PRE_ANALYSIS_CREATED_AT) AS max_ts
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_ID IS NOT NULL
    AND pa.PRE_ANALYSIS_TYPE IS NOT NULL
  GROUP BY 1
)
SELECT
  COUNT(*)::NUMBER AS n_ids_total,
  COUNT_IF(n_types >= 2)::NUMBER AS n_ids_with_type_collision,
  (COUNT_IF(n_types >= 2)::FLOAT / NULLIF(COUNT(*)::FLOAT, 0))::FLOAT AS pct_ids_with_type_collision,
  MIN(min_ts) AS min_created_at_in_scope,
  MAX(max_ts) AS max_created_at_in_scope
FROM per_id
;

WITH colliding AS (
  SELECT
    pa.PRE_ANALYSIS_ID
  FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
  WHERE pa.PRE_ANALYSIS_ID IS NOT NULL
    AND pa.PRE_ANALYSIS_TYPE IS NOT NULL
  GROUP BY 1
  HAVING COUNT(DISTINCT pa.PRE_ANALYSIS_TYPE) >= 2
  QUALIFY ROW_NUMBER() OVER (ORDER BY pa.PRE_ANALYSIS_ID) <= 50
)
SELECT
  pa.PRE_ANALYSIS_ID,
  pa.PRE_ANALYSIS_TYPE,
  MIN(pa.PRE_ANALYSIS_CREATED_AT) AS min_created_at,
  MAX(pa.PRE_ANALYSIS_CREATED_AT) AS max_created_at,
  COUNT(*)::NUMBER AS n_rows
FROM CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES pa
JOIN colliding c
  ON c.PRE_ANALYSIS_ID = pa.PRE_ANALYSIS_ID
GROUP BY 1,2
ORDER BY 1,2
;


