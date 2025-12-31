# Guia de governança de sinais do enrichment (core) — como decidir “valor” vs “ruído”
> Última revisão: **2025-12-30T15:35-03:00**.

## Princípio
Não promover para “core” nada que não seja:
- **interpretável** (semântica clara + unidade/escala),
- **estável** (contrato não muda frequentemente),
- **auditável** (fill-rate + linhagem + sanidade),
- e **útil** (sinal preditivo/analítico plausível).

---

## Checklist por feature/sinal candidato
- **Semântica**
  - O que mede? (ex.: “contagem de protestos”, “renda estimada mensal”)
  - Unidade/escala (centavos vs reais; inteiro vs percentual)
  - Faixa plausível e tratamento de inválidos (ex.: `<=0` vira `NULL`)
  - Diferença entre `NULL` (não observado) vs `0` (observado e zero)

- **Robustez do contrato**
  - `TYPEOF(data)` muda? (ARRAY↔OBJECT)
  - Path muda? (ex.: `report.score` escalar ↔ objeto)
  - Existem múltiplas versões concorrentes (new vs old) ao mesmo tempo?

- **Cobertura**
  - Fill-rate global e por período
  - Fill-rate por `source/kind` (evita “média enganosa”)

- **Linhagem**
  - `*_source`, `*_match_stage`, `*_minutes_from_event`
  - A feature pode ser “mascarada” por uma fonte leniente? (precisa seleção dinâmica)

- **Sanidade**
  - Outliers (p50/p90/p99) e valores impossíveis
  - Drift mensal (mudou distribuição sem motivo?)

---

## Como usar as auditorias deste repo
- **Inventário estrutural** (`queries/audit/inventory_credit_checks_crivo_checks.sql`)
  - Responde “quais tipos existem e como variam por mês”
  - Detecta mudanças de contrato por `TYPEOF` e por chaves dominantes

- **Anti-cegueira por paths** (`queries/audit/audit_payload_paths_sampling.sql`)
  - Mede fill-rate por path “crítico” (ex.: `registration.birthDate`, `negativeData.summary.count`)
  - Para `check_income_only`, mede distribuição de `scoreModel` e presença de `score`

- **Corretude do enrichment** (`queries/audit/audit_enrichment_correctness_sampling.sql`)
  - Re-extrai do payload bruto e compara com o que foi materializado
  - Mostra métricas de mismatch + amostra de linhas problemáticas para investigação

### Nota sobre legado `pre_analysis_id` (C1 legado)
Quando o objetivo for enriquecer/analisar o **C1 legado** (entidade `pre_analysis_id`) ou lidar com o dual-run 2025+:
- Use o guia oficial: `docs/reference/PRE_ANALYSIS_ENTITY_GUIDE.md`
  - define a chave canônica (`pre_analysis_type`, `pre_analysis_id`)
  - lista as tabelas estudadas (`SOURCE_PRE_ANALYSIS_API`, `PRE_ANALYSIS_CREDIT_CHECK`, `SOURCE_CREDIT_ENGINE_INFORMATION`) e seus guardrails

> Nota de organização: não mantemos scripts auxiliares no repo para executar essas auditorias.
> A execução recomendada é diretamente no Snowflake Worksheet (as queries já são “self-contained”).

---

## Exemplo de decisão: SERASA `check_income_only`
- **Sinal observado**: `scoreModel ILIKE 'HRP%'` + `score` que representa **renda estimada em centavos**.
- **Decisão**:
  - promover como `serasa_income_estimated` (R$) no eixo renda/proxies
  - **não** usar como `serasa_score` (evita mistura de semântica/escala)
- **Validação**:
  - `queries/audit/audit_payload_paths_sampling.sql`: ver % `HRP%` e p50 do raw
  - `queries/audit/audit_enrichment_correctness_sampling.sql`: mismatch ~0 quando há evidência estrita


