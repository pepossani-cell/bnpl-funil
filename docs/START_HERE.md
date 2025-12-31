# START HERE — BNPL funil + enrichment (AI-first)

## Objetivo deste repo (1 parágrafo)
Este repositório documenta e implementa, em **Snowflake-first**, o enrichment do funil BNPL com foco em **C1 (simulações)** e sua expansão para **C2 (requests)**. O objetivo é produzir datasets **auditáveis**, com **grão e chaves explícitos**, e com **guardrails de semântica** para evitar regressões (ex.: misturar score vs renda, centavos vs reais).

---

## Contrato de execução (para humanos e agentes)
### Princípios
- **Preferir Snowflake Worksheet**: queries em `queries/` são (em geral) autocontidas e “pushdown total”.
- **Python local é permitido, mas não é o default**: use para **materialização/automação** (CTAS / criação de tabelas) quando fizer sentido.
- **Evitar custo local**: não mover dados para a máquina local para EDA pesada.
- **Higiene do repo**: arquivos/códigos criados **apenas** para investigação pontual devem ser **apagados** ao final (manter apenas o que é core/canônico).

### Como rodar (atalhos)
- **Snowflake Worksheet**: cole e execute diretamente os SQLs de `queries/`.
- **Python local (orquestração/materialização)**:
  - Preferir execução como módulo: `python -m src.cli.run_sql_file --file queries/<arquivo>.sql`
  - Scripts “core” devem ficar em `src/` (evitar scripts investigativos soltos).

---

## Invariantes do projeto (sempre declarar em novas threads)
### Funil (definições canônicas)
- **C1 (Simulação)**:
  - Novo fluxo: `credit_simulation` (`CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS`)
  - Legado: `pre_analysis` (`CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API`)
- **C2 (Request/Pedido)**: request pós pré-aprovação (ver core de requests).
- **C2S (Assinado)**: request assinado / originação.

### Tempo do evento (padrão)
- **Evento = `created_at`** (para `pre_analysis`, `credit_simulation`, `credit_lead`, `request`).

### Dual-run (2025+)
- Em tabelas “unificadas” (ex.: `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES`), `PRE_ANALYSIS_ID` é **polimórfico**.
- Chave canônica unificada para C1 quando necessário: **(`pre_analysis_type`, `pre_analysis_id`)**.

---

## Onde está a “fonte de verdade” por entidade (docs core)
- **C1 — credit simulations (core)**: `docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md`
- **C1 — pre_analysis (core)**: `docs/ENRICHMENT_PRE_ANALYSIS_CORE.md`
- **C2 — requests (core)**: `docs/ENRICHMENT_REQUESTS_CORE.md`

Contexto de negócio do funil:
- `docs/PROJECT_CONTEXT.md`

---

## Referências e runbooks (detalhes; não duplicar no core)
### Reference (tabelas, payloads, contratos)
- `docs/reference/PAYLOAD_CONTRACTS_MAP.md`
- `docs/reference/CREDIT_CHECKS_INCREMENTAL_API_NOTES.md`
- `docs/reference/CRIVO_CHECKS_NOTES.md`
- `docs/reference/CREDIT_SIMULATIONS_CREDIT_CHECKS_ASSOCIATION_NOTES.md`
- `docs/reference/PRE_ANALYSIS_ENTITY_GUIDE.md`
 - `docs/ENRICHMENT_SIGNAL_GOVERNANCE.md` (governança: quando promover “sinais/features” para o core)

### Runbooks (como executar/operar)
- `docs/runbooks/EXECUTION_IN_SNOWFLAKE.md`

### Decisões homologadas (ADRs)
- `docs/adr/` (decisões curtas e linkáveis; evita reabrir debates)

---

## “Se você quer X, rode Y” (lookup rápido)
### Enriquecer C1 (credit_simulations)
- Implementação: `queries/enrich/enrich_credit_simulations_borrower.sql`
- Validação (amostral, 100% SQL): `queries/validate/validate_enrich_credit_simulations_borrower_sampling.sql`
- Anti-cegueira de payload: `queries/audit/audit_payload_paths_sampling.sql`

### Unificar C1 em uma “tabela oficial” comparável (4 eixos)
- Requisito: materializar `PRE_ANALYSES_ENRICHED_BORROWER` via `python -m src.cli.materialize_enriched_pre_analyses_borrower`
- Criar a view oficial: `queries/views/create_view_c1_enriched_borrower_v1.sql`
- Painel/consulta (mesma lógica; sem duplicar credit_simulation): `queries/validate/c1_enriched_timeseries_panel.sql`

### Auditar C1 unificado / dual-run (`PRE_ANALYSES`)
- Auditoria canônica: `queries/audit/audit_pre_analyses.sql`

### Auditar `pre_analysis` legado (features curadas + motor)
- Curado: `queries/audit/audit_pre_analysis_credit_check.sql`
- Motor: `queries/audit/audit_credit_engine_information.sql`

