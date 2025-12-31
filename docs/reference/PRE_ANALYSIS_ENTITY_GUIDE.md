# Entidade `pre_analysis` (C1 legado) — Guia oficial de chaves, tabelas e enriquecimento

> Última revisão: **2025-12-30T13:55-03:00**.

## Propósito
Este documento é a referência oficial (no escopo deste repo) para:
- definir a **entidade C1 legado** (`pre_analysis`) e suas chaves;
- listar as tabelas estudadas que permitem enriquecer C1 legado com sinais de bureau/motor;
- evitar equívocos comuns (ex.: assumir `pre_analysis_id == credit_simulation_id` sem checar tipo).

> Contexto: a partir de 2025, `credit_simulation` (novo fluxo) passa a coexistir com `pre_analysis` (legado). Para o novo fluxo, o enrichment core permanece no grão `credit_simulation_id` (ver `docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md`).

---

## Modelo de entidade e chaves canônicas

### Chave canônica de C1 unificado (2025+ dual-run)
Quando analisamos C1 como “universo unificado”, a chave correta é:
- **(`pre_analysis_type`, `pre_analysis_id`)**

Motivo:
- `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES.PRE_ANALYSIS_ID` é **polimórfico** e pode colidir entre tipos.

### Tipos
Em `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES`:
- `PRE_ANALYSIS_TYPE='credit_simulation'` → `PRE_ANALYSIS_ID == CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS.id` (validado em `queries/audit/audit_pre_analyses.sql`, seção `[E]`)
- `PRE_ANALYSIS_TYPE='pre_analysis'` → entidade legado (flat); **C1 legado canônico = `pre_analysis_id`**

### Deduplicação (reprocessamento/snapshot)
Algumas tabelas reprocessam o mesmo `pre_analysis_id` (duplicidade tipicamente 2 linhas).
Recomendação:
- deduplicar por timestamp “mais recente” (`*_UPDATED_AT` quando existir; senão usar o timestamp de consulta mais recente do bureau).

---

## Tabelas “fonte de verdade” e bridges estudadas

### 1) Unificação (com ressalvas)
- **`CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES`**
  - Função: tabela unificada (legado + novo) **para metadata do C1**.
  - Guardrail: não usar `PRE_ANALYSIS_ID` sozinho como chave; usar `(PRE_ANALYSIS_TYPE, PRE_ANALYSIS_ID)`.

### 2) Legado (CPF e cadastro básico)
- **`CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API`**
  - Função: “flat” legado; contém **`CPF`** e campos básicos (ex.: `BIRTHDATE`, `ZIPCODE`, `STATE`, `OCCUPATION`).
  - Papel no enrichment: ponte para qualquer integração por CPF (credit checks / crivo via CPF+tempo).

### 3) Features curadas de bureau/motor no grão `pre_analysis_id`
- **`CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK`**
  - Função: dataset **curado** por `pre_analysis_id` com sinais como:
    - SERASA: `SERASA_POSITIVE_SCORE`, `SERASA_PEFIN`, `SERASA_REFIN`, `SERASA_PROTEST`, `SERASA_PRESUMED_INCOME`, timestamps e flags de erro/cache
    - BVS: `BVS_POSITIVE_SCORE`, `BVS_TOTAL_DEBT`, `BVS_CCF_COUNT`, timestamps e flags de erro/cache
    - SCR: `SCORE_SCR`, `SCR_*` (muitos campos), timestamps e flags de erro/cache
  - Importante: **não** expõe `credit_check_id` para join determinístico com `INCREMENTAL_CREDIT_CHECKS_API.id`.
  - Uso recomendado: fonte principal de features “rápidas e estáveis” para C1 legado.

### 4) Execução do motor (resultado / decisão)
- **`CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION`**
  - Função: tabela de execução do motor com:
    - `ENGINEABLE_TYPE/ENGINEABLE_ID` (ex.: `pre_analysis`)
    - `SOURCE` (observado: `crivo`, `n8n`)
    - `DATA` (para `crivo`: texto/log; para `n8n`: JSON com features agregadas, inclusive `cpf` e scores)
  - Vínculo claro: `ENGINEABLE_TYPE='pre_analysis'` → `ENGINEABLE_ID = SOURCE_PRE_ANALYSIS_API.PRE_ANALYSIS_ID` (alta cobertura)
  - Limitação: não fornece `credit_check_id` nem `crivo_check_id` de forma canônica.
  - Nota de compatibilidade (aprovado): campos `pefinSerasa/refinSerasa/protestoSerasa` do `n8n` podem ser tratados como **contagens compatíveis** com os counts SERASA do eixo de negativação.

### 5) Payload bruto de credit checks (auditável por contrato)
- **`CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API`**
  - Função: payload bruto por **CPF** + `created_at` + `source/kind/new_data_format`.
  - Não tem `pre_analysis_id`/`credit_simulation_id` → associação é por **CPF + tempo**.

### 6) Crivo checks (staging)
- **`CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS`**
  - Vínculo oficial: apenas para `CreditSimulation` (`ENGINEABLE_TYPE='CreditSimulation'`).
  - Para legado (`pre_analysis`): não há engineable “PreAnalysis”; se for necessário usar Crivo, a estratégia é **CPF + tempo** (usando CPF no `KEY_PARAMETERS:campos:"CPF"`).
  - Guardrail: `PRE_ANALYSES.CRIVO_ID` não é chave confiável para `SOURCE_CRIVO_CHECKS.CRIVO_CHECK_ID` (join direto não fecha).

---

## Recomendações de enrichment (práticas)

### Caso A — `PRE_ANALYSIS_TYPE='credit_simulation'`
- **Não reprocessar**.
- Reusar o enrichment core já materializado no grão `credit_simulation_id` (ver `docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md`).

### Caso B — `PRE_ANALYSIS_TYPE='pre_analysis'` (legado)
Recomendação “default” (cobertura + simplicidade):
- base: `SOURCE_PRE_ANALYSIS_API` (CPF + birthdate/zipcode/state/occupation)
- features de bureau/motor: `PRE_ANALYSIS_CREDIT_CHECK` (curado)
- `SOURCE_CREDIT_ENGINE_INFORMATION` (opcional) para variáveis/decisão do motor (ex.: n8n), com proveniência explícita.

Quando precisar de **auditabilidade por payload** (semântica fina / anti-cegueira):
- associar `INCREMENTAL_CREDIT_CHECKS_API` por **CPF + tempo** (±1h/±24h e fallbacks -15d/-180d).

---

## Queries e auditorias relacionadas (Snowflake-first)
- Auditoria de dual-run e chaves: `queries/audit/audit_pre_analyses.sql` (seção `[E]`)
- Auditoria da tabela do motor: `queries/audit/audit_credit_engine_information.sql`
- Auditoria da tabela curada: `queries/audit/audit_pre_analysis_credit_check.sql`
- Enrichment C1 unificado (com bifurcação por tipo): `queries/enrich/enrich_pre_analyses_borrower.sql`

