# Contexto do projeto (AI-first) — Funil BNPL + entidades + fontes + guardrails
> Última revisão: **2025-12-31T00:30-03:00**.  
> Público-alvo: **agentes de IA** e pessoas técnicas.  
> Comece por: `docs/START_HERE.md`.

## Propósito deste arquivo
Este documento é um **mapa determinístico** do projeto para reduzir confusões comuns (principalmente em agentes de IA):
- alinhar **terminologia de negócio** (C1/C2/C2S) com **entidades técnicas** (`credit_simulation`/`pre_analysis`/`request`);
- listar **chaves canônicas** e **fontes da verdade** por domínio;
- registrar **guardrails do dual-run 2025+** (para não errar joins/grão);
- orientar execução: **SQL-first no Snowflake** (com queries canônicas já prontas).

> Detalhes de enrichment:  
> - `docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md`  
> - `docs/ENRICHMENT_PRE_ANALYSIS_CORE.md`  
> - `docs/ENRICHMENT_REQUESTS_CORE.md` (placeholder)
> - `docs/ENRICHMENT_SIGNAL_GOVERNANCE.md` (governança de sinais/features: quando promover para o core)

> Camada oficial (consumo) — C1 unificado:
> - `CAPIM_DATA_DEV.POSSANI_SANDBOX.C1_ENRICHED_BORROWER`
>   - inclui flags canônicas/inferidas (ex.: `APPEALABLE` no CS; `*_prob/*_source` no legado)
>   - inclui risco paciente (`risk_capim`) com campos “safe for aggregation”
>   - inclui risco clínica dinâmico via `CAPIM_ANALYTICS.CLINIC_SCORE_LOGS` com `match_stage`

---

## 1) Glossário canônico (negócio ↔ técnico)
### Etapas do funil (negócio)
- **C1**: pré-análise / simulação
- **C2**: pedido
- **C2S**: contrato assinado / originação
- **Pós-C2S**: ciclo de vida (cancelamento, fraude, inadimplência, renegociação/cobrança)

### Entidades técnicas (tabelas/queries)
- **`credit_lead`**: “pai” do novo fluxo; pode gerar N simulations
- **`credit_simulation`**: C1 do novo fluxo (2025+)
- **`pre_analysis`**: C1 legado (flat)
- **`request`**: C2
- **`credit_check`**: consulta a bureau por CPF (SERASA/BVS/BACEN/SCR…)
- **`crivo_check`**: check do motor interno (Crivo), orquestrando evidências

> Regra prática para agentes: sempre que usar “C1/C2/C2S” em texto, mencione a entidade técnica correspondente na mesma seção.

---

## 2) Funil BNPL (end-to-end, mínimo útil)
### 2.1) C1 — pré-análise / simulação (`credit_simulation` ou `pre_analysis`)
Entrada típica: CPF + valor desejado. Saída típica: aprovado/recusado + (quando aprovado) condições financeiras.

- A decisão considera risco do paciente e risco da clínica, usando evidências (bureaus + Crivo).
- Pode existir simulação em nome de **responsável financeiro** (quando aplicável), o que altera o “CPF efetivo”.

### 2.2) C2 — pedido (`request`)
Nem todo C1 aprovado vira C2. No pedido, dados adicionais são solicitados e ocorre revalidação de risco.

> **Nota de escopo (atual)**: ainda não investigamos a fundo os contratos/detalhes de `request`. Este trecho é propositalmente conciso; será aprofundado no core de `requests`.

### 2.3) C2S — contrato assinado / originação
Nem todo pedido é aprovado. Nem todo pedido aprovado é assinado. Se assinado, conta como originação; pode haver cancelamento mesmo após assinatura.

> **Nota de escopo (atual)**: detalhamento de fontes/tabelas de C2S e pós-C2S será feito quando o foco migrar para `requests`.

### 2.4) Pós-C2S — ciclo de vida
Após assinatura, pode haver cancelamento, fraude, inadimplência, cobrança e renegociação. Estes eventos alimentam recalibração de risco e operação.

---

## 3) Guardrails obrigatórios (dual-run 2025+)
### 3.1) `PRE_ANALYSES` é polimórfica (NÃO QUEBRE ISTO)
Em `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES`:
- **Nunca** use `PRE_ANALYSIS_ID` sozinho como chave canônica.
- Chave canônica para “C1 unificado”: **(`PRE_ANALYSIS_TYPE`, `PRE_ANALYSIS_ID`)**
- Quando `PRE_ANALYSIS_TYPE='credit_simulation'`:
  - `PRE_ANALYSIS_ID == CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS.id`
- Existem **colisões históricas** de `PRE_ANALYSIS_ID` entre tipos diferentes.

### 3.2) Legado (`pre_analysis`) ≠ novo (`credit_simulation`)
- Se `PRE_ANALYSIS_TYPE='credit_simulation'`: **reusar** enrichment de `credit_simulation` (não reprocessar como legado).
- Se `PRE_ANALYSIS_TYPE='pre_analysis'`: seguir o pipeline legado (ver `docs/reference/PRE_ANALYSIS_ENTITY_GUIDE.md`).

### 3.3) Reprocessamento/snapshot
Algumas tabelas podem ter múltiplas linhas por ID (reprocessamento/snapshot).  
Quando necessário, deduplicar pegando o registro mais recente (`*_UPDATED_AT` quando existir).

---

## 4) Fontes de dados (mapa curto por domínio)
### 4.1) Clínicas (perfil/segmentação)
- `CAPIM_DATA_DEV.POSSANI_SANDBOX.CLINIC_MOST_RELEVANT_INFO` (chave `clinic_id`)
  - `is_independent_clinic`, `business_segmentation`, `clinic_credit_score`

### 4.2) Engajamento (SaaS)
- `CAPIM_DATA.CAPIM_ANALYTICS.CLINIC_ACTIVITY` (volumosa; requer recortes por `activity_type`)

### 4.3) C1 — novo fluxo (`credit_simulation`)
- `CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_LEADS`
- `CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS` (**fonte da verdade**)
- Bridge CPF/cadastro: `CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API` (via `patient_id` e `financial_responsible_id`)
- Nota (ETL incompleto): `CAPIM_DATA.RESTRICTED.SOURCE_RESTRICTED_CREDIT_SIMULATIONS` (tem lacunas); nele `PRE_ANALYSIS_ID = credit_simulation_id`.

### 4.4) C1 — legado (`pre_analysis`)
- `CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API` (flat; contém CPF e cadastro básico)
- `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK` (features curadas por `pre_analysis_id`)
- `CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION` (execução/decisão; `ENGINEABLE_TYPE/ENGINEABLE_ID`)

Guia oficial (chaves, bridges, guardrails): `docs/reference/PRE_ANALYSIS_ENTITY_GUIDE.md`.

### 4.5) C2 — pedidos (`request`) (parcial)
- `CAPIM_DATA.SOURCE_STAGING.SOURCE_REQUESTS` (raw)
- `CAPIM_DATA.CAPIM_ANALYTICS.ENRICHED_REQUESTS` (analítica; economics)

### 4.6) Bureau e motor (C1/C2)
- **Credit checks (bureau por CPF)**: `CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API`
  - Notas: `docs/reference/CREDIT_CHECKS_INCREMENTAL_API_NOTES.md`
  - Contratos/paths: `docs/reference/PAYLOAD_CONTRACTS_MAP.md`
- **Crivo checks (motor)**: `CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS`
  - Notas: `docs/reference/CRIVO_CHECKS_NOTES.md`
  - Ordem de chamadas entre bureaus (ex.: “BVS → SERASA → SCR”) e lógica de cache são **detalhes voláteis**; não assumir sem validar por período.
  - Para validar/monitorar: `queries/audit/inventory_credit_checks_crivo_checks.sql` + `queries/audit/audit_payload_paths_sampling.sql` (e, quando relevante, o core `docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md`).

---

## 5) Execução (recomendação deste repo)
- **Default**: **SQL-first no Snowflake Worksheet**, usando arquivos em `queries/` (self-contained).
- **Python local**: só para materialização/orquestração (CTAS) quando fizer sentido; evitar processar dados localmente.

Runbook: `docs/runbooks/EXECUTION_IN_SNOWFLAKE.md`.

---

## 6) “Primeiras queries” para se orientar
- Dual-run / chaves / cobertura: `queries/audit/audit_pre_analyses.sql`
- Inventário e drift de payloads: `queries/audit/inventory_credit_checks_crivo_checks.sql`
- Fill-rate por paths (anti-cegueira): `queries/audit/audit_payload_paths_sampling.sql`
- Enrichment C1 novo: `queries/enrich/enrich_credit_simulations_borrower.sql`
- Enrichment C1 unificado (por tipo): `queries/enrich/enrich_pre_analyses_borrower.sql`

---

## 7) Estrutura do repositório (atual)
```text
bnpl-funil/
├── docs/                # Entry points + cores + reference/runbooks/adr
├── queries/             # Queries canônicas (Snowflake-first)
├── outputs/             # Outputs versionados (md/csv) gerados por runs pontuais
└── src/                 # Scripts “core” (materialização/orquestração) + utils Snowflake
```

