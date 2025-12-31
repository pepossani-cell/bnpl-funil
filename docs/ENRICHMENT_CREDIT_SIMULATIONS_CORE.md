# Core — Enriquecimento de `credit_simulations` (C1)
> Última revisão: **2025-12-30T13:55-03:00**.

## Propósito
Este documento é a **fonte de verdade (core)** para o enrichment de C1 no grão `credit_simulation_id`. Ele define:
- grão, chaves e tempo do evento;
- quais tabelas são usadas e por quê;
- regras canônicas de associação (credit checks e crivo);
- guardrails de semântica;
- quais queries rodar.

> Decisões homologadas e “por que fazemos assim” ficam em `docs/adr/` (evita inchamento e repetição).

---

## Grão, chaves e tempo do evento
- **Grão**: 1 linha por `CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS.id` (`credit_simulation_id`).
- **Tempo do evento**: `credit_simulations.created_at` (padrão do projeto: `created_at` para todas as entidades).
- **Chaves úteis**:
  - `credit_simulation_id`, `credit_lead_id`, `retail_id` (clinic), `patient_id`, `financial_responsible_id`.

---

## Tabelas no core (fonte de verdade + bridges)
### Fonte da verdade (C1)
- `CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS`

### Bridge CPF/cadastro interno (para CPF efetivo)
- `CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API`
  - Join por `patient_id` e `financial_responsible_id`

### Credit checks (payload bruto)
- `CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API`
  - Não tem `credit_simulation_id` → associação por **CPF + tempo**

### Crivo checks (staging)
- `CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS`
  - Vínculo oficial: `ENGINEABLE_TYPE='CreditSimulation'` e `ENGINEABLE_ID = credit_simulation_id`
  - Fallback: CPF em `KEY_PARAMETERS:campos:"CPF"`

### Auxiliares
- `CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_LEADS` (campos constantes por `credit_lead_id`, quando necessários)

---

## Regras canônicas (associação e seleção)
### 1) CPF efetivo (paciente vs responsável)
Regra:
- Se `financial_responsible_id` existe e `!= patient_id`, **CPF efetivo = CPF do responsável**.
- Caso contrário, **CPF efetivo = CPF do paciente**.

### 2) Associação `credit_simulation` → `credit_checks` (N checks)
Como `INCREMENTAL_CREDIT_CHECKS_API` não tem chaves de simulação, associamos por **CPF efetivo + tempo**.

Janelas canônicas (com ranking de leniência):
- **strict_primary_1h**: ±1h (rank 0)
- **lenient_primary_24h**: ±24h (rank 1)
- **lenient_fallback_15d**: lookback 15d (rank 2)
- **lenient_fallback_180d**: lookback 180d (rank 3)

### 3) Resolução de `crivo_check_id`
Manter:
- `crivo_check_id` (original, pode ser NULL)
- `crivo_check_id_resolved` (original OU imputado)
- `crivo_resolution_stage` (linhagem)

Estágios (do mais confiável ao mais leniente):
- `original`
- `engineable` (via `SOURCE_CRIVO_CHECKS.engineable_id = credit_simulation_id`)
- `cpf_primary` (±1h por CPF)
- `cpf_fallback_15d`
- `cpf_fallback_180d`
- `unresolved`

### 4) Seleção de fonte por eixo (anti-mascaramento)
Não usar `COALESCE` fixo. A seleção é **dinâmica por eixo** conforme ADR 0001:
- `docs/adr/0001-dynamic-evidence-selection-per-axis.md`

### 5) Guardrails SERASA (decisões críticas)
- SERASA new deve ser separado por `kind` (ADR 0002)
- `check_income_only.score` pode ser renda (centavos), não score (ADR 0003)
- Normalização de score e inválidos (ADR 0004)

---

## Ordem de chamadas e cache (a validar; evitar “lore”)
**Não assumir** uma ordem fixa de chamadas (ex.: “BVS → SERASA → SCR”) nem uma janela de cache sem evidência por período.

- **Como validar ordem (proxy)**:
  - usar `queries/audit/inventory_credit_checks_crivo_checks.sql` para ver, por mês, volumes por `source/kind/new_data_format` e mudanças de contrato;
  - quando necessário, cruzar com timestamps relativos ao evento (via `queries/enrich/enrich_credit_simulations_borrower.sql`) e observar quais `source/kind` aparecem mais frequentemente em `±1h/±24h`.
- **Como tratar cache (no enrichment)**:
  - as janelas (±1h, ±24h, lookback 15d/180d) são **heurísticas do pipeline analítico** para maximizar recall mantendo linhagem;
  - qualquer afirmação de “cache do produto” deve ser registrada como **observação datada** e acompanhada de query/validação.

## Onde estão os detalhes (sem duplicar no core)
- Contratos de payload e paths: `docs/reference/PAYLOAD_CONTRACTS_MAP.md`
- Notas de credit checks: `docs/reference/CREDIT_CHECKS_INCREMENTAL_API_NOTES.md`
- Notas de crivo: `docs/reference/CRIVO_CHECKS_NOTES.md`
- Associação simulations ↔ checks: `docs/reference/CREDIT_SIMULATIONS_CREDIT_CHECKS_ASSOCIATION_NOTES.md`

---

## Queries canônicas (o que rodar)
### Enrichment
- Implementação (core): `queries/enrich/enrich_credit_simulations_borrower.sql`

### Validações e auditorias
- Validação amostral (100% SQL): `queries/validate/validate_enrich_credit_simulations_borrower_sampling.sql`
- Anti-cegueira (paths): `queries/audit/audit_payload_paths_sampling.sql`
- Inventário por período: `queries/audit/inventory_credit_checks_crivo_checks.sql`

