# Investigação: associação `credit_simulations` ↔ `credit_checks` (amostral)

> Última revisão: **2025-12-30T13:55-03:00** (snapshot; números podem mudar).  
> Para recalcular: rode queries de auditoria/validação no Snowflake e compare por período.

## Objetivo
Preparar o terreno para enriquecer o **grão `credit_simulation_id`** com dados de `CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API` (ver doc específica), entendendo:
- Como recuperar o **CPF efetivo** (paciente vs responsável financeiro).
- Por que o ETL oficial (tabela `SOURCE_RESTRICTED_CREDIT_SIMULATIONS`) tem **faltantes**.
- Qual estratégia **coerente e performática** para associar `credit_simulations` a *N* `credit_checks`, considerando **cache (~15 dias)** e `clinic_id (retail_id)`.

## Fontes envolvidas (escopo desta investigação)
- **Fonte da verdade (simulações)**: `CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS`
- **Bridge CPF ↔ pessoa**: `CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API` (via `patient_id` e/ou `financial_responsible_id`)
- **Checks**: `CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API`
- **ETL oficial incompleto**: `CAPIM_DATA.RESTRICTED.SOURCE_RESTRICTED_CREDIT_SIMULATIONS`
  - **Semântica esperada**: quando a entidade é do novo fluxo, `pre_analysis_id` corresponde ao `credit_simulation_id` (`CREDIT_SIMULATIONS.id`).
  - **Guardrail (2025+)**: os fluxos legado (`pre_analysis`) e novo (`credit_simulation`) coexistem; em tabelas “unificadas” como `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES`, `PRE_ANALYSIS_ID` é polimórfico e deve ser interpretado junto de `PRE_ANALYSIS_TYPE`.
- **Leads**: `CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_LEADS` (campos `credit_lead_requested_amount`, `under_age_patient_verified`)

## Premissas fornecidas
- Uma `credit_simulation` pode ter **N credit checks**.
- Cadeia de chamadas e `source/kind` evoluíram com o tempo.
- **Cache**: checks podem ser reaproveitados; janela aparente de **~15 dias**, também condicionada por `clinic_id`.
- CPF efetivo: se `financial_responsible_id` existir e for diferente de `patient_id`, usar CPF do responsável.

## Achados (amostrais)
> Nota: manter números com “as of” e recomputar quando necessário.

### 1) Cobertura do ETL oficial
- `credit_simulations` (fonte da verdade): **268.960**
- `source_restricted_credit_simulations` (ETL oficial): **153.458**
- missing no ETL: **115.502** (≈ 42,95%)

### 2) CPF efetivo (completude)
- casos `financial_responsible_id <> patient_id`: **4.698** (≈ **1,75%**)
- amostra 50k: `cpf_effective` presente em **49.995/50.000** (≈ **99,99%**)

### 3) Constância de campos de lead
- `credit_lead_requested_amount` constante por `credit_lead_id`: **0% violações observadas**
- `under_age_patient_verified` constante por `credit_lead_id`: **0% violações observadas**

### 4) Heurísticas de associação (CPF + tempo)
Achado: janela “só lookback [-15d, 0]” perde a maioria dos checks; janelas centradas no evento funcionam muito melhor.

Janelas recomendadas (com ranking de leniência):
- **±1h**
- **±24h**
- **lookback 15d**
- **lookback 180d**

> Core: ver `docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md` e ADR 0001.

