# Investigação: `SOURCE_CRIVO_CHECKS` ↔ `credit_simulations` (amostral)

> Última revisão: **2025-12-30T13:55-03:00** (snapshot; números podem mudar).

## Objetivo
Entender `CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS` e como associá-la a `CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS`, com foco em:
- caracterizar `ENGINEABLE_TYPE/ENGINEABLE_ID` como vínculo “oficial”;
- usar CPF em `KEY_PARAMETERS:campos:"CPF"` como fallback;
- imputar `crivo_check_id` para simulations com nulo (alto recall), respeitando cache (~15 dias).

## Estratégia canônica (v1)
1) **Vínculo oficial (engineable)**:
   - `ENGINEABLE_TYPE='CreditSimulation'` e `ENGINEABLE_ID = credit_simulation_id`
2) **Fallback por CPF + tempo**:
   - `cpf_primary`: ±1h
   - `cpf_fallback_15d`: lookback 15d
   - `cpf_fallback_180d`: lookback 180d (opcional)
3) **Leniência entra na seleção por eixo** (ver ADR 0001):
   - um Crivo resolvido por `fallback_180d` não deve mascarar credit check estrito.

## Achados (amostrais)
### ENGINEABLE_TYPE
- `CreditSimulation`: **195.128**
- `Request`: **5**

### CPF em `KEY_PARAMETERS`
- cobertura em amostra 50k: **100%**
- formato: mascarado (ex.: `393.677.048-44`)

### `ENGINEABLE_ID` mapeia para simulation
- match observado: **100%** (quando existe linha no staging)

### Nulidade de `credit_simulations.crivo_check_id`
Variável no tempo (ex.: 2025-01 a 2025-03 ~100% nulo; 2025-09 em diante alto).

### Recuperação de `crivo_check_id` nulo (amostral)
Em amostra de 50.000 simulations com `crivo_check_id IS NULL`:
- `engineable`: **35,21%**
- `cpf_primary`: **0,10%**
- `cpf_fallback_15d`: **1,41%**
- `cpf_fallback_180d`: **5,23%**

Cobertura total:
- com lookback **15d**: **36,72%**
- com lookback **180d**: **40,54%**

### Cache/reuso
`crivo_check_id` aparece repetido em múltiplas simulations (máximo observado: **59**).

### “Valor escondido” (JSON)
Boa parte do valor do Crivo está em:
- `BUREAU_CHECK_INFO:campos` (ARRAY `{nome,tipo,valor}`)
- `KEY_PARAMETERS:campos` (OBJECT)

Importante:
- `PEFIN/REFIN/Protesto` no Crivo se comportam como **contagens/métricas curadas**, não valores monetários.
- strings com `R$` no Crivo tendem a ser **texto de política/limiar**, não saldo total.

