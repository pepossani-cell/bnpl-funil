# Core — Enriquecimento de `pre_analysis` (C1 legado)
> Última revisão: **2025-12-31T00:00-03:00**.

## Propósito
Este documento define o **core** do enrichment de C1 legado no grão `pre_analysis_id`, de forma **Snowflake-first** e auditável. Ele existe para evitar que agentes/humanos tentem aplicar regras de `credit_simulations` diretamente ao legado sem considerar:
- ausência de chaves determinísticas para credit checks/crivo,
- dual-run 2025+ (legado vs novo fluxo),
- tabelas curadas específicas do legado.

> Para a entidade `pre_analysis` como “dicionário” (chaves, bridges e tabelas estudadas), ver: `docs/reference/PRE_ANALYSIS_ENTITY_GUIDE.md`.

---

## Grão e tempo do evento
- **Grão**: 1 linha por `pre_analysis_id` (legado).
- **Evento (tempo do C1)**: para C1 unificado via `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES`, usar `PRE_ANALYSIS_CREATED_AT` (materializado como `c1_created_at` em `queries/enrich/enrich_pre_analyses_borrower.sql`).

---

## Escopo (muito importante)
### Não reprocessar `PRE_ANALYSIS_TYPE='credit_simulation'`
Quando `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES.PRE_ANALYSIS_TYPE='credit_simulation'`:
- `PRE_ANALYSIS_ID` corresponde ao `credit_simulation_id`.
- O enrichment **não** deve ser refeito aqui; reusar o core de simulations: `docs/ENRICHMENT_CREDIT_SIMULATIONS_CORE.md`.

### Escopo deste core
Este core cobre **apenas**:
- `pre_analysis` legado (flat) e seu enrichment.

---

## Fontes recomendadas (prioridade)
### Base e bridge (CPF/cadastro)
- `CAPIM_DATA.RESTRICTED.SOURCE_PRE_ANALYSIS_API` (CPF + dados básicos)

### Financing (prazo/parcela/dívida) — detalhe importante do legado
Na `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSES` para `PRE_ANALYSIS_TYPE='pre_analysis'`, a coluna `FINANCING_CONDITIONS` pode vir **0% preenchida** (achado empírico).  
Para reconstruir o eixo de financing no legado, a fonte “pronta” observada é:
- `MINIMUM_TERM_AVAILABLE` / `MAXIMUM_TERM_AVAILABLE` (termos min/max)
- `INTEREST_RATES_ARRAY` (OBJECT com chaves tipo `"3..6"`, `"7..9"`, … → taxa mensal por faixa de prazo)

No `queries/enrich/enrich_pre_analyses_borrower.sql`, os campos `financing_*` do legado são estimados a partir desses 3 inputs (com fórmula de PMT aproximada).

### Features curadas (recomendado como “primeira escolha”)
- `CAPIM_DATA.CAPIM_ANALYTICS.PRE_ANALYSIS_CREDIT_CHECK`
  - Features já agregadas por `pre_analysis_id` (SERASA/BVS/SCR + flags/timestamps)
  - Mais simples/rápido do que reparsear payload bruto

### Fallback “dbt-like” (hash+tempo; alta cobertura no legado)
- `CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_SERASA`
- `CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_BOA_VISTA_SCORE_PF`
- `CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_BOA_VISTA_SCPC_NET`
- `CAPIM_DATA.RESTRICTED.SOURCE_CREDIT_CHECKS_API_SCR_REPORT`
  - Associação por `hash_cpf` + janela **0..15 dias** (mesma lógica do dbt/PACC, com ajuste `DATEADD('hours', -3, consulted_at)`).
  - No SCPC, `BVS_TOTAL_DEBT` e `BVS_TOTAL_PROTEST` podem ser usados como fallback de **valores** de negativação quando SERASA não traz `balance`.

### Payload bruto (quando precisar de auditabilidade/anti-cegueira)
- `CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API`
  - Associação por **CPF + tempo** (janelas centradas em `created_at`, com ranking de leniência)

### Motor/execução (opcional; explicabilidade e variáveis agregadas)
- `CAPIM_DATA.SOURCE_STAGING.SOURCE_CREDIT_ENGINE_INFORMATION`
  - Join por `ENGINEABLE_TYPE='pre_analysis'` e `ENGINEABLE_ID=pre_analysis_id`
  - **A validar**: existe coluna `CRIVO_PATIENT_ID`, mas em amostra recente ela pode vir nula; não dependa dela para demografia.
  - `SOURCE='n8n'` (2025-10+ observado): `DATA` é JSON com features agregadas (scores/negativação/renda/CEP/nascimento).
    - Recomendação: usar apenas como **fallback** com `*_source` explícito (pode embutir regras/decisão).
    - Compatibilidade (aprovado): `pefinSerasa/refinSerasa/protestoSerasa` podem ser usados como **contagens SERASA compatíveis** no eixo de negativação.

### Demografia “last resort” (interoperabilidade histórica)
- `CAPIM_DATA.RESTRICTED.INCREMENTAL_SENSITIVE_DATA_API`
  - Quando o legado tiver CPF resolvido via `SOURCE_PRE_ANALYSIS_API`, dá para complementar **cidade/UF/CEP/birthdate** por CPF (fallback), para viabilizar série histórica.

---

## Queries canônicas (o que rodar)
- Auditoria (curado): `queries/audit/audit_pre_analysis_credit_check.sql`
- Auditoria (motor): `queries/audit/audit_credit_engine_information.sql`
- Auditoria (financing legado): `queries/audit/audit_pre_analyses_financing_legacy.sql`
- Enrichment unificado (quando precisar juntar legado+novo por tipo): `queries/enrich/enrich_pre_analyses_borrower.sql`

---

## Execução segura (volume alto) — amostragem representativa primeiro
O volume de C1 legado (`pre_analysis`) costuma ser **muito maior** do que `credit_simulations` e cobre um período maior.  
Recomendação prática:
- **Nunca** começar com materialização full; começar com amostra por **múltiplos períodos**.
- Amostrar de forma **estratificada por mês** e por `PRE_ANALYSIS_TYPE` (pelo menos `pre_analysis`), para capturar mudanças de contrato/cobertura ao longo do tempo.

Sugestão (padrão):
- selecionar N linhas por mês (ex.: 200–2.000) para 6–12 meses distribuídos;
- incluir pelo menos alguns meses antigos (ex.: 2023/2024) se existirem no legado;
- medir, na amostra:
  - % com CPF resolvido via `SOURCE_PRE_ANALYSIS_API`;
  - % com matches em credit checks nas janelas (±1h/±24h/-15d/-180d);
  - fill-rate dos eixos (cadastro/negativação/renda/scores).

> Para rodar amostragem sem full scan: use o modo **SAMPLING (opcional)** dentro de `queries/enrich/enrich_pre_analyses_borrower.sql` (`params.sample_n_per_month` + CTE `sample_months`).
