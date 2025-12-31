# Mapa de contratos de payload (Credit Checks + Crivo) — tipo → formato → paths → features (core)

> Última revisão: **2025-12-30T13:55-03:00**.

> Objetivo: manter uma referência **única** e auditável de “onde está cada sinal” nos JSONs, com semântica (unidade/escala) e tolerância a variações.
>
> Queries relacionadas:
> - Inventário por período (volumes, `TYPEOF`, keys): `queries/audit/inventory_credit_checks_crivo_checks.sql`
> - Auditoria anti-cegueira (fill-rate por paths): `queries/audit/audit_payload_paths_sampling.sql`
> - Enriquecimento (fonte da verdade, 1 linha por simulation): `queries/enrich/enrich_credit_simulations_borrower.sql`

---

## Credit checks (`CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API`)

### SERASA — **novo formato** (`new_data_format=TRUE` OU payload `OBJECT` com `reports`)
- **Kinds relevantes**
  - `check_score_without_income`
  - `check_income_only`

- **Formato**
  - `TYPEOF(data)='OBJECT'`
  - Top-level comum: `reports` (em `check_score_without_income`)

- **Paths “core” (cadastro/demografia)**
  - `data:reports[*].registration.birthDate` → **data de nascimento** (YYYY-MM-DD)
  - `data:reports[*].registration.consumerGender` → **sexo** (`M`/`F`, pode vir vazio)
  - `data:reports[*].registration.address.zipCode` → **CEP**
  - `data:reports[*].registration.statusRegistration` / `statusDate` → **status cadastral**
  - Evidências (flags): `registration.phone`, `registration.address`

- **Paths “core” (negativação)**
  - `data:reports[*].negativeData.{pefin,refin,notary,check}.summary.count` → **contagens**
  - `data:reports[*].negativeData.{...}.summary.balance` → **saldo/valor** (quando disponível)

- **Paths “core” (score SERASA)**
  - **Preferir** `check_score_without_income` quando houver score no payload.
  - Paths auditados (variam por versão):
    - `data:score` (top-level)
    - `data:data.score` (bloco alternativo)
    - `data:reports[*].score` (escalar)
    - `data:reports[*].score.score` (objeto)
  - **Sanidade**:
    - valores `<=0` → `NULL`
    - valores “escalados” (ex.: `435000`, `8975000`) exigem normalização (ver ADR 0004)

- **Paths “core” (renda/proxies) — `check_income_only`**
  - `data:score` + `data:scoreModel` + `data:range`
  - **Semântica (crítica)**:
    - quando `scoreModel ILIKE 'HRP%'`, `score` tem evidência forte de ser **RENDA ESTIMADA em centavos** (ver ADR 0003).

---

### SERASA — **formato antigo** (`new_data_format=FALSE` e payload sem `reports`)
- **Formato**
  - `TYPEOF(data)='ARRAY'`
  - Conteúdo por B-codes (ex.: `B001`, `B002`, `B003`, `B004`, `B280`, `B357`, `B361`, …)

- **Cadastro (core)**
  - `B002.birth_date` / `B001.birthdate` / `B001.birth_date` → nascimento
  - `B002.gender` / `B001.gender` → sexo
  - `B004.zip_code` / `B004.zipcode` / `B004.cep` → CEP
  - evidências: presença de `B003` (telefone) e `B004` (endereço)

- **Score (core)**
  - `B280.score` → score
  - `B280.score_range_name` → faixa
  - `B280.delinquency_probability_percent` → probabilidade (%)

- **Negativação (core)**
  - `B357.occurrences_count` + `B357.total_occurrence_value` → pendências/REFIN (**observado**: `total_occurrence_value` vem como inteiro em **R$**; **não** dividir por 100)
  - `B361.occurrences_count` + `B361.total_occurrence_value` → protestos/cartório (**observado**: `total_occurrence_value` vem como inteiro em **R$**; **não** dividir por 100)

---

### BOA VISTA — `boa_vista_scpc_net`
- **Formato**
  - `TYPEOF(data)='ARRAY'` de blocos (objetos com chaves `"249"`, `"141"`, etc.)

- **Cadastro (core)**
  - `data[*]."249".birthdate` (DDMMYYYY) → nascimento
  - `data[*]."249".status` → status (quando existir)

- **Negativação (core)**
  - `data[*]."141".debit_total_count` → contagem
  - `data[*]."141".debit_total_value` → valor total (escala precisa ser confirmada; auditar antes de canonizar)

---

### BOA VISTA — `boa_vista_score_pf`
- **Formato**: `TYPEOF(data)='OBJECT'`
- **Score (core)**:
  - `data:score_positivo:score_classificacao_varios_modelos:score`

---

### BACEN — `bacen_internal_score`
- **Formato**: `TYPEOF(data)='OBJECT'`
- **Score/proxies (core)**:
  - `data:predictions[0].score`
  - `data:predictions[0].limitesdecredito`
  - `data:predictions[0].valorvencimento_mean_credit_limits`
  - `data:predictions[0].is_not_banked` (normalizar string/boolean)

---

### SCR — `scr`
- **Formato**: `TYPEOF(data)` varia; paths observados:
  - `data:resumoDoCliente`
  - `data:listaDeResumoDasOperacoes` (ou dentro de `resumoDoCliente`)
- **Core mínimo (materializado)**:
  - `scr_operations_count`, `scr_vencimentos_count` (contagens)
  - `scr_sum_valor_raw` (somatório **raw**; unidade/escala ainda não canonizada — interpretar com cautela e auditar antes de tratar como R$)

---

## Crivo checks (`CAPIM_DATA.SOURCE_STAGING.SOURCE_CRIVO_CHECKS`)

### Estruturas principais
- `key_parameters:campos` → **OBJECT**
- `bureau_check_info:campos` → **ARRAY** de `{nome, tipo, valor, ...}`
- `bureau_check_info:drivers` → **ARRAY**

### Paths “core” (negativação e score — curadoria)
- `bureau_check_info:campos[]` filtrando por `nome`:
  - `Score Serasa` → score
  - `PEFIN Serasa`, `REFIN Serasa`, `Protesto Serasa` → **contagens/métricas** (não valores monetários)

### Paths “core” (cadastro)
- `bureau_check_info:campos[]`:
  - `CEP do Proponente` → CEP
  - `Telefone do proponente` → evidência (não persistir telefone cru)
- `bureau_check_info:drivers[*].produtos["api DataBusca - Consulta Dados Pessoa - PF"]`:
  - campos contendo `sexo` → mapeamento inferido: `1→M`, `2→F`

### Paths “core” (renda/proxies)
- `key_parameters:campos.BacenScore`
- `key_parameters:campos.CreditLimits`
- `key_parameters:campos.OverduePortfolio` (string PT-BR; parse para número)
- `key_parameters:campos.Loss` (string PT-BR; parse para número)
- `bureau_check_info:campos[]` (`nome='CREDILINK - Renda Presumida'`) → renda presumida (R$)

---

## Checklist “anti-cegueira” (o que monitorar)
- Mudança de `TYPEOF(data)` (ARRAY ↔ OBJECT) por `source/kind`
- Surgimento/desaparecimento de `reports` em SERASA new
- `report.score` mudando de escalar → objeto (e vice-versa)
- Mudança na distribuição de `scoreModel` em `check_income_only`
- Fill-rate de paths core caindo (birthDate, negativeData.summary.count, bacen predictions[0], etc.)

