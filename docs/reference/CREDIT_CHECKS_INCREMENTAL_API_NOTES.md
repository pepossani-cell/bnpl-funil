# INCREMENTAL_CREDIT_CHECKS_API — achados (investigação amostral)

> Última revisão: **2025-12-30T13:55-03:00** (snapshot; números podem mudar).  
> Para recalcular: usar `queries/audit/inventory_credit_checks_crivo_checks.sql` e auditorias em `queries/audit/audit_payload_paths_sampling.sql`.

## Escopo
Sumário consolidado da investigação **amostral** da tabela `CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API`.

Objetivo prático: **enriquecer “credit simulations”** com sinais vindos dos **credit checks**. Importante: **uma credit simulation pode ter N credit checks**. Nesta etapa, estamos focados em entender semântica/estrutura do que dá para extrair **por credit check**, sem resolver ainda a bridge de chaves entre entidades.

## Esquema (INFORMATION_SCHEMA)
- `ID` (NUMBER) — identificador da checagem.
- `DATA` (VARIANT) — payload bruto (JSON/array), estrutura varia por `SOURCE`.
- `CREATED_AT`, `UPDATED_AT` (TIMESTAMP_NTZ).
- `CPF` (TEXT) — retorna já mascarado em amostras.
- `SOURCE` (TEXT) — origem do bureau/motor.
- `KIND` (TEXT) — tipo de checagem (score, renda, etc.).
- `NEW_DATA_FORMAT` (BOOLEAN) — indica novo layout (observado em SERASA).
- `__HEVO_ID`, `__HEVO__INGESTED_AT`, `__HEVO__LOADED_AT` — metadados de ingestão (Hevo).

## Volume por SOURCE (top)
- `boa_vista_scpc_net`: ~2.26M
- `serasa`: ~1.93M
- `boa_vista_score_pf`: ~1.38M
- `bacen_internal_score`: ~457k
- `scr`: ~44k
- `boa_vista_acerta`: ~1.3k
- `score-bacen-v2-test4`: 1

## Distribuições úteis (amostras e agregações leves)

### KIND por SOURCE (principais combinações)
(Contagens observadas)
- `boa_vista_scpc_net`
  - `check_score`: 1.528.473
  - `NULL`: 735.793
- `boa_vista_score_pf`
  - `check_score`: 886.484
  - `NULL`: 490.943
- `serasa`
  - `check_score_without_income`: 795.045
  - `check_income_only`: 662.112
  - `NULL`: 466.125
  - `check_score`: 5.130
- `bacen_internal_score`: `check_score` 457.303
- `scr`: `NULL` 43.583
- `boa_vista_acerta`: `NULL` 1.277; `check_score` 31

### SERASA — NEW_DATA_FORMAT x KIND
(Contagens observadas em `SOURCE='serasa'`)
- `NEW_DATA_FORMAT = TRUE`
  - `check_score_without_income`: 428.299
  - `check_income_only`: 349.439
- `NEW_DATA_FORMAT = FALSE/NULL`
  - `NULL`: 466.125
  - `check_score_without_income`: 366.746
  - `check_income_only`: 312.673
  - `check_score`: 5.130

## Observações por SOURCE
- **SERASA**
  - **Dois formatos relevantes** (ambos coexistem na mesma tabela), com diferenças importantes:
    - **Novo formato** (`NEW_DATA_FORMAT = TRUE`):
      - `DATA` é **OBJECT** e contém **`reports`**.
      - `reportName` observado: **`COMBO_CONCESSAO`** (dominante).
      - Para `KIND='check_score_without_income'`:
        - `negativeData` aparece como `DATA:reports[*].negativeData`.
      - **Cadastro/demografia no novo formato (via `registration`)**
        - `registration` aparece em `DATA:reports[*].registration`.
      - Para `KIND='check_income_only'`:
        - payload tende a trazer `score`, `range`, `scoreModel` (sem depender de `reports`).
        - **Atenção**: para `scoreModel ILIKE 'HRP%'`, `score` tem evidência forte de ser **RENDA ESTIMADA em centavos** (ver ADR 0003).
    - **Formato antigo** (`NEW_DATA_FORMAT = FALSE/NULL`):
      - `DATA` é majoritariamente **ARRAY** com B-codes.
      - Ex.: `B001/B002/B003/B004` (cadastro), `B280` (score), `B357/B361` (negativação agregada).
      - **Achado (dez/2025, auditoria amostral)**: `B357/B361.total_occurrence_value` aparece como inteiro em **R$** (evitar `÷100` nesses campos).

- **boa_vista_score_pf**
  - `KIND`: `check_score`.

- **boa_vista_scpc_net**
  - `DATA` é array de blocos (ex.: `"249"` cadastro, `"141"` sumário de débitos).

- **bacen_internal_score**
  - `DATA.predictions[0]`: `score`, `probs0/1`, flags, limites e atributos financeiros.

- **SCR**
  - `resumoDoCliente`, `listaDeResumoDasOperacoes` e vencimentos.

- **boa_vista_acerta**
  - baixo volume; JSON com blocos `essencial`/`identificacao`/`consultas_anteriores`.

## Campos úteis a extrair (sugestão)
Organizado pelo que queremos enriquecer “por credit check”:

### Cadastro / demografia / contato / endereço
- SERASA antigo: `B001/B002/B003/B004`
- boa_vista_scpc_net: bloco `"249"`

### Negativação / restrições
- SERASA new: `reports[*].negativeData.{pefin, refin, notary, check}.summary.(count,balance)`
- SERASA old: `B357/B358`, `B361/B362`, `A900`
- boa_vista_scpc_net: blocos `"123"` e `"141"`

### Renda / proxies
- bacen_internal_score: limites/somatórios/flags
- SCR: vencimentos e somatórios raw
- SERASA: `check_income_only` (renda estimada quando `HRP%`)

### Scores (por bureau)
- SERASA old: `B280.*`
- boa_vista_score_pf: `score_positivo.*`
- bacen_internal_score: `predictions[0].score`

