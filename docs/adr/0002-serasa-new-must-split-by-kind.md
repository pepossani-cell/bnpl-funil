# ADR 0002 — SERASA new deve ser separado por `kind` (evitar perda de sinal)

## Contexto
SERASA “new format” aparece em `INCREMENTAL_CREDIT_CHECKS_API` com múltiplos `kind` coexistindo. Tentar escolher “1 melhor SERASA por simulation” quebra features porque:
- `kind='check_score_without_income'` tende a conter `reports[*].registration` e `reports[*].negativeData`
- `kind='check_income_only'` tende a conter `score/range/scoreModel` e pode não conter `reports`

## Decisão
Separar a seleção de SERASA new por `kind`:
- **cadastro + negativação + score SERASA**: priorizar `check_score_without_income`
- **renda/proxies**: usar `check_income_only` quando houver evidência semântica (ver ADR 0003)

## Consequências
- Evita “nulos falsos” em cadastro/negativação por escolher um payload que não tem `reports`.
- Mantém semântica e auditabilidade por eixo.

## Implementação
- Core: `queries/enrich/enrich_credit_simulations_borrower.sql`
- Anti-cegueira: `queries/audit/audit_payload_paths_sampling.sql`

