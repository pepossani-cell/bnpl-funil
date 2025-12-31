# ADR 0003 — SERASA `check_income_only.score` pode ser renda (centavos), não score

## Contexto
No SERASA `kind='check_income_only'`, o campo `score` é ambíguo. Em produção observamos casos em que `scoreModel ILIKE 'HRP%'` e o valor de `score` tem evidência forte de representar **renda estimada mensal em centavos** (e não um score 0..1000).

## Decisão
Quando `scoreModel ILIKE 'HRP%'`:
- interpretar `score` como **renda estimada (centavos)** e materializar como **R$** (`score / 100`)
- **não** usar esse campo como `serasa_score` (para não misturar semânticas)

## Consequências
- Preserva semântica por eixo (renda/proxies vs score).
- Evita regressões comuns (dividir por 100 indevidamente ou tratar como score).

## Implementação
- Core: `queries/enrich/enrich_credit_simulations_borrower.sql`
- Auditoria: `queries/audit/audit_payload_paths_sampling.sql`, `queries/audit/audit_enrichment_correctness_sampling.sql`

