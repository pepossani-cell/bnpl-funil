# ADR 0004 — Normalização de score e tratamento de inválidos (SERASA)

## Contexto
Observamos `serasa_new_score` chegando com:
- valores inválidos (`<=0`, ex.: `-1`)
- valores em escalas diferentes (ex.: `435000`, `8975000`)

Sem normalização, o enrichment “passa” valores fora da faixa plausível e cria drift artificial.

## Decisão
Regras estáveis:
- `raw <= 0` → `NULL`
- normalizar escala (heurística):
  - `raw >= 1_000_000` → `raw / 10_000`
  - `raw >= 1_000` → `raw / 1_000`
  - senão usar `raw`
- “clip” final: score > 1000 → `NULL`

## Consequências
- Melhora sanidade e comparabilidade temporal.
- Requer auditorias recorrentes para detectar mudanças de contrato (anti-cegueira).

## Implementação
- Core: `queries/enrich/enrich_credit_simulations_borrower.sql`
- Anti-cegueira: `queries/audit/audit_payload_paths_sampling.sql`

