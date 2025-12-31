# ADR 0001 — Seleção dinâmica de evidência por eixo (anti-mascaramento)

## Contexto
No enrichment de C1, existem múltiplas fontes candidatas (credit checks em janelas com diferentes leniências, crivo resolvido por engineable/CPF+tempo, etc.). Um `COALESCE` fixo (“Crivo sempre vence”, “SERASA sempre vence”) gera **mascaramento** quando uma evidência leniente (ex.: `cpf_fallback_15d/180d`) sobrepõe uma evidência estrita (ex.: credit check em ±1h).

## Decisão
Selecionar a melhor evidência **por eixo** (cadastro, negativação, renda/proxies, scores) usando regra estável:
- **estrito primeiro** (menor leniência / estágio mais confiável)
- depois **proximidade temporal** (menor \(|minutes_from_event|\))
- depois **completude do eixo** (desempate)

Persistir colunas de auditoria por eixo (ex.: `*_evidence_source`, `*_evidence_match_stage`, `*_evidence_minutes_from_cs`).

## Consequências
- Reduz regressões e “nulos falsos” causados por escolha de fonte errada.
- Exige que o output carregue **linhagem** (stage/minutes/source) para auditoria.

## Implementação
- SQL core: `queries/enrich/enrich_credit_simulations_borrower.sql`
- Auditorias: `queries/validate/validate_enrich_credit_simulations_borrower_sampling.sql`, `queries/audit/audit_enrichment_correctness_sampling.sql`

