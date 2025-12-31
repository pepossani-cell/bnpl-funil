# Rodar em Snowflake (Worksheet-first) e quando usar Python local

> Última revisão: **2025-12-30T13:55-03:00**.

## Contrato de execução (padrão deste repo)
- **Default**: executar SQL no **Snowflake Worksheet** (queries autocontidas em `queries/`).
- **Python local**: permitido para **materialização/orquestração** (CTAS, rotinas repetíveis), evitando processar dados localmente.
- **Higiene**: scripts/arquivos criados só para investigação pontual devem ser **apagados** ao final.

## Recomendação prática (o que fazer na maioria dos casos)
- Enrichment (C1 simulations):
  - `queries/enrich/enrich_credit_simulations_borrower.sql`
- Validação/amostragem (100% SQL):
  - `queries/validate/validate_enrich_credit_simulations_borrower_sampling.sql`

## Quando faz sentido usar Python
Use Python local quando você precisar:
- materializar uma tabela/view em schema de desenvolvimento;
- executar um SQL file grande em lote;
- automatizar uma rotina (ex.: materialização periódica).

Sugestão:
- executar como módulo (evita problemas de import no Windows):
  - `python -m src.cli.run_sql_file --file queries/<arquivo>.sql`

