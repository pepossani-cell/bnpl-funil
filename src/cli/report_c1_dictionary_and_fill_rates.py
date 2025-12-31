"""
Gera:
- dicionário de colunas da view oficial C1 (nome, tipo, nullable, descrição heurística)
- fill-rate (não-nulo) por coluna: overall e por c1_entity_type

Saídas:
- outputs/c1_enriched_borrower_dictionary.md
- outputs/c1_enriched_borrower_fill_rates.csv
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

from src.utils.snowflake_connection import get_snowflake_connection


@dataclass(frozen=True)
class ColInfo:
    name: str
    data_type: str
    is_nullable: str


def _describe(col: str) -> str:
    c = col.lower()
    # chaves / metadados
    if c in {"c1_entity_type", "c1_entity_id", "c1_created_at", "clinic_id"}:
        return "Chave/tempo da entidade no funil."
    if c.startswith("c1_") and "rejection" in c:
        return "Motivo de reprovação/recusa."
    if c.startswith("c1_") and "approved" in c:
        return "Sinal/valor de aprovação (semântica definida no core)."
    if c.startswith("c1_") and "requested" in c:
        return "Valor solicitado/simulado."
    if c.startswith("c1_") and "appeal" in c:
        return "Retry/appeal: canônico no CS; inferido no legado com prob/source."
    if c.startswith("c1_") and "counter" in c:
        return "Contra-oferta (proxy no CS; canônico no legado)."

    # risk
    if c.startswith("risk_capim"):
        return "Risco paciente (0..5, -1, 9) e versões safe-for-aggregation."
    if c.startswith("payment_default_risk"):
        return "Probabilidade/score contínuo (não é o risco 0..5/-1/9)."
    if c.startswith("clinic_credit_score"):
        return "Risco/score dinâmico da clínica (join temporal via CLINIC_SCORE_LOGS)."

    # financing
    if c.startswith("financing_"):
        return "Condições de financiamento (prazo/parcela/dívida total min/max)."

    # cadastro/demografia
    if c.startswith("borrower_"):
        return "Cadastro/demografia do tomador (com *_source para linhagem)."
    if "cadastro" in c:
        return "Linhagem/feature do eixo cadastro/demografia."

    # negativação
    if c.endswith("_count") or c.endswith("_value") or "negativ" in c or c.startswith(("pefin_", "refin_", "protesto_")):
        return "Negativação (contagens/valores) + fonte."

    # renda / proxies
    if "income" in c or c.startswith("scr_") or "renda" in c:
        return "Renda e proxies (inclui SCR) + fonte."

    # scores
    if c.endswith("_score") or "score_source" in c or "boa_vista" in c or "serasa" in c:
        return "Scores de bureau + fonte."

    return "Campo do modelo C1 oficial (ver cores de enrichment)."


def _fetch_columns(conn, db: str, schema: str, view: str) -> list[ColInfo]:
    q = f"""
    SELECT
      column_name,
      data_type,
      is_nullable
    FROM {db}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '{schema}'
      AND table_name   = '{view}'
    ORDER BY ordinal_position
    """
    df = pd.read_sql(q, conn)  # uses snowflake connector DBAPI
    return [ColInfo(str(r["COLUMN_NAME"]), str(r["DATA_TYPE"]), str(r["IS_NULLABLE"])) for _, r in df.iterrows()]


def _build_fill_sql(db: str, schema: str, view: str, cols: list[ColInfo]) -> tuple[str, list[str]]:
    # generate stable aliases
    col_names = [c.name for c in cols]
    pieces = []
    for i, name in enumerate(col_names):
        alias = f"nn__{i:03d}"
        pieces.append(f'COUNT_IF("{name}" IS NOT NULL) AS {alias}')
    counts_expr = ",\n      ".join(pieces)

    sql = f"""
    WITH base AS (
      SELECT * FROM {db}.{schema}.{view}
    )
    SELECT
      c1_entity_type,
      COUNT(*)::NUMBER AS n,
      {counts_expr}
    FROM base
    GROUP BY 1

    UNION ALL

    SELECT
      '__all__' AS c1_entity_type,
      COUNT(*)::NUMBER AS n,
      {counts_expr}
    FROM base
    ;
    """
    aliases = [f"nn__{i:03d}" for i in range(len(col_names))]
    return sql, aliases


def _md_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    """Render simples de DataFrame em Markdown sem dependência de tabulate."""
    if max_rows is not None:
        df = df.head(max_rows).copy()
    if df.empty:
        return "_(vazio)_"
    cols = list(df.columns)
    # stringify
    rows = [[("" if pd.isna(v) else str(v)) for v in r] for r in df.itertuples(index=False, name=None)]
    # widths
    widths = [len(c) for c in cols]
    for r in rows:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(v))
    def fmt_row(r: list[str]) -> str:
        return "| " + " | ".join(r[i].ljust(widths[i]) for i in range(len(cols))) + " |"
    header = fmt_row(cols)
    sep = "| " + " | ".join(("-" * widths[i]) for i in range(len(cols))) + " |"
    body = "\n".join(fmt_row(r) for r in rows)
    return "\n".join([header, sep, body])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="CAPIM_DATA_DEV")
    ap.add_argument("--schema", default="POSSANI_SANDBOX")
    ap.add_argument("--view", default="C1_ENRICHED_BORROWER")
    args = ap.parse_args()

    conn = get_snowflake_connection()
    if conn is None:
        raise SystemExit("Falha ao conectar no Snowflake.")

    cols = _fetch_columns(conn, args.db, args.schema, args.view)
    if not cols:
        raise SystemExit(f"Nenhuma coluna encontrada para {args.db}.{args.schema}.{args.view}.")

    # dictionary
    dict_rows = []
    for c in cols:
        dict_rows.append(
            {
                "column": c.name.lower(),
                "snowflake_type": c.data_type,
                "nullable": c.is_nullable,
                "description": _describe(c.name),
            }
        )
    df_dict = pd.DataFrame(dict_rows)

    # fill rates
    fill_sql, aliases = _build_fill_sql(args.db, args.schema, args.view, cols)
    df_counts = pd.read_sql(fill_sql, conn)

    # reshape to long
    id_cols = ["C1_ENTITY_TYPE", "N"]
    keep_cols = id_cols + [a.upper() for a in aliases]
    df_counts = df_counts[keep_cols]

    # map alias -> column
    alias_to_col = {aliases[i].upper(): cols[i].name.lower() for i in range(len(cols))}
    long_rows = []
    for _, r in df_counts.iterrows():
        entity = r["C1_ENTITY_TYPE"]
        n = int(r["N"])
        for a_upper, col in alias_to_col.items():
            nn = int(r[a_upper])
            long_rows.append(
                {
                    "c1_entity_type": entity,
                    "column": col,
                    "n_rows": n,
                    "n_nonnull": nn,
                    "fill_rate": (nn / n) if n else None,
                }
            )
    df_fill = pd.DataFrame(long_rows).sort_values(["c1_entity_type", "fill_rate", "column"], ascending=[True, True, True])

    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)
    out_csv = out_dir / "c1_enriched_borrower_fill_rates.csv"
    out_md = out_dir / "c1_enriched_borrower_dictionary.md"
    out_docs = Path("docs/reference")
    out_docs.mkdir(parents=True, exist_ok=True)
    out_docs_md = out_docs / "C1_ENRICHED_BORROWER_DATA_DICTIONARY.md"

    df_fill.to_csv(out_csv, index=False)

    # markdown (compact): include dictionary + a small summary per entity
    lines: list[str] = []
    lines.append("# Dicionário — C1_ENRICHED_BORROWER (oficial)")
    lines.append("")
    lines.append(f"- View: `{args.db}.{args.schema}.{args.view}`")
    lines.append(f"- Colunas: **{len(cols)}**")
    lines.append("")
    lines.append("## Dicionário de colunas")
    lines.append("")
    lines.append(_md_table(df_dict))
    lines.append("")
    lines.append("## Fill-rate (resumo)")
    lines.append("")
    for entity in df_fill["c1_entity_type"].unique():
        sub = df_fill[df_fill["c1_entity_type"] == entity]
        # top 15 mais vazias
        worst = sub.nsmallest(15, "fill_rate")[["column", "fill_rate", "n_nonnull", "n_rows"]]
        lines.append(f"### {entity}")
        lines.append("")
        lines.append(_md_table(worst))
        lines.append("")
    out_md.write_text("\n".join(lines), encoding="utf-8")

    # docs/reference: dicionário completo + fill-rate por entidade (wide)
    tz_sp = timezone(timedelta(hours=-3))
    now_sp = datetime.now(tz=tz_sp).strftime("%Y-%m-%dT%H:%M:%S%z")

    # pivot fill to wide
    wide = (
        df_fill.pivot(index="column", columns="c1_entity_type", values="fill_rate")
        .reset_index()
        .rename(columns={"__all__": "fill_rate_all"})
    )
    # attach types/nullable/description
    df_meta = df_dict.copy()
    merged = df_meta.merge(wide, on="column", how="left")
    # stable column order
    for c in ["fill_rate_all", "credit_simulation", "pre_analysis"]:
        if c not in merged.columns:
            merged[c] = None
    merged = merged[
        [
            "column",
            "snowflake_type",
            "nullable",
            "description",
            "fill_rate_all",
            "credit_simulation",
            "pre_analysis",
        ]
    ].sort_values("column")

    doc_lines: list[str] = []
    doc_lines.append("# Dicionário de dados — `C1_ENRICHED_BORROWER`")
    doc_lines.append("")
    doc_lines.append(f"> Gerado automaticamente em: **{now_sp}** (America/Sao_Paulo).")
    doc_lines.append("")
    doc_lines.append("## Fonte")
    doc_lines.append("")
    doc_lines.append(f"- View: `{args.db}.{args.schema}.{args.view}`")
    doc_lines.append(f"- Colunas: **{len(cols)}**")
    doc_lines.append("")
    doc_lines.append("## Campos (descrição + fill-rate)")
    doc_lines.append("")
    doc_lines.append(
        "_`fill_rate_*` = fração de linhas onde a coluna é **não-nula**. Valores são um snapshot do momento de geração._"
    )
    doc_lines.append("")
    doc_lines.append(_md_table(merged))
    doc_lines.append("")

    out_docs_md.write_text("\n".join(doc_lines), encoding="utf-8")
    print(f"Wrote: {out_docs_md}")

    conn.close()
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

