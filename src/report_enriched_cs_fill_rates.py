import pathlib

import pandas as pd

from src.utils.snowflake_connection import run_query


def _quote_ident(ident: str) -> str:
    # Snowflake identifier quoting (double quotes); escape embedded quotes.
    return f'"{ident.replace(chr(34), chr(34) + chr(34))}"'


def main() -> None:
    database = "CAPIM_DATA_DEV"
    schema = "POSSANI_SANDBOX"
    table = "CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1"
    fqtn = f"{database}.{schema}.{table}"

    out_dir = pathlib.Path("outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "credit_simulations_enriched_borrower_v1_fill_rates.csv"
    out_md = out_dir / "credit_simulations_enriched_borrower_v1_fill_rates.md"

    cols_df = run_query(
        f"""
        SELECT
          column_name,
          ordinal_position,
          data_type
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
        ORDER BY ordinal_position
        """
    )
    if cols_df is None or cols_df.empty:
        raise RuntimeError(f"Não consegui listar colunas de {fqtn}.")

    n_total_df = run_query(f"SELECT COUNT(*)::NUMBER AS n_total FROM {fqtn}")
    if n_total_df is None or n_total_df.empty:
        raise RuntimeError(f"Não consegui contar linhas de {fqtn}.")
    n_total = int(n_total_df.loc[0, "N_TOTAL"])

    union_sql_parts: list[str] = []
    for _, r in cols_df.iterrows():
        col = str(r["COLUMN_NAME"])
        pos = int(r["ORDINAL_POSITION"])
        dtype = str(r["DATA_TYPE"])
        col_q = _quote_ident(col)
        union_sql_parts.append(
            f"""
            SELECT
              '{col}' AS column_name,
              {pos} AS ordinal_position,
              '{dtype}' AS data_type,
              COUNT_IF({col_q} IS NOT NULL)::NUMBER AS n_filled
            FROM {fqtn}
            """
        )

    fill_df = run_query("\nUNION ALL\n".join(union_sql_parts))
    if fill_df is None or fill_df.empty:
        raise RuntimeError("Falha ao calcular preenchimento por coluna.")

    fill_df["N_TOTAL"] = n_total
    fill_df["FILL_RATE"] = (fill_df["N_FILLED"] / fill_df["N_TOTAL"]).astype(float)
    fill_df = fill_df.sort_values("ORDINAL_POSITION")

    # CSV completo
    fill_df.to_csv(out_csv, index=False)

    # Markdown resumido (top/bottom) + tabela completa em CSV
    top10 = fill_df.sort_values("FILL_RATE", ascending=False).head(12)[
        ["COLUMN_NAME", "DATA_TYPE", "N_FILLED", "N_TOTAL", "FILL_RATE"]
    ]
    bot10 = fill_df.sort_values("FILL_RATE", ascending=True).head(12)[
        ["COLUMN_NAME", "DATA_TYPE", "N_FILLED", "N_TOTAL", "FILL_RATE"]
    ]

    def _fmt_md_table(df: pd.DataFrame) -> str:
        d = df.copy()
        d["FILL_RATE_%"] = (d["FILL_RATE"] * 100).round(2)
        d = d.drop(columns=["FILL_RATE"])

        headers = list(d.columns)
        rows = d.astype(str).values.tolist()

        # Simple GitHub-flavored markdown table (no external deps)
        lines = []
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        for row in rows:
            lines.append("| " + " | ".join(row) + " |")
        return "\n".join(lines)

    out_md.write_text(
        "\n".join(
            [
                f"# Fill-rate por coluna — {fqtn}",
                "",
                f"- n_total: {n_total}",
                f"- CSV completo: `{out_csv.as_posix()}`",
                "",
                "## Top preenchimento (amostra)",
                "",
                _fmt_md_table(top10),
                "",
                "## Menor preenchimento (amostra)",
                "",
                _fmt_md_table(bot10),
                "",
            ]
        ),
        encoding="utf-8",
    )

    print(f"[ok] Escrevi: {out_csv}")
    print(f"[ok] Escrevi: {out_md}")


if __name__ == "__main__":
    main()


