import argparse
import re
import time

from src.utils.snowflake_connection import get_snowflake_connection


def read_enrichment_sql() -> str:
    sql = open(
        "queries/enrich/enrich_credit_simulations_borrower.sql", "r", encoding="utf-8"
    ).read()
    return re.sub(r";\s*$", "", sql.strip())


def make_sampled_sql(sql: str, sample_rows: int) -> str:
    """
    Faz amostragem no cs_base via TABLESAMPLE/SAMPLE, para reduzir custo do CTAS.
    Estratégia: trocar o FROM do CREDIT_SIMULATIONS por '... SAMPLE (N ROWS)'.
    """
    needle = "FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs"
    replacement = f"FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS cs SAMPLE ({int(sample_rows)} ROWS)"
    if needle not in sql:
        raise ValueError("Não encontrei o FROM do CREDIT_SIMULATIONS em cs_base para aplicar SAMPLE.")
    return sql.replace(needle, replacement, 1)


def exec_and_time(cur, query: str) -> float:
    t0 = time.time()
    cur.execute(query)
    # force fetch for completion (some drivers may lazily stream)
    if cur.description is not None:
        _ = cur.fetchall()
    return time.time() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--schema",
        default="CAPIM_DATA_DEV.POSSANI_SANDBOX",
        help="Schema destino (db.schema), ex: CAPIM_DATA_DEV.POSSANI_SANDBOX",
    )
    ap.add_argument(
        "--table",
        default="CREDIT_SIMULATIONS_ENRICHED_BORROWER",
        help="Nome da tabela final (sem schema).",
    )
    ap.add_argument(
        "--sample-rows",
        type=int,
        default=20000,
        help="Tamanho da amostra para CTAS de benchmark (via SAMPLE (N ROWS)).",
    )
    ap.add_argument(
        "--only-sample",
        action="store_true",
        help="Se setado, apenas roda o CTAS de amostra e estimativa (não materializa full).",
    )
    ap.add_argument(
        "--keep-sample",
        action="store_true",
        help="Se setado, mantém a tabela _SAMPLE_ criada para benchmark. Por padrão, removemos o sample no final para evitar artefatos.",
    )
    args = ap.parse_args()

    schema = args.schema
    final_table = f"{schema}.{args.table}"
    sample_table = f"{schema}.{args.table}_SAMPLE_{args.sample_rows}"
    legacy_v1_table = f"{final_table}_V1" if not args.table.endswith("_V1") else None

    sql = read_enrichment_sql()
    sql_sample = make_sampled_sql(sql, args.sample_rows)

    conn = get_snowflake_connection()
    if conn is None:
        raise SystemExit("Falha ao conectar no Snowflake.")
    cur = conn.cursor()

    print("Medindo universo total de credit_simulations...")
    cur.execute("SELECT COUNT(*) FROM CAPIM_DATA.CAPIM_PRODUCTION.CREDIT_SIMULATIONS")
    (n_total,) = cur.fetchone()
    print("TOTAL credit_simulations =", int(n_total))

    print("\nCTAS amostral (benchmark):", sample_table)
    t_sample = exec_and_time(cur, f"CREATE OR REPLACE TABLE {sample_table} AS {sql_sample}")
    cur.execute(f"SELECT COUNT(*) FROM {sample_table}")
    (n_sample_out,) = cur.fetchone()
    n_sample_out = int(n_sample_out)
    print("Linhas amostra materializadas =", n_sample_out)
    print("Tempo amostra (s) =", round(t_sample, 2))

    if n_sample_out > 0:
        est_seconds = t_sample * (int(n_total) / n_sample_out)
        print("Estimativa linear full (min) ~", round(est_seconds / 60, 1))
        print("Observação: estimativa é aproximada; custo pode não escalar linearmente.")

    if args.only_sample:
        print("\n--only-sample: não materializando tabela full.")
        conn.close()
        return

    print("\nCTAS FULL:", final_table)
    print("DROP (limpeza) tabela destino se existir:", final_table)
    cur.execute(f"DROP TABLE IF EXISTS {final_table}")
    if legacy_v1_table is not None:
        print("DROP (limpeza) tabela legado _V1 se existir:", legacy_v1_table)
        cur.execute(f"DROP TABLE IF EXISTS {legacy_v1_table}")
    t_full = exec_and_time(cur, f"CREATE OR REPLACE TABLE {final_table} AS {sql}")
    cur.execute(f"SELECT COUNT(*) FROM {final_table}")
    (n_full,) = cur.fetchone()
    print("Linhas full materializadas =", int(n_full))
    print("Tempo full (min) =", round(t_full / 60, 2))

    if not args.keep_sample:
        print("\nRemovendo tabela sample (limpeza):", sample_table)
        cur.execute(f"DROP TABLE IF EXISTS {sample_table}")

    conn.close()


if __name__ == "__main__":
    main()


