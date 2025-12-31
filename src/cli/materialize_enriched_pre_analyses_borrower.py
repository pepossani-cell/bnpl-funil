import argparse
import re
import time

from src.utils.snowflake_connection import get_snowflake_connection


def read_sql() -> str:
    sql = open("queries/enrich/enrich_pre_analyses_borrower.sql", "r", encoding="utf-8").read()
    return re.sub(r";\s*$", "", sql.strip())


def exec_and_time(cur, query: str) -> float:
    t0 = time.time()
    cur.execute(query)
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
        default="PRE_ANALYSES_ENRICHED_BORROWER",
        help="Nome da tabela final (sem schema).",
    )
    args = ap.parse_args()

    schema = args.schema
    final_table = f"{schema}.{args.table}"
    legacy_v1_table = f"{final_table}_V1" if not args.table.endswith("_V1") else None

    sql = read_sql()

    conn = get_snowflake_connection()
    if conn is None:
        raise SystemExit("Falha ao conectar no Snowflake.")
    cur = conn.cursor()

    print("\nCTAS FULL:", final_table)
    print("DROP (limpeza) tabela destino se existir:", final_table)
    cur.execute(f"DROP TABLE IF EXISTS {final_table}")
    if legacy_v1_table is not None:
        print("DROP (limpeza) tabela legado _V1 se existir:", legacy_v1_table)
        cur.execute(f"DROP TABLE IF EXISTS {legacy_v1_table}")
    t_full = exec_and_time(cur, f"CREATE OR REPLACE TABLE {final_table} AS {sql}")
    cur.execute(f"SELECT COUNT(*) FROM {final_table}")
    (n_full,) = cur.fetchone()
    print("Linhas materializadas =", int(n_full))
    print("Tempo (min) =", round(t_full / 60, 2))

    conn.close()


if __name__ == "__main__":
    main()


