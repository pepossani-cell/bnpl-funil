from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.utils.snowflake_connection import run_query  # noqa: E402


def q(title: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    df = run_query(sql)
    if df is None:
        print("falhou")
        return
    print(df.to_string(index=False))


def main() -> None:
    # 1) SERASA check_income_only: top-level keys (amostra recente)
    q(
        "SERASA check_income_only (últimos 180d) - TYPEOF e top-level keys (amostra)",
        """
        WITH s AS (
          SELECT id, created_at, new_data_format, data
          FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
          WHERE source='serasa'
            AND kind='check_income_only'
            AND created_at >= DATEADD('day', -180, CURRENT_TIMESTAMP())
            AND (COALESCE(new_data_format,FALSE)=TRUE OR (TYPEOF(data)='OBJECT' AND data:reports IS NOT NULL))
          QUALIFY ROW_NUMBER() OVER (ORDER BY created_at DESC) <= 50
        )
        SELECT
          id,
          created_at,
          COALESCE(new_data_format,FALSE) AS new_data_format,
          TYPEOF(data) AS typeof_data,
          CASE
            WHEN TYPEOF(data)='OBJECT' THEN ARRAY_TO_STRING(OBJECT_KEYS(data), ',')
            WHEN TYPEOF(data)='ARRAY' AND TYPEOF(data[0])='OBJECT' THEN ARRAY_TO_STRING(OBJECT_KEYS(data[0]), ',')
            ELSE CONCAT('TYPEOF=', TYPEOF(data))
          END AS top_keys
        FROM s
        ORDER BY created_at DESC
        """,
    )

    # 2) SERASA check_income_only: procura por chaves contendo income/renda/salary (flatten recursivo)
    q(
        "SERASA check_income_only - paths com key LIKE '%income%'/'%renda%'/'%salary%' (amostra)",
        """
        WITH s AS (
          SELECT id, created_at, data
          FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
          WHERE source='serasa'
            AND kind='check_income_only'
            AND created_at >= DATEADD('day', -365, CURRENT_TIMESTAMP())
            AND (COALESCE(new_data_format,FALSE)=TRUE OR (TYPEOF(data)='OBJECT' AND data:reports IS NOT NULL))
          QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= 200
        ),
        flat AS (
          SELECT
            s.id,
            f.path AS json_path,
            LOWER(f.key::string) AS k,
            f.value::string AS v
          FROM s,
          LATERAL FLATTEN(input => s.data, recursive => TRUE) f
          WHERE f.key IS NOT NULL
        )
        SELECT
          json_path,
          k,
          COUNT(*) AS n
        FROM flat
        WHERE k LIKE '%income%'
           OR k LIKE '%renda%'
           OR k LIKE '%salary%'
        GROUP BY 1,2
        ORDER BY 3 DESC
        LIMIT 50
        """,
    )

    # 3) BACEN internal score: keys dentro de predictions[0] (amostra)
    q(
        "BACEN internal score - keys em predictions[0] (amostra)",
        """
        WITH s AS (
          SELECT id, created_at, data
          FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
          WHERE source='bacen_internal_score'
            AND created_at >= DATEADD('day', -365, CURRENT_TIMESTAMP())
          QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= 500
        ),
        p0 AS (
          SELECT data:predictions[0] AS p
          FROM s
          WHERE data:predictions[0] IS NOT NULL
        )
        SELECT
          k.value::string AS key_name,
          COUNT(*) AS n
        FROM p0,
        LATERAL FLATTEN(input => OBJECT_KEYS(p0.p)) k
        GROUP BY 1
        ORDER BY 2 DESC
        """,
    )

    # 4) SERASA new check_score_without_income: procura por income-like (pode vir em facts)
    q(
        "SERASA check_score_without_income - paths income/renda/salary (amostra)",
        """
        WITH s AS (
          SELECT id, created_at, data
          FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
          WHERE source='serasa'
            AND kind='check_score_without_income'
            AND created_at >= DATEADD('day', -365, CURRENT_TIMESTAMP())
            AND (COALESCE(new_data_format,FALSE)=TRUE OR (TYPEOF(data)='OBJECT' AND data:reports IS NOT NULL))
          QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= 200
        ),
        flat AS (
          SELECT
            f.path AS json_path,
            LOWER(f.key::string) AS k
          FROM s,
          LATERAL FLATTEN(input => s.data, recursive => TRUE) f
          WHERE f.key IS NOT NULL
        )
        SELECT json_path, k, COUNT(*) AS n
        FROM flat
        WHERE k LIKE '%income%'
           OR k LIKE '%renda%'
           OR k LIKE '%salary%'
        GROUP BY 1,2
        ORDER BY 3 DESC
        LIMIT 50
        """,
    )


if __name__ == "__main__":
    main()


