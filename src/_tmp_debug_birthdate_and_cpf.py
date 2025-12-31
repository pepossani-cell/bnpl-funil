from __future__ import annotations

from src.utils.snowflake_connection import run_query


def main() -> None:
    # 1) Birthdate sanity distribution
    sql_birth = """
    SELECT
      COUNT(*) AS n,
      COUNT_IF(borrower_birthdate IS NOT NULL) AS n_birth,
      COUNT_IF(borrower_birthdate >= '2020-01-01'::DATE) AS n_birth_ge_2020,
      COUNT_IF(borrower_birthdate >= '2025-01-01'::DATE) AS n_birth_ge_2025,
      MIN(borrower_birthdate) AS min_birthdate,
      MAX(borrower_birthdate) AS max_birthdate
    FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1
    """
    df_birth = run_query(sql_birth)
    print("--- Birthdate range/sanity ---")
    print(df_birth.to_string(index=False) if df_birth is not None else "query failed")

    # 2) Inspect one CPF
    cpf = "34460602865"
    sql_cpf = f"""
    SELECT
      credit_simulation_id,
      cs_created_at,
      credit_simulation_state,
      credit_simulation_rejection_reason,
      total_credit_checks_count,
      boa_vista_score,
      serasa_score,
      serasa_score_source,
      bacen_internal_score,
      crivo_check_id_resolved,
      crivo_resolution_stage,
      borrower_birthdate,
      borrower_birthdate_source
    FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1
    WHERE cpf_effective_digits = '{cpf}'
    ORDER BY cs_created_at DESC
    LIMIT 50
    """
    df_cpf = run_query(sql_cpf)
    print(f"\n--- Enriched rows for cpf_effective_digits={cpf} ---")
    print(df_cpf.to_string(index=False) if df_cpf is not None else "query failed")

    # 3) Pull raw credit checks for that CPF (last 180 days) for debugging score paths
    sql_cc = f"""
    SELECT
      id,
      source,
      kind,
      COALESCE(new_data_format, FALSE) AS new_data_format,
      created_at,
      -- common score paths
      TRY_TO_NUMBER(data:score::string) AS score_top_level,
      TRY_TO_NUMBER(data:data:score::string) AS score_data_block,
      TRY_TO_NUMBER(data:score_positivo:score_classificacao_varios_modelos:score::string) AS bvs_score_path,
      -- a bit of serasa report score if present
      TRY_TO_NUMBER(r.value:score::string) AS serasa_report_score
    FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc
    WHERE REGEXP_REPLACE(cc.cpf, '\\\\D','') = '{cpf}'
      AND cc.created_at >= DATEADD('day', -180, CURRENT_TIMESTAMP())
    , LATERAL FLATTEN(input => cc.data:reports, OUTER => TRUE) r
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cc.id ORDER BY r.index ASC NULLS LAST) = 1
    ORDER BY created_at DESC
    LIMIT 50
    """
    df_cc = run_query(sql_cc)
    print(f"\n--- Raw credit checks (last 30d) for cpf={cpf} ---")
    print(df_cc.to_string(index=False) if df_cc is not None else "query failed")

    # 4) Birthdates impossible: breakdown by source/role
    sql_bad_birth = """
    SELECT
      borrower_role,
      COALESCE(borrower_birthdate_source, 'NULL') AS borrower_birthdate_source,
      COUNT(*) AS n
    FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1
    WHERE borrower_birthdate >= '2025-01-01'::DATE
    GROUP BY 1,2
    ORDER BY n DESC
    """
    df_bad = run_query(sql_bad_birth)
    print("\n--- Birthdates >= 2025-01-01 by role/source ---")
    print(df_bad.to_string(index=False) if df_bad is not None else "query failed")


if __name__ == "__main__":
    main()


