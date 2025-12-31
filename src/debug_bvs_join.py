from src.utils.snowflake_connection import run_query

sql = """
WITH targets AS (
    SELECT credit_simulation_id, cpf_effective_digits, cs_created_at
    FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1
    WHERE negativacao_source = 'boa_vista_scpc_net'
    LIMIT 5
)
SELECT * FROM targets
"""

print("--- Targets ---")
df = run_query(sql)
print(df.to_string(index=False))

if len(df) > 0:
    first_cpf = df.iloc[0]['CPF_EFFECTIVE_DIGITS']
    print(f"\n--- Checks for CPF {first_cpf} ---")
    sql2 = f"""
    SELECT id, source, kind, created_at, cpf
    FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
    WHERE REGEXP_REPLACE(cpf, '\\D','') = '{first_cpf}'
    ORDER BY created_at DESC
    LIMIT 10
    """
    df2 = run_query(sql2)
    print(df2.to_string(index=False))

