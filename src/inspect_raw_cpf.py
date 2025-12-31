from src.utils.snowflake_connection import run_query

sql = """
SELECT id, cpf, source, created_at
FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
LIMIT 5
"""

df = run_query(sql)
print(df.to_string(index=False))

