from src.utils.snowflake_connection import run_query

cpf_target = '07252175456'

sql = f"""
SELECT id, cpf, source
FROM CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API
WHERE cpf LIKE '%{cpf_target}%'
   OR REGEXP_REPLACE(cpf, '\\D','') = '{cpf_target}'
LIMIT 5
"""

print(f"Caçando {cpf_target}...")
df = run_query(sql)
if df is not None:
    print(df.to_string(index=False))
else:
    print("Nada encontrado.")

