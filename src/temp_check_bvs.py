from src.utils.snowflake_connection import run_query

sql = """
WITH rejected_missing AS (
    SELECT 
        credit_simulation_id, 
        cs_created_at, 
        cpf_effective_digits 
    FROM CAPIM_DATA_DEV.POSSANI_SANDBOX.CREDIT_SIMULATIONS_ENRICHED_BORROWER_V1 
    WHERE simulation_outcome = 'rejected' 
      AND negativacao_source IS NULL
),
overlap AS (
    SELECT 
        m.credit_simulation_id, 
        cc.data
    FROM rejected_missing m 
    JOIN CAPIM_DATA.RESTRICTED.INCREMENTAL_CREDIT_CHECKS_API cc 
      ON REGEXP_REPLACE(cc.cpf, '[^0-9]', '') = m.cpf_effective_digits 
     AND cc.created_at BETWEEN DATEADD('hour', -1, m.cs_created_at) AND DATEADD('hour', 1, m.cs_created_at) 
    WHERE cc.source = 'boa_vista_scpc_net' 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY m.credit_simulation_id ORDER BY cc.created_at DESC) = 1
)
SELECT 
    COUNT(*) as total_missing, 
    COUNT(o.credit_simulation_id) as recover_with_bvs, 
    COUNT(IFF(o.data[2]:"123":exists::string = 'S', 1, NULL)) as bvs_flagged_neg 
FROM rejected_missing m 
LEFT JOIN overlap o ON o.credit_simulation_id = m.credit_simulation_id
"""

df = run_query(sql)
if df is not None:
    print(df.to_string(index=False))
else:
    print("Query returned no data or failed.")

