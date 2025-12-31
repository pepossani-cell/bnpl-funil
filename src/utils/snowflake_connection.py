import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

def get_snowflake_connection():
    """
    Estabelece uma conexão com o Snowflake usando credenciais do arquivo .env.
    Retorna o objeto de conexão, ou None se falhar.
    """
    # Credenciais obrigatórias
    required_credentials = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    }

    # Credenciais opcionais (usará o padrão do usuário/role se não definidas)
    optional_credentials = {
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }

    # Verifica se alguma credencial obrigatória está faltando
    missing_creds = [key for key, value in required_credentials.items() if value is None]
    if missing_creds:
        print(f"Erro: As seguintes variáveis de ambiente OBRIGATÓRIAS não foram encontradas no arquivo .env: {', '.join(missing_creds)}")
        print("Certifique-se de ter criado o arquivo '.env' e preenchido os valores de user, password e account.")
        return None

    try:
        # Monta os argumentos de conexão
        connect_args = {
            **required_credentials,
            **{k: v for k, v in optional_credentials.items() if v is not None}
        }
        
        conn = snowflake.connector.connect(**connect_args)
        print("Conexão com Snowflake estabelecida com sucesso!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar no Snowflake: {e}")
        return None

def run_query(query):
    """
    Executa uma query SQL no Snowflake e retorna os resultados como um DataFrame do Pandas.
    """
    conn = get_snowflake_connection()
    if conn:
        try:
            # Usando fetch_pandas_all() para eficiência com grandes volumes de dados
            # Requer instalação de 'pyarrow' (incluído no requirements.txt)
            cur = conn.cursor()
            cur.execute(query)
            # Para DDL/DML (sem result set), cur.description é None
            if cur.description is None:
                return pd.DataFrame()
            return cur.fetch_pandas_all()
        except Exception as e:
            print(f"Erro ao executar query: {e}")
            return None
        finally:
            conn.close()
    return None

if __name__ == "__main__":
    # Teste simples de conexão
    print("Testando conexão...")
    test_df = run_query("SELECT CURRENT_VERSION()")
    if test_df is not None:
        print("Versão do Snowflake:", test_df.iloc[0,0])
