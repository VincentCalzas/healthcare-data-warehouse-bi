import snowflake.connector
import os
import dotenv

# Charger les variables d'environnement
dotenv.load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "database": "WRK",
    "schema": "PUBLIC",
}

SCRIPT_PATH = os.path.join("scripts", "insert__STG__to__WRK_STG.sql")

def main():
    with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
        with conn.cursor() as cur:
            with open(SCRIPT_PATH, "r", encoding="utf-8") as f:
                sql = f.read()
                for statement in [s.strip() for s in sql.split(';') if s.strip()]:
                    cur.execute(statement)
                    print(f"Exécuté : {statement[:80]}...")
    print("Script exécuté avec succès.")

if __name__ == "__main__":
    main()
