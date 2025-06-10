import snowflake.connector
import os
import logging
import re
import dotenv
import csv
import datetime

dotenv.load_dotenv()

# Configuration Snowflake
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "database": "STG",
    "schema": "PUBLIC",
}

def connect_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def execute_query(cursor, query):
    try:
        cursor.execute(query)
        print(f"Exécuté : {query.strip()[:100]}")
    except Exception as e:
        print(
            f"Erreur lors de l'exécution de la requête : {query.strip()[:100]}"
        )

def process_sql_script(cursor, script_path):
    with open(script_path, "r") as f:
        content = f.read()

    # Découper en requêtes individuelles
    statements = [s.strip() for s in content.split(";") if s.strip()]

    for stmt in statements:
        execute_query(cursor, stmt)

def main():
    try:
        conn = connect_snowflake()
        cursor = conn.cursor()
        print("Connexion établie à Snowflake.")
        file = "scripts/drop_databases.sql"
        process_sql_script(cursor, file)
        print("Installation terminée sans erreur.")

    except Exception as e:
        print("Erreur inattendue pendant le chargement")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()
