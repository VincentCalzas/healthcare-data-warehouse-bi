import snowflake.connector
import os
import logging
import re
import dotenv

logger = logging.getLogger(__name__)
logger.setLevel('INFO')
logging.basicConfig(    
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

# Load environment variables from .env file
dotenv.load_dotenv()

# Configuration Snowflake à partir de .env
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "database": "",  # défini dynamiquement
    "schema": "",  # défini dynamiquement
}

SCRIPTS_DIR = "scripts"


def connect_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def execute_query(cursor, query):
    try:
        cursor.execute(query)
        logger.info(f"Exécuté : {query.strip()[:100]}")
    except Exception as e:
        logger.exception(
            f"Erreur lors de l'exécution de la requête : {query.strip()[:100]}"
        )


def database_exists(cursor, db_name):
    cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
    return cursor.fetchone() is not None


def table_exists(cursor, db, schema, table):
    cursor.execute(f"SHOW TABLES IN {db}.{schema} LIKE '{table}'")
    return cursor.fetchone() is not None


def process_sql_script(cursor, script_path):
    with open(script_path, "r") as f:
        content = f.read()

    # Découper en requêtes individuelles
    statements = [s.strip() for s in content.split(";") if s.strip()]

    for stmt in statements:
        stmt_upper = stmt.upper()
        match = re.search(
            r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
            stmt_upper,
        )
        if match:
            db, schema, table = match.group(2), match.group(3), match.group(4)

            if schema in ["STG", "WRK"]:
                execute_query(cursor, f"DROP TABLE IF EXISTS {db}.{schema}.{table}")
                execute_query(cursor, stmt)
            elif schema in ["SOC", "TCH"]:
                if not table_exists(cursor, db, schema, table):
                    execute_query(cursor, stmt)
                else:
                    logger.info(
                        f"Table {db}.{schema}.{table} existe déjà, non recréée."
                    )
            else:
                logger.warning(f"Schéma non reconnu pour la table : {stmt}")
        else:
            execute_query(cursor, stmt)


def main():
    try:
        conn = connect_snowflake()
        cursor = conn.cursor()
        logger.info("Connexion réussie à Snowflake.")

        for file in os.listdir(SCRIPTS_DIR):
            if file.endswith(".sql"):
                path = os.path.join(SCRIPTS_DIR, file)
                logger.info(f"Traitement du script : {file}")
                with open(path, "r") as f:
                    first_line = f.readline().strip()
                    match = re.search(
                        r"CREATE\s+DATABASE\s+([a-zA-Z0-9_]+)", first_line.upper()
                    )
                    if match:
                        db_name = match.group(1)
                        if not database_exists(cursor, db_name):
                            logger.info(f"Création de la base {db_name}")
                            execute_query(cursor, f"CREATE DATABASE {db_name}")
                        else:
                            logger.info(f"Base {db_name} déjà existante, non recréée.")

                # Exécution du script complet après traitement de la base
                process_sql_script(cursor, path)

        logger.info("Installation terminée sans erreur.")
    except Exception as e:
        logger.exception("Erreur inattendue pendant l'installation")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
