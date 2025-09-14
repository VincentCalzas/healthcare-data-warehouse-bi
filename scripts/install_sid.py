
"""
install_sid.py

Ce script initialise la structure de la base de données Snowflake pour le projet Healthcare Data Warehouse BI.
Il exécute les scripts SQL nécessaires à la création des bases et des schémas.

Étapes principales :
1. Connexion à Snowflake
2. Exécution des scripts SQL de création
3. Journalisation des opérations

Auteur : Vincent
"""

import datetime
import logging
import os
import dotenv
import snowflake.connector

# Créer le dossier logs s'il n'existe pas
os.makedirs("logs", exist_ok=True)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f"logs/install_sid_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        ),
        logging.StreamHandler(),
    ],
)

# Chargement des variables d'environnement depuis .env
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
    """
    Établit une connexion à Snowflake.
    """
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def execute_query(cursor, query):
    """
    Exécute une requête SQL et journalise l'opération.
    """
    try:
        cursor.execute(query)
        logger.info(f"Exécuté : {query.strip()[:100]}")
    except Exception:
        logger.exception(
            f"Erreur lors de l'exécution de la requête : {query.strip()[:100]}",
        )

def process_sql_script(cursor, script_path):
    """
    Lit et exécute toutes les requêtes d'un fichier SQL.
    """
    with open(script_path) as f:
        content = f.read()

    # Découper en requêtes individuelles
    statements = [s.strip() for s in content.split(";") if s.strip()]

    for stmt in statements:
        execute_query(cursor, stmt)

def main():
    """
    Fonction principale : initialise la base et exécute les scripts SQL.
    """
    try:
        conn = connect_snowflake()
        cursor = conn.cursor()
        logger.info("Connexion réussie à Snowflake.")
        files = [
            "create_database.sql",
            "create_SOC.sql",
            "create_STG.sql",
            "create_WRK.sql",
        ]
        for file in files:
            path = os.path.join(SCRIPTS_DIR, file)

            # Exécution du script complet après traitement de la base
            process_sql_script(cursor, path)

        logger.info("Installation terminée sans erreur.")
    except Exception:
        logger.exception("Erreur inattendue pendant l'installation")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

if __name__ == "__main__":
    main()
