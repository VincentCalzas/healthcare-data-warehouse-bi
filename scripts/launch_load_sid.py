import argparse
import csv
import datetime
import logging
import os
import re
"""
launch_load_sid.py

Ce script charge les données hospitalières pour une date donnée dans Snowflake.
Il traite les fichiers du dossier data_hospital, appelle les procédures d'insertion et exécute les scripts de transformation.

Étapes principales :
1. Connexion à Snowflake
2. Chargement des fichiers pour la date
3. Appel des procédures d'insertion
4. Exécution des scripts de transformation
5. Journalisation des opérations

Auteur : Vincent
"""

from pathlib import Path

import dotenv
import snowflake.connector

import dotenv
import snowflake.connector

# Créer le dossier logs s'il n'existe pas
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

SCRIPTS_DIR = Path("scripts")
if not SCRIPTS_DIR.exists():
    raise FileNotFoundError(
        f"Le dossier {SCRIPTS_DIR} n'existe pas. Veuillez vérifier le chemin.",
    )

# Chemin local racine
LOCAL_DATA_ROOT = Path("Data Hospital")
if not LOCAL_DATA_ROOT.exists():
    raise FileNotFoundError(
        f"Le dossier {LOCAL_DATA_ROOT} n'existe pas. Veuillez vérifier le chemin.",
    )

FOLDER_PATH_PREFIX = "BDD_HOSPITAL_"
FILE_PATTERN = re.compile(r"([A-Z]+)_?(\d{8})$")

# Initialisation du logger
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            (
                LOGS_DIR
                / f"launch_load_sid_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            ).as_posix(),
        ),
        logging.StreamHandler(),
    ],
)

# Chargement des variables d'environnement
dotenv.load_dotenv()

# Configuration Snowflake
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "database": "",
    "schema": "",
}

EXPECTED_ENTITIES = {
    "CHAMBRE",
    "CONSULTATION",
    "HOSPITALISATION",
    "MEDICAMENT",
    "PATIENT",
    "PERSONNEL",
    "TRAITEMENT",
}

ENTITY_COLUMN_COUNTS = {
    "CHAMBRE": 7,
    "CONSULTATION": 13,
    "HOSPITALISATION": 7,
    "MEDICAMENT": 5,
    "PATIENT": 17,
    "PERSONNEL": 10,
    "TRAITEMENT": 8,
}


def connect_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def create_procedures(cursor):
    logger.info("Création des procédures...")
    sql_script = SCRIPTS_DIR / "insert_STG_procedures.sql"
    if not sql_script.exists():
        logger.error(f"Le fichier {sql_script} n'existe pas.")
        raise FileNotFoundError(
            f"Le fichier {sql_script} n'existe pas. Veuillez vérifier le chemin.",
        )

    with open(sql_script, encoding="utf-8") as f:
        sql_script = f.read()

    # On découpe chaque procédure avec END; suivie de $$ (fin de bloc)
    procedure_blocks = re.findall(
        r"(CREATE OR REPLACE PROCEDURE.*?END;\s*\$\$)",
        sql_script,
        flags=re.DOTALL | re.IGNORECASE,
    )

    for i, proc_sql in enumerate(procedure_blocks, start=1):
        try:
            cursor.execute("USE DATABASE STG;")
            cursor.execute("USE SCHEMA PUBLIC;")
            cursor.execute(proc_sql)
            logger.info(f"Procédure {i} créée avec succès.")
        except Exception as e:
            logger.warning(f"Erreur création procédure {i}: {e}")


def call_insert_procedure(cursor, entity_name, all_rows):
    proc_name = f"insert_{entity_name.lower()}"

    # Transposer les lignes en colonnes (zip)
    columns = list(zip(*all_rows, strict=False))

    param_list = []
    for col in columns:
        formatted_col = []
        for val in col:
            if val is None or val == "" or val == "NULL":
                formatted_col.append("NULL")
            else:
                val = str(val).replace("'", "''")  # Échappement des quotes
                formatted_col.append(f"'{val}'")
        array_str = f"ARRAY_CONSTRUCT({', '.join(formatted_col)})"
        param_list.append(array_str)

    call_sql = f"CALL {proc_name}({', '.join(param_list)})"
    cursor.execute("USE DATABASE STG;")
    cursor.execute("USE SCHEMA PUBLIC;")
    logger.debug(f"Appel SQL: {call_sql}")
    cursor.execute(call_sql)


def process_local_file(cursor, file_path, entity_name, date_str):
    logger.info(
        f"Traitement local de {entity_name} pour la date {date_str} - fichier: {file_path}",
    )

    expected_cols = ENTITY_COLUMN_COUNTS.get(entity_name)
    if not expected_cols:
        logger.error(f"Nombre de colonnes inconnu pour l'entité {entity_name}")
        return

    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")

        all_rows = []
        line_num = 0

        for row in reader:
            line_num += 1
            row = row[1:]  # Retirer identifiant ou marqueur de début

            if line_num == 1:
                continue  # Saut du header si nécessaire

            if len(row) != expected_cols:
                logger.warning(
                    f"Ligne {line_num} ignorée (colonnes {len(row)} != {expected_cols}) dans {file_path}",
                )
                continue

            all_rows.append(row)

        if not all_rows:
            logger.warning(f"Aucune donnée valide dans {file_path}")
            return

        try:
            call_insert_procedure(cursor, entity_name, all_rows)
            logger.info(
                f"{len(all_rows)} lignes insérées pour {entity_name}_{date_str}",
            )
        except Exception as e:
            logger.exception(f"Erreur d'insertion pour {entity_name}_{date_str} : {e}")


def bascule_STG_WRK(cursor):
    logger.info("Exécution du script STG vers WRK...")
    sql_script = SCRIPTS_DIR / "insert__STG__to__WRK_STG.sql"
    if not sql_script.exists():
        logger.error(f"Le fichier {sql_script} n'existe pas.")
        raise FileNotFoundError(
            f"Le fichier {sql_script} n'existe pas. Veuillez vérifier le chemin.",
        )
    try:
        with open(sql_script, encoding="utf-8") as f:
            sql_script = f.read()

        sql_statements = sql_script.split(";")
        for statement in sql_statements:
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute("USE DATABASE WRK;")
                    cursor.execute("USE SCHEMA PUBLIC;")
                    cursor.execute(statement)
                    logger.info(f"Statement exécuté avec succès: {statement[:100]}...")
                except Exception as e:
                    logger.error(
                        f"Erreur lors de l'exécution de la requête: {statement[:100]}... Erreur: {e}",
                    )
                    raise e
        logger.info("Script STG vers WRK exécuté avec succès.")

    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du script STG vers WRK: {e}")
        raise e


def traitement_WRK(cursor):
    logger.info("Exécution du script WRK STG vers WRK SOC...")
    script_path = SCRIPTS_DIR / "insert__WRK_STG__to__WRK_SOC.sql"
    if not script_path.exists():
        logger.error(f"Le fichier {script_path} n'existe pas.")
        raise FileNotFoundError(
            f"Le fichier {script_path} n'existe pas. Veuillez vérifier le chemin.",
        )
    try:
        with open(script_path, encoding="utf-8") as f:
            sql_script = f.read()

        sql_statements = sql_script.split(";")
        for statement in sql_statements:
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute("USE DATABASE WRK;")
                    cursor.execute("USE SCHEMA PUBLIC;")
                    cursor.execute(statement)
                    logger.info(f"Statement exécuté avec succès: {statement[:100]}...")
                except Exception as e:
                    logger.error(
                        f"Erreur lors de l'exécution de la requête: {statement[:100]}... Erreur: {e}",
                    )
                    raise e
        logger.info("Script WRK STG vers WRK SOC exécuté avec succès.")

    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du script WRK STG vers WRK SOC: {e}")
        raise e


def bascule_WRK_SOC(cursor):
    logger.info("Exécution du script WRK_SOC vers SOC...")
    sql_script = SCRIPTS_DIR / "insert__WRK_SOC__to__SOC.sql"
    if not sql_script.exists():
        logger.error(f"Le fichier {sql_script} n'existe pas.")
        raise FileNotFoundError(
            f"Le fichier {sql_script} n'existe pas. Veuillez vérifier le chemin.",
        )
    try:
        with open(sql_script, encoding="utf-8") as f:
            sql_script = f.read()

        sql_statements = sql_script.split(";")
        for statement in sql_statements:
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute("USE DATABASE SOC;")
                    cursor.execute("USE SCHEMA PUBLIC;")
                    cursor.execute(statement)
                    logger.info(f"Statement exécuté avec succès: {statement[:100]}...")
                except Exception as e:
                    logger.error(
                        f"Erreur lors de l'exécution de la requête: {statement[:100]}... Erreur: {e}",
                    )
                    raise e
        logger.info("Script WRK_SOC vers SOC exécuté avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du script WRK_SOC vers SOC: {e}")
        raise e


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date", type=str, help="Date spécifique au format YYYYMMDD", required=True,
    )
    args = parser.parse_args()

    folder_date = LOCAL_DATA_ROOT / (FOLDER_PATH_PREFIX + args.date)
    if not folder_date.exists():
        logger.error(
            f"Le dossier {folder_date} n'existe pas. Veuillez vérifier le chemin.",
        )
        raise FileNotFoundError(
            f"Le dossier {folder_date} n'existe pas. Veuillez vérifier le chemin.",
        )

    try:
        conn = connect_snowflake()
        cursor = conn.cursor()
        logger.info("Connexion établie à Snowflake.")

        create_procedures(cursor)

        # On parcourt les fichiers txt dans le dossier contenant les données du jour dans DATA_HOSPITAL localement avec pathlib
        for file_path in folder_date.glob("*.txt"):
            match = FILE_PATTERN.search(file_path.stem)
            if not match:
                logger.warning(f"Ignoré (fichier non reconnu) : {file_path}")
                continue

            entity, date_str = match.group(1), match.group(2)
            if date_str != args.date:
                logger.warning(f"Ignoré (date non correspondante) : {file_path}")
                continue

            if entity not in EXPECTED_ENTITIES:
                logger.warning(f"Fichier inattendu : {file_path}")
                continue

            try:
                process_local_file(cursor, file_path, entity, date_str)
            except Exception as e:
                logger.exception(f"Erreur sur fichier {entity}_{date_str} : {e}")

        conn.commit()  # commit global des inserts

    except Exception:
        logger.exception("Erreur inattendue pendant le chargement")
    finally:
        try:
            bascule_STG_WRK(cursor)
            traitement_WRK(cursor)
            bascule_WRK_SOC(cursor)
        except Exception as e:
            logger.exception(f"Erreur lors dans le traitement de données : {e}")
        try:
            cursor.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
