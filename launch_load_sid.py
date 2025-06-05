import snowflake.connector
import os
import logging
import re
import dotenv
import csv

# Initialisation du logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

# Chargement des variables d'environnement
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

# Chemin local racine où sont stockés tes fichiers (à adapter)
LOCAL_DATA_ROOT = "./Data Hospital"

FILE_PATTERN = re.compile(r'BDD_HOSPITAL_(\d{8})/([A-Z_]+)_\d{8}\.txt$')

EXPECTED_ENTITIES = {
    "CHAMBRE",
    "CONSULTATION",
    "HOSPITALISATION",
    "MEDICAMENT",
    "PATIENT",
    "PERSONNEL",
    "TRAITEMENT"
}

ENTITY_COLUMN_COUNTS = {
    "CHAMBRE": 7,
    "CONSULTATION": 13,
    "HOSPITALISATION": 7,
    "MEDICAMENT": 5,
    "PATIENT": 17,
    "PERSONNEL": 10,
    "TRAITEMENT": 8
}


def connect_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def call_insert_procedure(cursor, entity_name, row):
    proc_name = f"insert_{entity_name}"

    param_list = []
    for val in row:
        if val is None or val == '':
            param_list.append("NULL")
        elif isinstance(val, str):
            escaped_val = val.replace("'", "''")
            param_list.append(f"'{escaped_val}'")
        elif isinstance(val, (float, int)):
            param_list.append(str(val))
        else:
            # Pour dates ou autres, on suppose que c'est déjà une string à insérer
            param_list.append(f"'{val}'")

    call_sql = f"CALL {proc_name}({', '.join(param_list)})"
    cursor.execute(call_sql)


def process_local_file(cursor, file_path, entity_name, date_str):
    logger.info(f"Traitement local de {entity_name} pour la date {date_str} - fichier: {file_path}")

    expected_cols = ENTITY_COLUMN_COUNTS.get(entity_name)
    if not expected_cols:
        logger.error(f"Nombre de colonnes inconnu pour l'entité {entity_name}")
        return

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')  # adapte le delimiter si besoin
        
        inserted = 0
        line_num = 0
        for row in reader:
            line_num += 1

            row = row[1:]

            # Optionnel : si le fichier a un header, on peut sauter la première ligne ici
            if line_num == 1:
                continue

            if len(row) != expected_cols:
                logger.warning(f"Ligne {line_num} ignorée (nombre colonnes {len(row)} != attendu {expected_cols}) dans {file_path}")
                continue

            try:
                call_insert_procedure(cursor, entity_name, row)
                inserted += 1
            except Exception as e:
                logger.warning(f"Erreur insertion ligne {line_num} dans {file_path}: {e}")

    logger.info(f"{inserted} lignes insérées pour {entity_name}_{date_str}")


def main():
    try:
        conn = connect_snowflake()
        cursor = conn.cursor()
        logger.info("Connexion établie à Snowflake.")

        # On parcourt le dossier DATA_HOSPITAL localement
        for root, dirs, files in os.walk(LOCAL_DATA_ROOT):
            for file_name in files:
                if not file_name.endswith(".txt"):
                    continue

                relative_path = os.path.relpath(os.path.join(root, file_name), LOCAL_DATA_ROOT)
                match = FILE_PATTERN.search(relative_path.replace("\\", "/"))  # pour Windows aussi
                if not match:
                    logger.info(f"Ignoré (fichier non reconnu) : {relative_path}")
                    continue

                date_str, entity = match.group(1), match.group(2)
                if entity not in EXPECTED_ENTITIES:
                    logger.warning(f"Fichier inattendu : {relative_path}")
                    continue

                full_local_path = os.path.join(LOCAL_DATA_ROOT, relative_path)
                try:
                    process_local_file(cursor, full_local_path, entity, date_str)
                except Exception as e:
                    logger.exception(f"Erreur sur fichier {entity}_{date_str} : {e}")

        conn.commit()  # commit global des inserts

    except Exception as e:
        logger.exception("Erreur inattendue pendant le chargement")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
