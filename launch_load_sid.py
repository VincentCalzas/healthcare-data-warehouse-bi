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

# Chemin local racine 
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

def normalize_timestamp(ts_str):

    # Exemple attendu : '2024-04-29-17-25-30'

    try:

        if ts_str is None:

            return None

        ts_str = ts_str.replace("#", "0")

        parts = ts_str.split('-')

        if len(parts) == 6:

            date_part = '-'.join(parts[0:3])   # '2024-04-29'

            time_part = ':'.join(parts[3:6])   # '17:25:30'

            return f"{date_part} {time_part}"

        else:

            return ts_str

    except Exception:

        return ts_str


def create_procedures(cursor):
    logger.info("Création des procédures...")
    with open("scripts/insert_STG_procedures.sql", "r", encoding="utf-8") as f:
        sql_script = f.read()

    # On découpe chaque procédure avec END; suivie de $$ (fin de bloc)
    procedure_blocks = re.findall(
        r'(CREATE OR REPLACE PROCEDURE.*?END;\s*\$\$)',
        sql_script,
        flags=re.DOTALL | re.IGNORECASE
    )

    for i, proc_sql in enumerate(procedure_blocks, start=1):
        try:
            cursor.execute(proc_sql)
            logger.info(f"Procédure {i} créée avec succès.")
        except Exception as e:
            logger.warning(f"Erreur création procédure {i}: {e}")


def call_insert_procedure(cursor, entity_name, all_rows):
    proc_name = f"insert_{entity_name.lower()}"

    # Transposer les lignes en colonnes (zip)
    columns = list(zip(*all_rows))

    param_list = []
    for col in columns:
        formatted_col = []
        for val in col:
            if val is None or val == '':
                formatted_col.append("NULL")
            else:
                val = str(val).replace("'", "''")  # Échappement des quotes
                formatted_col.append(f"'{val}'")
        array_str = f"ARRAY_CONSTRUCT({', '.join(formatted_col)})"
        param_list.append(array_str)

    call_sql = f"CALL {proc_name}({', '.join(param_list)})"
    logger.debug(f"Appel SQL: {call_sql}")
    cursor.execute(call_sql)


def process_local_file(cursor, file_path, entity_name, date_str):
    logger.info(f"Traitement local de {entity_name} pour la date {date_str} - fichier: {file_path}")

    expected_cols = ENTITY_COLUMN_COUNTS.get(entity_name)
    if not expected_cols:
        logger.error(f"Nombre de colonnes inconnu pour l'entité {entity_name}")
        return

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        
        all_rows = []
        line_num = 0

        for row in reader:
            line_num += 1
            row = row[1:]  # Retirer identifiant ou marqueur de début

            if line_num == 1:
                continue  # Saut du header si nécessaire

            if entity_name == "CONSULTATION":
                row[3] = normalize_timestamp(row[3])
                row[4] = normalize_timestamp(row[4])
            if entity_name == "TRAITEMENT":
                row[7] = normalize_timestamp(row[7])
            if entity_name == "PERSONNEL":
                row[4] = normalize_timestamp(row[4])
                row[5] = normalize_timestamp(row[5])
            if entity_name == "HOSPITALISATION":
                row[3] = normalize_timestamp(row[3])
                row[4] = normalize_timestamp(row[4])

            if len(row) != expected_cols:
                logger.warning(f"Ligne {line_num} ignorée (colonnes {len(row)} != {expected_cols}) dans {file_path}")
                continue

            all_rows.append(row)

        if not all_rows:
            logger.warning(f"Aucune donnée valide dans {file_path}")
            return

        try:
            call_insert_procedure(cursor, entity_name, all_rows)
            logger.info(f"{len(all_rows)} lignes insérées pour {entity_name}_{date_str}")
        except Exception as e:
            logger.exception(f"Erreur d'insertion pour {entity_name}_{date_str} : {e}")


def main():
    try:
        conn = connect_snowflake()
        cursor = conn.cursor()
        logger.info("Connexion établie à Snowflake.")

        create_procedures(cursor)

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
