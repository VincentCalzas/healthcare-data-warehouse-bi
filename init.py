import os
import sys
import subprocess
from dotenv import load_dotenv

print("Configuration d'Airflow avec Snowflake")
print("==========================================")

if not os.path.isfile(".env"):
    print("Fichier .env non trouvé !")
    sys.exit(1)

load_dotenv()

required_vars = [
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_PRIVATE_KEY",
    "SNOWFLAKE_PRIVATE_KEY_PASSWORD"
]

missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print("Variables Snowflake manquantes dans le fichier .env !")
    print("Variables requises :", ", ".join(required_vars))
    sys.exit(1)

# Récupération des variables
sf_user = os.getenv("SNOWFLAKE_USER")
sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_role = os.getenv("SNOWFLAKE_ROLE")
sf_private_key = os.getenv("SNOWFLAKE_PRIVATE_KEY")
sf_private_key_password = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSWORD")

# 3. Installer les dépendances
print("Installation des dépendances...")
subprocess.run(["uv", "sync"], check=True)

# 4. Configurer Airflow
airflow_home = os.path.join(os.getcwd(), "airflow")
os.environ["AIRFLOW_HOME"] = airflow_home
print(f"Configuration d'Airflow (AIRFLOW_HOME={airflow_home})...")

# 5. Initialiser Airflow
print("Initialisation de Airflow...")
subprocess.run(["uv", "run", "airflow", "db", "migrate"], check=True)

# 6. Créer la connexion Snowflake
print("Création de la connexion Snowflake...")
print(f"   Utilisateur: {sf_user}")
print(f"   Compte: {sf_account}")
print(f"   Warehouse: {sf_warehouse}")
print(f"   Rôle: {sf_role}")


# Supprimer la connexion si elle existe déjà
print("Suppression de la connexion existante (si elle existe)...")
subprocess.run([
    "uv", "run", "airflow", "connections", "delete", "my_snowflake_conn"
], check=False) 


conn_extra = {
    "account": sf_account,
    "warehouse": sf_warehouse,
    "role": sf_role,
    "authenticator": "SNOWFLAKE_JWT",
    "private_key_file": sf_private_key
}

subprocess.run([
    "uv", "run", "airflow", "connections", "add", "my_snowflake_conn",
    "--conn-type", "snowflake",
    "--conn-login", sf_user,
    "--conn-password", sf_private_key_password,
    "--conn-extra", str(conn_extra).replace("'", '"')
], check=True)

print("\nConfiguration terminée ! Lancement d'Airflow...\n")
subprocess.run(["uv", "run", "airflow", "standalone"], check=True)
