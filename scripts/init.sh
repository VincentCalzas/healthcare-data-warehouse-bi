#!/bin/bash

echo "Configuration d'Airflow avec Snowflake"
echo "=========================================="


# Charger les variables d'environnement depuis le fichier .env
if [ -f .env ]; then
    set -a # Automatically export all variables defined afterwards
    . ./.env # Source the .env file (the dot is shorthand for source)
    set +a # Turn off automatic export
else
    echo "Fichier .env non trouvé !"
    exit 1
fi

# Vérifier que les variables Snowflake sont définies
if [ -z "$SNOWFLAKE_USER" ] || [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_WAREHOUSE" ] || [ -z "$SNOWFLAKE_ROLE" ] || [ -z "$SNOWFLAKE_PRIVATE_KEY" ] || [ -z "$SNOWFLAKE_PRIVATE_KEY_PASSWORD" ]; then
    echo "Variables Snowflake manquantes dans le fichier .env !"
    echo "Variables requises : SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, SNOWFLAKE_PRIVATE_KEY, SNOWFLAKE_PRIVATE_KEY_PASSWORD"
    exit 1
fi

# 1. Installer les dépendances
echo "Installation des dépendances..."
uv sync

# 2. Configurer Airflow
echo "Configuration d'Airflow..."
export AIRFLOW_HOME=$(pwd)/airflow

# 3. Initialiser Airflow
echo "Initialisation de Airflow..."
uv run airflow db migrate


echo "Attente du démarrage d'Airflow..."
# sleep 10

# 4. Créer la connexion Snowflake en utilisant les variables d'environnement
echo "Création de la connexion Snowflake..."
echo "   Utilisateur: $SNOWFLAKE_USER"
echo "   Compte: $SNOWFLAKE_ACCOUNT"
echo "   Warehouse: $SNOWFLAKE_WAREHOUSE"
echo "   Rôle: $SNOWFLAKE_ROLE"

uv run airflow connections add my_snowflake_conn \
    --conn-type snowflake \
    --conn-login "$SNOWFLAKE_USER" \
    --conn-password "$SNOWFLAKE_PRIVATE_KEY_PASSWORD" \
    --conn-extra "{\
    \"account\":\"$SNOWFLAKE_ACCOUNT\",\
    \"warehouse\":\"$SNOWFLAKE_WAREHOUSE\",\
    \"role\":\"$SNOWFLAKE_ROLE\",\
    \"authenticator\": \"SNOWFLAKE_JWT\",\
    \"private_key_file\":\"$SNOWFLAKE_PRIVATE_KEY\"\
    }"

echo ""
echo "Configuration terminée ! Lancement d'Airflow..."
echo ""
uv run airflow standalone