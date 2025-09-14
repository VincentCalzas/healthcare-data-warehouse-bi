
# Healthcare Data Warehouse BI

Projet de gestion et d’analyse de données hospitalières avec Airflow et Snowflake.

## Objectifs
- Automatiser le chargement et le traitement de données hospitalières
- Utiliser Snowflake comme entrepôt de données
- Orchestration via Apache Airflow

## Structure du projet
- `Data Hospital/` : Données hospitalières brutes
- `scripts/` : Scripts SQL et Python sans dag
- `airflow/dags/` : DAGs Airflow pour l’orchestration
- `model/` : Modèles de données
- `reports/` : Rapports et analyses

## Installation

```sh
git clone https://github.com/ton-utilisateur/healthcare-data-warehouse-bi.git
cd healthcare-data-warehouse-bi
pip install -r requirements.txt
```

## Utilisation

```sh
uv run python scripts/install_sid.py
uv run python scripts/launch_load_sid.py --date YYYYMMDD
uv run python scripts/run_all_by_date.py
```

## Tests

```sh
uv run python -m unittest scripts/test_example.py
```

Ce projet a été réalisé en collaboration avec [Smart Teem](https://www.smartteem.com/)
