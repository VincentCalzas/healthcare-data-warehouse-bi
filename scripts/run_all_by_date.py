
"""
run_all_by_date.py

Automatise l'installation et le chargement des données hospitalières pour chaque date
disponible dans le dossier 'data_hospital'.

Étapes :
1. Exécute install_sid.py pour créer la structure de la base.
2. Exécute launch_load_sid.py pour charger les données de chaque date.

Auteur : Vincent
"""

import re
import subprocess
from pathlib import Path

DATA_ROOT = Path("./data_hospital")
PATTERN = re.compile(r"BDD_HOSPITAL_(\d{8})")

def get_dates():
    """
    Récupère la liste des dates disponibles dans le dossier DATA_ROOT.
    Retourne une liste triée de chaînes de caractères représentant les dates.
    """
    dates = []
    for entry in DATA_ROOT.iterdir():
        match = PATTERN.match(entry.name)
        if match:
            dates.append(match.group(1))
    return sorted(dates)

def main():
    """
    Pour chaque date trouvée, exécute les scripts d'installation et de chargement.
    Affiche les erreurs éventuelles sans interrompre le traitement des autres dates.
    """
    dates = get_dates()
    for date in dates:
        print(f"\n=== Traitement pour la date {date} ===")

        # Étape 1 : Installation (création de la structure)
        print(f"-> Exécution de install_sid.py pour {date}")
        try:
            subprocess.run(["uv", "run", "python", "install_sid.py"], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Erreur lors de l'installation pour {date}: {e}")
            continue

        # Étape 2 : Chargement des données pour cette date uniquement
        print(f"-> Exécution de launch_load_sid.py pour {date}")
        try:
            subprocess.run(
                ["uv", "run", "python", "launch_load_sid.py", "--date", date], check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Erreur lors du chargement des données pour {date}: {e}")
            continue

if __name__ == "__main__":
    main()
