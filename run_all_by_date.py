import os
import subprocess
import re

DATA_ROOT = "./Data Hospital"
PATTERN = re.compile(r"BDD_HOSPITAL_(\d{8})")

def get_dates():
    dates = []
    for entry in os.listdir(DATA_ROOT):
        match = PATTERN.match(entry)
        if match:
            dates.append(match.group(1))
    return sorted(dates)

def main():
    dates = get_dates()
    for date in dates:
        print(f"\n=== Traitement pour la date {date} ===")

        # Étape 1 : Installation (création de la structure)
        print(f"-> Exécution de install_sid.py pour {date}")
        subprocess.run(["python", "install_sid.py"], check=True)

        # Étape 2 : Chargement des données pour cette date uniquement
        print(f"-> Exécution de launch_load_sid.py pour {date}")
        subprocess.run(["python", "launch_load_sid.py", "--date", date], check=True)

if __name__ == "__main__":
    main()
