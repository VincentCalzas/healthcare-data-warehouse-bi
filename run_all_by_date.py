import subprocess
import re
from pathlib import Path

DATA_ROOT = Path("./Data Hospital")
PATTERN = re.compile(r"BDD_HOSPITAL_(\d{8})")

def get_dates():
    dates = []
    for entry in DATA_ROOT.iterdir():
        match = PATTERN.match(entry.name)
        if match:
            dates.append(match.group(1))
    return sorted(dates)

def main():
    dates = get_dates()
    for date in dates:
        print(f"\n=== Traitement pour la date {date} ===")

        # Étape 1 : Installation (création de la structure)
        print(f"-> Exécution de install_sid.py pour {date}")
        subprocess.run(["uv", "run", "python", "install_sid.py"], check=True)

        # Étape 2 : Chargement des données pour cette date uniquement
        print(f"-> Exécution de launch_load_sid.py pour {date}")
        subprocess.run(["uv", "run", "python", "launch_load_sid.py", "--date", date], check=True)

if __name__ == "__main__":
    main()
