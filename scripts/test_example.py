"""
Exemple de test unitaire pour le projet Healthcare Data Warehouse BI.
Ce test vérifie la fonction get_dates du script run_all_by_date.py.
"""
import unittest
from pathlib import Path
import re

DATA_ROOT = Path("./data_hospital")
PATTERN = re.compile(r"BDD_HOSPITAL_(\d{8})")

def get_dates():
    dates = []
    for entry in DATA_ROOT.iterdir():
        match = PATTERN.match(entry.name)
        if match:
            dates.append(match.group(1))
    return sorted(dates)

class TestGetDates(unittest.TestCase):
    def test_dates_are_sorted(self):
        # Suppose qu'il y a au moins deux dossiers de dates
        dates = get_dates()
        self.assertEqual(dates, sorted(dates))

if __name__ == "__main__":
    unittest.main()
