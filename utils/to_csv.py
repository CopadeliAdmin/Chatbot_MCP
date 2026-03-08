"""
Export de la base capodeli_revops.db vers CSV
"""

import sqlite3
import csv
import os
from pathlib import Path

DB_PATH = Path("data") / "revops_demo.db"
CSV_PATH = "commandes.csv"

conn = sqlite3.connect(DB_PATH)
cur  = conn.cursor()

cur.execute("SELECT * FROM commandes")
rows    = cur.fetchall()
headers = [desc[0] for desc in cur.description]

with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

conn.close()
print(f"✅  {len(rows)} lignes exportées → {CSV_PATH}")