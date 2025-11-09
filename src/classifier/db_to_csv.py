import sqlite3
import csv
import os
from pathlib import Path

# convert .db to .csv since classifier script accepts .csv files

DB_DIR = Path("../../data")
OUT_DIR = Path("./csvs/unfinished")

for db_path in DB_DIR.glob("*.db"):
    db_stem = db_path.stem
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        Select name 
        From sqlite_master 
        Where type='table' And name Not Like 'sqlite_%'
    """)
    tables = [row[0] for row in cur.fetchall()]

    for table in tables:
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

        csv_filename = f"{db_stem}_{table}.csv"
        csv_path = OUT_DIR / csv_filename

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)

        print(f"Exported {db_path.name}:{table} -> {csv_filename}")

    conn.close()
