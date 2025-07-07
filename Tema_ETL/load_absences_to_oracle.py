import pandas as pd
import oracledb
import os

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "Cleaned_data/Absences")
csv_name = "Confluence_absences.csv"
csv_path = os.path.join(OUTPUT_DIR, csv_name)

oracle_user = "etl"
oracle_password = "etl_pass"
oracle_dsn = "localhost:1521/XEPDB1"

# === CONECTARE ORACLE ===
conn = oracledb.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsn)
cursor = conn.cursor()

# === CURĂȚARE TABEL ===
cursor.execute("TRUNCATE TABLE confluence_absence")

# === CITIRE XLSX ===
df = pd.read_csv(csv_path)

# === VALIDARE ȘI CURĂȚARE ===
df["SDATE"] = pd.to_datetime(df["SDATE"], errors="coerce").dt.date
df["EDATE"] = pd.to_datetime(df["EDATE"], errors="coerce").dt.date

df["SHOUR"] = df["SHOUR"].astype(str).str.strip()
df["EHOUR"] = df["EHOUR"].astype(str).str.strip()
df["CITY"] = df["CITY"].fillna("").astype(str)
df["REASON"] = df["REASON"].fillna("").astype(str)
df["NAME"] = df["NAME"].fillna("").astype(str)

df = df[df["SDATE"].notna() & df["EDATE"].notna()]

# === INSERARE CU ERORI LOGATE ===
insert_sql = """
    INSERT INTO confluence_absence (
        name, reason, sdate, shour,
        edate, ehour, city
    ) VALUES (:1, :2, :3, :4, :5, :6, :7)
"""

success_count = 0
failed_rows = []

for i, row in df.iterrows():
    try:
        values = (
            row["NAME"],
            row["REASON"],
            row["SDATE"],
            row["SHOUR"],
            row["EDATE"],
            row["EHOUR"],
            row["CITY"]
        )
        cursor.execute(insert_sql, values)
        success_count += 1
    except Exception as e:
        failed_rows.append((i, str(e), values))

# === FINAL ===
conn.commit()
cursor.close()
conn.close()

print(f"Inserate cu succes: {success_count}")
print(f" Rânduri eșuate: {len(failed_rows)}")


