import pandas as pd
import oracledb
import os

# === CONFIGURATION ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.join(BASE_DIR, "Cleaned_data/Training")

oracle_user = "etl"
oracle_password = "etl_pass"
oracle_dsn = "localhost:1521/XEPDB1"

# === ORACLE CONNECTION ===
conn = oracledb.connect(user=oracle_user, password=oracle_password, dsn=oracle_dsn)
cursor = conn.cursor()

# === CLEAR STAGING TABLE ===
cursor.execute("TRUNCATE TABLE stg_attandance")

# === LOAD AND INSERT ALL CLEANED CSVs ===
for file in os.listdir(base_path):
    if not file.endswith(".csv"):
        continue

    
    full_path = os.path.join(base_path, file)
    source_label = os.path.splitext(file)[0].replace("Processed_", "")
    
    print(f"\n🔄 Processing file: {file} [{source_label}]")
    df = pd.read_csv(full_path)

    # Evită eventualul header dublu
    df = df[df["First Join"] != "Join Time"]

    # Conversii date
    df["first_join"] = pd.to_datetime(df["First Join"], errors="coerce")
    df["last_leave"] = pd.to_datetime(df["Last Leave"], errors="coerce")

    # Eliminare date invalide
    bad_rows = df[df["first_join"].isna() | df["last_leave"].isna()]
    if not bad_rows.empty:
        print(f"⚠️ Skipping {len(bad_rows)} rows with invalid date formats.")
    df = df[df["first_join"].notna() & df["last_leave"].notna()]

    # Normalizează coloane string
    string_cols = [
        "Name", "In-Meeting Duration", "Email", "Participant ID (UPN)",
        "Role", "Calculated_duration"
    ]
    df[string_cols] = df[string_cols].fillna("").astype(str)

    df["source_file"] = source_label
    df["Duration_minutes"] = pd.to_numeric(df["Duration_minutes"], errors="coerce").fillna(0)

    # === Pregătim datele pentru Oracle ===
    rows = list(df[[ 
        "Name", "first_join", "last_leave", "In-Meeting Duration",
        "Email", "Participant ID (UPN)", "Role",
        "Calculated_duration", "Duration_minutes", "source_file"
    ]].itertuples(index=False, name=None))

    cursor.executemany("""
        INSERT INTO stg_attandance (
            name, first_join, last_leave, in_meeting_duration,
            email, participant_id, role,
            calculated_duration, duration_minutes, source_file
        ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)
    """, rows)

    print(f"✅ Inserted {len(rows)} records from {file}")

# === COMMIT ȘI ÎNCHIDERE ===
conn.commit()
cursor.close()
conn.close()
