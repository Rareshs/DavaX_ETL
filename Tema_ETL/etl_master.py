import os
import subprocess

# === Definește toate scripturile ETL în ordinea execuției ===
VENV_PYTHON = os.path.join(os.getcwd(), ".venv", "Scripts", "python.exe")
if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = "python"  # Fallback to system Python if venv not found


etl_scripts = [
    ("🔄 Running: Clean Attendance (Merge)", "transform_attendance_raw_clean.py"),
    ("🔄 Running: Clean Absence Bucuresti", "transform_absence_raw_clean_buc.py"),
    ("🔄 Running: Clean Absence Cluj", "transform_absence_raw_clean_cj.py"),
    ("🔄 Running: Clean Absence Pitesti", "transform_absence_raw_clean_pit.py"),
    ("🔄 Running: Load training into Oracle", "load_training_to_oracle.py"),
    ("🔄 Running: Load Absence into Oracle", "load_absences_to_oracle.py")
]

# === Setează folderul de bază ===
BASE_DIR = os.path.join(os.getcwd(), "Tema_ETL")
results = []

for message, script_name in etl_scripts:
    script_path = os.path.join(BASE_DIR, script_name)
    print(message)

    if not os.path.exists(script_path):
        results.append((message, "❌ SKIPPED (File Not Found)"))
        continue

    try:
        subprocess.run([VENV_PYTHON, script_path], check=True)
        results.append((message, "✅ Success"))
    except subprocess.CalledProcessError as e:
        results.append((message, f"❌ ERROR: {e}"))

# === Summary
print("\n📋 ETL Summary:")
for msg, status in results:
    print(f"{msg} -> {status}")