import pandas as pd
import os
from datetime import timedelta
from io import StringIO

# Setează directorul de bază ca fiind folderul în care se află acest notebook
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#BASE_DIR = os.getcwd()

# Setează căile pentru Raw_data și Cleaned_data dinamic
INPUT_DIR = os.path.join(BASE_DIR, "Raw_data/Training")
OUTPUT_DIR = os.path.join(BASE_DIR, "Cleaned_data/Training")

def process_xls_file(input_path):
    try:
        xls = pd.ExcelFile(input_path)
        sheet_name = next((s for s in xls.sheet_names if "Participant" in s), xls.sheet_names[0])
        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=None)

        # Caută rândul unde începe headerul tabelului (coloana 0 == "Name")
        header_row = df_raw[df_raw.iloc[:, 0] == "Name"].index[0]

        # Caută rândul unde apare "3. In-Meeting Activities"
        end_row = df_raw[df_raw.iloc[:, 0].astype(str).str.contains("3. In-Meeting Activities", na=False)].index
        if len(end_row) > 0:
            end_row = end_row[0]
            nrows = end_row - header_row - 2  # -1 ca să nu includă rândul cu "3. In-Meeting Activities"
        else:
            nrows = None  # Citește până la final dacă nu există

        df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row, nrows=nrows)


        required_cols = ['Name', 'First Join', 'Last Leave', 'In-Meeting Duration',
                         'Email', 'Participant ID (UPN)', 'Role']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""

        df['First Join'] = pd.to_datetime(df['First Join'], format='%m/%d/%y, %I:%M:%S %p')
        df['Last Leave'] = pd.to_datetime(df['Last Leave'], format='%m/%d/%y, %I:%M:%S %p')
        df['Duration_seconds'] = (df['Last Leave'] - df['First Join']).dt.total_seconds()
        df['Duration_minutes'] = (df['Duration_seconds'] / 60).round(1)
        df['Calculated_duration'] = df['Duration_seconds'].apply(
            lambda x: str(timedelta(seconds=int(x))) if pd.notnull(x) else ""
        )

        df['Role'] = df['Role'].astype(str).str.strip()
        df = df[df['Role'].str.lower() != 'organizer']

        columns_order = [
            'Name', 'First Join', 'Last Leave', 'In-Meeting Duration',
            'Email', 'Participant ID (UPN)', 'Role',
            'Calculated_duration', 'Duration_minutes'
        ]
        df_final = df[columns_order]
        return df_final

    except Exception as e:
        print(f"⚠️ Eroare la procesarea fișierului {input_path}: {e}")
        return
    
def process_csv_file(input_path):
    try:
        # Citim fișierul ca text
        with open(input_path, "r", encoding="utf-16") as f:
            lines = f.readlines()

        # Căutăm secțiunea "2. Participants" -> "3. In-Meeting Activities"
        start = next(i for i, line in enumerate(lines) if "2. Participants" in line)
        end = next(i for i, line in enumerate(lines) if "3. In-Meeting Activities" in line)
        participant_lines = lines[start + 1:end]

        # Conversie în DataFrame
        data_str = ''.join(participant_lines)
        df = pd.read_csv(StringIO(data_str), sep='\t')

        # Transformări
        df['First Join'] = pd.to_datetime(df['First Join'], format='%m/%d/%y, %I:%M:%S %p')
        df['Last Leave'] = pd.to_datetime(df['Last Leave'], format='%m/%d/%y, %I:%M:%S %p')
        df['Duration_seconds'] = (df['Last Leave'] - df['First Join']).dt.total_seconds()
        df['Duration_minutes'] = (df['Duration_seconds'] / 60).round(1)
        df['Calculated_duration'] = df['Duration_seconds'].apply(
            lambda x: str(timedelta(seconds=int(x)))
        )

        df['Role'] = df['Role'].astype(str).str.strip()  
        df = df[df['Role'].str.lower() != 'organizer']
        
        # Selectăm coloanele necesare
        columns_order = [
            'Name', 'First Join', 'Last Leave', 'In-Meeting Duration',
            'Email', 'Participant ID (UPN)', 'Role',
            'Calculated_duration', 'Duration_minutes'
        ]
        df_final = df[columns_order]
        return df_final

    except Exception as e:
        print(f"⚠️ Eroare la procesarea fișierului {input_path}: {e}")
        return None
    
def save_file(df_final, filename, output_dir):
    if df_final is not None:
        # Salvează toate fișierele ca .csv, indiferent de extensia originală
        base_name = os.path.splitext(filename)[0]
        output_filename = f"Processed_{base_name}.csv"
        output_path = os.path.join(output_dir, output_filename)
        df_final.to_csv(output_path, index=False, encoding='utf-8')
        print(f"[✓] Fișier salvat la: {output_path}")

os.makedirs(OUTPUT_DIR, exist_ok=True)
# Parcurgem toate fișierele din folderul de input
for filename in os.listdir(INPUT_DIR):
    input_path = os.path.join(INPUT_DIR, filename)
    print(f"\n🔄 Procesare fișier: {filename}")
    df_final = None
    if filename.endswith(".csv"):
        df_final = process_csv_file(input_path)
    elif filename.endswith(".xls") or filename.endswith(".xlsx"):
        df_final = process_xls_file(input_path)
    save_file(df_final, filename, OUTPUT_DIR)

