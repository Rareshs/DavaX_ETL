import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "Raw_Data", "Absences")
OUTPUT_DIR = os.path.join(BASE_DIR, "Clean_data", "Absences")
INPUT_FILE = os.path.join(INPUT_DIR, "Pitesti.csv")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Pitesti_abs.csv")

df = pd.read_csv(INPUT_FILE)
df = df.dropna(subset=['ATTENDEE'])
df['SUMMARY'] = df['SUMMARY'].str.lower()

def map_reason(reason):
    if any(word in reason for word in ['project', 'presentation', 'master']):
        return 'PROJECT'
    elif any(word in reason for word in ['exam', 'test', 'school', 'university', 'examen', 'colocviu']):
        return 'EXAM'
    elif any(word in reason for word in ['annual leave', 'vacanta', 'concediu', 'vacation', 'leave']):
        return 'ANNUAL LEAVE'
    elif 'graduation' in reason:
        return 'GRADUATION'
    else:
        return 'FACULTY'

df['CATEGORY'] = df['SUMMARY'].apply(map_reason)

# Extrage data și ora, indiferent de spații sau format AM/PM
df[['SDATE', 'SHOUR']] = df['DTSTART'].astype(str).str.extract(r'^\s*([0-9/]+)\s+(.+)$')
df[['EDATE', 'EHOUR']] = df['DTEND'].astype(str).str.extract(r'^\s*([0-9/]+)\s+(.+)$')

df = df.rename(columns={'ATTENDEE': 'NAME', 'CATEGORY': 'REASON'})

df['CITY'] = "Pitesti"
df['NAME'] = df['NAME'].astype(str)
df['NAME'] = df['NAME'].str.split(';')
df = df.explode('NAME')
df['NAME'] = df['NAME'].str.strip()

df = df[['NAME', 'REASON', 'SDATE', 'SHOUR', 'EDATE', 'EHOUR', 'CITY']]



os.makedirs(OUTPUT_DIR, exist_ok=True)
df.to_csv(OUTPUT_FILE, index=False)


