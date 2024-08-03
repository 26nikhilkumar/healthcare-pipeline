import pandas as pd
import sqlite3

# Paths to your data files
patients_path = "../data/01_raw/patients.csv"
symptoms_path = "../data/01_raw/symptoms.csv"
medications_path = "../data/01_raw/medications.csv"
conditions_path = "../data/01_raw/conditions.xlsx"
encounters_path = "../data/01_raw/encounters.parquet"
patient_gender_path = "../data/01_raw/patient_gender.csv"

# Path to your SQLite database
db_path = "/config/workspace/case_study/healthcare-pipeline/data/healthcare.db"

# Connect to the database
conn = sqlite3.connect(db_path)

# Load data into pandas DataFrames
patients_df = pd.read_csv(patients_path)
symptoms_df = pd.read_csv(symptoms_path)
medications_df = pd.read_csv(medications_path)
conditions_df = pd.read_excel(conditions_path)  # Updated to read xlsx
encounters_df = pd.read_parquet(encounters_path)
patient_gender_df = pd.read_csv(patient_gender_path)

# Write DataFrames to SQLite
patients_df.to_sql("raw_patients", conn, if_exists="replace", index=False)
symptoms_df.to_sql("raw_symptoms", conn, if_exists="replace", index=False)
medications_df.to_sql("raw_medications", conn, if_exists="replace", index=False)
conditions_df.to_sql("raw_conditions", conn, if_exists="replace", index=False)
encounters_df.to_sql("raw_encounters", conn, if_exists="replace", index=False)
patient_gender_df.to_sql("raw_patient_gender", conn, if_exists="replace", index=False)

# Close the connection
conn.close()