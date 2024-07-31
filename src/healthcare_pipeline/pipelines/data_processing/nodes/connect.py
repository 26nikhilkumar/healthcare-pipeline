import pandas as pd
import sqlite3

def load_datasets(patients, symptoms, medications, conditions, encounters, patient_gender):
    return patients, symptoms, medications, conditions, encounters, patient_gender

def save_to_database(patients_cleaned, symptoms_cleaned, medications_cleaned, conditions_cleaned, encounters_cleaned, patient_gender_cleaned):
    conn = sqlite3.connect("data/healthcare.db")

    # Convert PySpark DataFrames to Pandas DataFrames
    patients_cleaned_pd = patients_cleaned.toPandas()
    symptoms_cleaned_pd = symptoms_cleaned.toPandas()
    medications_cleaned_pd = medications_cleaned.toPandas()
    conditions_cleaned_pd = conditions_cleaned.toPandas()
    encounters_cleaned_pd = encounters_cleaned.toPandas()
    patient_gender_cleaned_pd = patient_gender_cleaned.toPandas()

    # Save Pandas DataFrames to SQLite
    patients_cleaned_pd.to_sql("patients_cleaned", conn, if_exists="replace", index=False)
    symptoms_cleaned_pd.to_sql("symptoms_cleaned", conn, if_exists="replace", index=False)
    medications_cleaned_pd.to_sql("medications_cleaned", conn, if_exists="replace", index=False)
    conditions_cleaned_pd.to_sql("conditions_cleaned", conn, if_exists="replace", index=False)
    encounters_cleaned_pd.to_sql("encounters_cleaned", conn, if_exists="replace", index=False)
    patient_gender_cleaned_pd.to_sql("patient_gender_cleaned", conn, if_exists="replace", index=False)

    conn.close()

    return "Data saved to SQLite database successfully"
