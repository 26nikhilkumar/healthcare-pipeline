import pandas as pd
import sqlite3

def load_datasets(patients, symptoms, medications, conditions, encounters, patient_gender):
    return patients, symptoms, medications, conditions, encounters, patient_gender

def save_to_database(patients_cleaned, symptoms_cleaned, medications_cleaned, conditions_cleaned, encounters_cleaned, patient_gender_cleaned):
    conn = sqlite3.connect("data/healthcare.db")

    # Convert PySpark DataFrames to Pandas DataFrames
    patients_cleaned_pd = patients_cleaned.toPandas().drop("ID",axis=1)
    symptoms_cleaned_pd = symptoms_cleaned.toPandas()
    medications_cleaned_pd = medications_cleaned.toPandas().drop("ID",axis=1)
    conditions_cleaned_pd = conditions_cleaned.toPandas().drop("ID",axis=1)
    encounters_cleaned_pd = encounters_cleaned.toPandas().drop("ID",axis=1)
    patient_gender_cleaned_pd = patient_gender_cleaned.toPandas()
    # Log the contents of the DataFrames
    print("Patients DataFrame:\n", patients_cleaned_pd.head())
    print("Symptoms DataFrame:\n", symptoms_cleaned_pd.head())
    print("Medications DataFrame:\n", medications_cleaned_pd.head())
    print("Conditions DataFrame:\n", conditions_cleaned_pd.head())
    print("Encounters DataFrame:\n", encounters_cleaned_pd.head())
    print("Patient Gender DataFrame:\n", patient_gender_cleaned_pd.head())

    # Save Pandas DataFrames to SQLite without index
    patients_cleaned_pd.to_sql("patients_cleaned", conn, if_exists="replace", index=False)
    symptoms_cleaned_pd.to_sql("symptoms_cleaned", conn, if_exists="replace", index=False)
    medications_cleaned_pd.to_sql("medications_cleaned", conn, if_exists="replace", index=False)
    conditions_cleaned_pd.to_sql("conditions_cleaned", conn, if_exists="replace", index=False)
    encounters_cleaned_pd.to_sql("encounters_cleaned", conn, if_exists="replace", index=False)
    patient_gender_cleaned_pd.to_sql("patient_gender_cleaned", conn, if_exists="replace", index=False)

    conn.close()

    return "Data saved to SQLite database successfully"
