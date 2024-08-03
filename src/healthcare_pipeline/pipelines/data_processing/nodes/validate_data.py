import subprocess
import pandas as pd
import os

def run_gx_validations(merged_data_pandas):
    # Save the merged data to a temporary CSV file for validation
    merged_data_path = "gx/validations/merged_data_temp.csv"
    merged_data_pandas.to_csv(merged_data_path, index=False)
    
    # Set the environment variable for the data path
    os.environ["MERGED_DATA_PATH"] = merged_data_path

    scripts = [
        "gx/validations/run_conditions_validations.py",
        "gx/validations/run_encounters_validations.py",
        "gx/validations/run_medications_validations.py",
        "gx/validations/run_patients_validations.py",
        "gx/validations/run_patient_gender_validations.py",
        "gx/validations/run_symptoms_validations.py"
    ]

    for script in scripts:
        subprocess.run(["python", script], check=True)
    
    # Remove the temporary file after validation
    os.remove(merged_data_path)
