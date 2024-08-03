from kedro.pipeline import Pipeline, node
from .nodes.connect import load_datasets
from .nodes.clean import clean_datasets
from .nodes.structure import structure_datasets
from .nodes.merge import merge_datasets
import pandas as pd
import sqlite3

def save_merged_data_to_sqlite(merged_data_pandas: pd.DataFrame, sqlite_creds: dict):
    try:
        if not isinstance(sqlite_creds, dict):
            raise ValueError("Expected sqlite_creds to be a dictionary")

        db_path = sqlite_creds["con"]
        print(f"Database path: {db_path}")

        # Save Pandas DataFrame to SQLite in chunks
        chunk_size = 50000  # Adjust chunk size as needed
        conn = sqlite3.connect(db_path.replace("sqlite:///", ""))
        for i in range(0, len(merged_data_pandas), chunk_size):
            merged_data_pandas.iloc[i:i + chunk_size].to_sql("merged_data", conn, if_exists="append", index=False)
        conn.close()

        print("Data saved to SQLite successfully.")
    except Exception as e:
        print(f"An error occurred while saving data to SQLite: {e}")
        raise e

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                load_datasets,
                inputs=["patients", "symptoms", "medications", "conditions", "encounters", "patient_gender"],
                outputs=["patients_loaded", "symptoms_loaded", "medications_loaded", "conditions_loaded", "encounters_loaded", "patient_gender_loaded"],
                name="load_datasets",
            ),
            node(
                clean_datasets,
                inputs=["patients_loaded", "symptoms_loaded", "medications_loaded", "conditions_loaded", "encounters_loaded", "patient_gender_loaded"],
                outputs=["patients_cleaned", "symptoms_cleaned", "medications_cleaned", "conditions_cleaned", "encounters_cleaned", "patient_gender_cleaned"],
                name="clean_datasets",
            ),
            node(
                structure_datasets,
                inputs=["patients_cleaned", "symptoms_cleaned", "medications_cleaned", "conditions_cleaned", "encounters_cleaned", "patient_gender_cleaned"],
                outputs=["patients_structured", "symptoms_structured", "medications_structured", "conditions_structured", "encounters_structured", "patient_gender_structured"],
                name="structure_datasets",
            ),
            node(
                merge_datasets,
                inputs=["patients_structured", "symptoms_structured", "medications_structured", "conditions_structured", "encounters_structured", "patient_gender_structured"],
                outputs="merged_data",
                name="merge_datasets",
            ),
            node(
                lambda df: df.toPandas(),  # Convert Spark DataFrame to Pandas DataFrame
                inputs="merged_data",
                outputs="merged_data_pandas",
                name="convert_to_pandas"
            ),
            node(
                save_merged_data_to_sqlite,  # Save Pandas DataFrame to SQLite with chunking
                inputs=["merged_data_pandas", "params:sqlite_creds"],
                outputs=None,
                name="save_merged_data_to_sqlite"
            ),
        ]
    )
