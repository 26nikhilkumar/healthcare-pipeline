from kedro.pipeline import Pipeline, node
from .nodes.connect import load_datasets
from .nodes.clean import clean_datasets
from .nodes.structure import structure_datasets
from .nodes.merge import merge_datasets

def get_sqlite_connection(credentials):
    import sqlite3
    return sqlite3.connect(credentials["con"])

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
                lambda df, con: df.to_sql("merged_data", con, if_exists="replace", index=False),  # Save Pandas DataFrame to SQLite
                inputs=["merged_data_pandas", "params:sqlite_creds"],
                outputs=None,
                name="save_merged_data_to_sqlite"
            ),
        ]
    )
