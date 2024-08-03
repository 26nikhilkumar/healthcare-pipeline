from kedro.pipeline import Pipeline, node
from .nodes import (
    count_distinct_patients,
    plot_distinct_medications_over_time,
    plot_racial_and_gender_distribution,
    calculate_percentage_patients_with_symptoms
)
import pandas as pd
import sqlite3

def load_data_from_sqlite(sqlite_creds: dict) -> pd.DataFrame:
    try:
        if not isinstance(sqlite_creds, dict):
            raise ValueError("Expected sqlite_creds to be a dictionary")

        db_path = sqlite_creds["con"]
        print(f"Database path: {db_path}")

        # Load data from SQLite into a Pandas DataFrame
        conn = sqlite3.connect(db_path.replace("sqlite:///", ""))
        df = pd.read_sql_query("SELECT * FROM merged_data", conn)
        conn.close()

        return df
    except Exception as e:
        print(f"An error occurred while loading data from SQLite: {e}")
        raise e

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=load_data_from_sqlite,
                inputs="params:sqlite_creds",
                outputs="merged_data_from_db",
                name="load_data_from_sqlite"
            ),
            node(
                func=count_distinct_patients,
                inputs="merged_data_from_db",
                outputs="distinct_patients",
                name="count_distinct_patients"
            ),
            node(
                func=plot_distinct_medications_over_time,
                inputs="merged_data_from_db",
                outputs=None,
                name="plot_distinct_medications_over_time"
            ),
            node(
                func=plot_racial_and_gender_distribution,
                inputs="merged_data_from_db",
                outputs=None,
                name="plot_racial_and_gender_distribution"
            ),
            node(
                func=calculate_percentage_patients_with_symptoms,
                inputs="merged_data_from_db",
                outputs="percentage_patients_with_symptoms",
                name="calculate_percentage_patients_with_symptoms"
            )
        ]
    )
