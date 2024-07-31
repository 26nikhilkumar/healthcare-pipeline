from pyspark.sql.functions import col

def structure_datasets(patients_df, symptoms_df, medications_df, conditions_df, encounters_df, patient_gender_df):
    # Rename columns in the patients DataFrame to match the expected schema
    patients_structured = patients_df.select(
        col("ID").alias("patient_id"),
        col("FIRST").alias("first_name"),
        col("LAST").alias("last_name"),
        col("GENDER").alias("sex"),
        col("RACE").alias("race"),
        col("birth_date"),
        col("death_date"),
        col("ADDRESS").alias("address"),
        col("CITY").alias("city"),
        col("STATE").alias("state"),
        col("ZIP").alias("zip_code"),
        col("BIRTHPLACE").alias("birthplace")
    )

    # Adjust the symptoms DataFrame to match the expected schema
    symptoms_structured = symptoms_df.select(
        col("PATIENT").alias("patient_id"),
        col("symptom"),
        col("frequency")
    )

    # Adjust the medications DataFrame to match the expected schema
    medications_structured = medications_df.select(
        col("ID").alias("medication_id"),
        col("PATIENT").alias("patient_id"),
        col("start"),
        col("stop"),
        col("out_of_pocket"),
        col("DESCRIPTION").alias("description")
    )

    # Adjust the conditions DataFrame to match the expected schema
    conditions_structured = conditions_df.select(
        col("ID").alias("condition_id"),
        col("PATIENT").alias("patient_id"),
        col("start"),
        col("stop"),
        col("duration"),
        col("DESCRIPTION").alias("description")
    )

    # Adjust the encounters DataFrame to match the expected schema
    encounters_structured = encounters_df.select(
        col("ID").alias("encounter_id"),
        col("PATIENT").alias("patient_id"),
        col("start"),
        col("stop"),
        col("duration"),
        col("REASONDESCRIPTION").alias("reason_description")
    )

    # Adjust the patient gender DataFrame to match the expected schema
    patient_gender_structured = patient_gender_df.select(
        col("ID").alias("patient_id"),
        col("GENDER").alias("gender")
    )

    return (patients_structured, symptoms_structured, medications_structured, conditions_structured, encounters_structured, patient_gender_structured)
