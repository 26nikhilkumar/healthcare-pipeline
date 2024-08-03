from pyspark.sql.functions import col

def merge_two_datasets(df1, df2, key1, key2, join_type="left", suffix1="_df1", suffix2="_df2"):
    """
    Merges two DataFrames on specified keys, avoiding ambiguous column names by adding suffixes only in case of conflicts.

    Parameters:
    df1 (DataFrame): First DataFrame to merge.
    df2 (DataFrame): Second DataFrame to merge.
    key1 (str): Primary key in the first DataFrame.
    key2 (str): Primary key in the second DataFrame.
    join_type (str): Type of join to perform. Default is "left".
    suffix1 (str): Suffix for columns from the first DataFrame to avoid ambiguity.
    suffix2 (str): Suffix for columns from the second DataFrame to avoid ambiguity.

    Returns:
    DataFrame: Merged DataFrame.
    """
    df1_cols = [col(c).alias(c + suffix1) if c in df2.columns and c != key1 else col(c) for c in df1.columns]
    df2_cols = [col(c).alias(c + suffix2) if c in df1.columns and c != key2 else col(c) for c in df2.columns]
    merged_df = df1.select(*df1_cols).join(df2.select(*df2_cols), col(key1) == col(key2), how=join_type)
    # Drop the duplicate key column from the second DataFrame
    merged_df = merged_df.drop(df2[key2])
    return merged_df

def merge_datasets(patients_structured, symptoms_structured, medications_structured, conditions_structured, encounters_structured, patient_gender_structured):
    # Debug: Print the initial DataFrames
    # print("Patients DataFrame:")
    # patients_structured.show(5, truncate=False)
    # print("Symptoms DataFrame:")
    # symptoms_structured.show(5, truncate=False)
    # print("Medications DataFrame:")
    # medications_structured.show(5, truncate=False)
    # print("Conditions DataFrame:")
    # conditions_structured.show(5, truncate=False)
    # print("Encounters DataFrame:")
    # encounters_structured.show(5, truncate=False)
    # print("Patient Gender DataFrame:")
    # patient_gender_structured.show(5, truncate=False)
    
    # Rename patient_id in patient_gender to avoid ambiguity
    patient_gender_structured = patient_gender_structured.withColumnRenamed("patient_id", "patient_id_gender")

    # Merge patients with patient_gender
    merged_df = merge_two_datasets(patients_structured, patient_gender_structured, "patient_id", "patient_id_gender", "left", suffix1="_patients", suffix2="_gender")
    # print("After merging patients and patient_gender:")
    # merged_df.show(5, truncate=False)

    merged_df = merged_df.drop("sex")

    # Rename patient_id in symptoms to avoid ambiguity
    symptoms_structured = symptoms_structured.withColumnRenamed("patient_id", "patient_id_symptoms")

    # Merge with symptoms
    merged_df = merge_two_datasets(merged_df, symptoms_structured, "patient_id", "patient_id_symptoms", "left", suffix1="_patients", suffix2="_symptoms")
    # print("After merging with symptoms:")
    # merged_df.show(5, truncate=False)

    merged_df = merged_df.drop("patient_id_symptoms")

    # Rename patient_id in encounters to avoid ambiguity
    encounters_structured = encounters_structured.withColumnRenamed("patient_id", "patient_id_encounters")

    # Debug: Print encounter IDs before merging
    # print("Encounter IDs before merging:")
    # encounters_structured.select("patient_id_encounters", "encounter_id").show(5, truncate=False)

    # Merge with encounters
    merged_df = merge_two_datasets(merged_df, encounters_structured, "patient_id", "patient_id_encounters", "left", suffix1="_patients", suffix2="_encounters")
    # print("After merging with encounters:")
    # merged_df.show(5, truncate=False)

    merged_df = merged_df.drop("patient_id_encounters")
    merged_df = merged_df.withColumnRenamed("encounter_id_encounters", "encounter_id")

    # Rename encounter_id in medications to avoid ambiguity
    medications_structured = medications_structured.withColumnRenamed("encounter_id", "encounter_id_medications")

    # Merge with medications based on encounter_id
    merged_df = merge_two_datasets(merged_df, medications_structured, "encounter_id", "encounter_id_medications", "left", suffix1="", suffix2="_medications")
    # print("After merging with medications:")
    # merged_df.show(5, truncate=False)

    merged_df = merged_df.drop("patient_id_medications")

    # Rename encounter_id in conditions to avoid ambiguity
    conditions_structured = conditions_structured.withColumnRenamed("encounter_id", "encounter_id_conditions")

    # Merge with conditions based on encounter_id
    merged_df = merge_two_datasets(merged_df, conditions_structured, "encounter_id", "encounter_id_conditions", "left", suffix1="", suffix2="_conditions")
    # print("After merging with conditions:")
    # merged_df.show(5, truncate=False)

    merged_df = merged_df.drop("patient_id_conditions").withColumnRenamed("gender_patients", "sex")

    return merged_df
