from pyspark.sql.functions import col

def merge_datasets(patients_structured, symptoms_structured, medications_structured, conditions_structured, encounters_structured, patient_gender_structured):
    # Print schemas for debugging
    print("Patients schema:")
    patients_structured.printSchema()
    print("Symptoms schema:")
    symptoms_structured.printSchema()
    print("Medications schema:")
    medications_structured.printSchema()
    print("Conditions schema:")
    conditions_structured.printSchema()
    print("Encounters schema:")
    encounters_structured.printSchema()
    print("Patient gender schema:")
    patient_gender_structured.printSchema()

    # Merge patients with patient_gender
    merged_df = patients_structured.alias("patients").join(
        patient_gender_structured.alias("gender"),
        col("patients.patient_id") == col("gender.patient_id"),
        how="left"
    ).select("patients.*", "gender.gender")

    # Merge patients with symptoms
    merged_df = merged_df.alias("merged").join(
        symptoms_structured.alias("symptoms"),
        col("merged.patient_id") == col("symptoms.patient_id"),
        how="left"
    ).select("merged.*", "symptoms.symptom", "symptoms.frequency")

    # Merge patients with encounters
    merged_df = merged_df.alias("merged").join(
        encounters_structured.alias("encounters"),
        col("merged.patient_id") == col("encounters.patient_id"),
        how="left"
    ).select("merged.*", 
             col("encounters.encounter_id").alias("encounter_encounter_id"), 
             col("encounters.start").alias("encounter_start"), 
             col("encounters.stop").alias("encounter_stop"), 
             col("encounters.duration").alias("encounter_duration"), 
             col("encounters.reason_description").alias("encounter_reason_description"))

    # Merge encounters with medications based on encounter_id
    merged_df = merged_df.alias("merged").join(
        medications_structured.alias("medications"),
        col("merged.encounter_encounter_id") == col("medications.encounter"),
        how="left"
    ).select("merged.*", 
             col("medications.start").alias("medication_start"), 
             col("medications.stop").alias("medication_stop"), 
             col("medications.description").alias("medication_description"))

    # Merge encounters with conditions based on encounter_id
    merged_df = merged_df.alias("merged").join(
        conditions_structured.alias("conditions"),
        col("merged.encounter_encounter_id") == col("conditions.encounter"),
        how="left"
    ).select("merged.*", 
             col("conditions.start").alias("condition_start"), 
             col("conditions.stop").alias("condition_stop"), 
             col("conditions.duration").alias("condition_duration"), 
             col("conditions.description").alias("condition_description"))

    return merged_df
