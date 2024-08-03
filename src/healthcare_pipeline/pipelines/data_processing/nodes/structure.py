from pyspark.sql.functions import col

def structure_datasets(patients_df, symptoms_df, medications_df, conditions_df, encounters_df, patient_gender_df):
    # Rename columns in the patients DataFrame to match the Tuva model
    patients_structured = patients_df.select(
        col("PATIENT_ID").alias("patient_id"),
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
        col("BIRTHPLACE").alias("birthplace"),
        col("SSN").alias("social_security_number"),
        col("HEALTHCARE_EXPENSES").alias("healthcare_expenses"),
        col("HEALTHCARE_COVERAGE").alias("healthcare_coverage"),
        col("INCOME").alias("income"),
        col("LAT").alias("latitude"),
        col("LON").alias("longitude")
    )

    # Adjust the symptoms DataFrame to match the Tuva model
    symptoms_structured = symptoms_df.select(
        col("PATIENT").alias("patient_id"),
        col("GENDER").alias("gender"),
        col("RACE").alias("race"),
        col("ETHNICITY").alias("ethnicity"),
        col("AGE_BEGIN").alias("age_begin"),
        col("AGE_END").alias("age_end"),
        col("PATHOLOGY").alias("pathology"),
        col("NUM_SYMPTOMS").alias("num_symptoms"),
        col("symptom").alias("symptom_code"),
        col("frequency").alias("symptom_value")
    )

    # Adjust the medications DataFrame to match the Tuva model
    medications_structured = medications_df.select(
        col("START").alias("start_date"),
        col("STOP").alias("end_date"),
        col("PATIENT").alias("patient_id"),
        col("PAYER").alias("payer"),
        col("ENCOUNTER").alias("encounter_id"),
        col("CODE").alias("medication_code"),
        col("DESCRIPTION").alias("medication_description"),
        col("BASE_COST").alias("base_cost"),
        col("PAYER_COVERAGE").alias("payer_coverage"),
        col("DISPENSES").alias("dispenses"),
        col("TOTALCOST").alias("total_cost"),
        col("REASONCODE").alias("reason_code"),
        col("REASONDESCRIPTION").alias("reason_description"),
        col("out_of_pocket").alias("out_of_pocket")
    )

    # Adjust the conditions DataFrame to match the Tuva model
    conditions_structured = conditions_df.select(
        col("START").alias("onset_date"),
        col("STOP").alias("resolved_date"),
        col("PATIENT").alias("patient_id"),
        col("ENCOUNTER").alias("encounter_id"),
        col("CODE").alias("condition_code"),
        col("DESCRIPTION").alias("condition_description"),
        col("duration")
    )

    # Adjust the encounters DataFrame to match the Tuva model
    encounters_structured = encounters_df.select(
        col("Id").alias("encounter_id"),
        col("START").alias("encounter_start_date"),
        col("STOP").alias("encounter_end_date"),
        col("PATIENT").alias("patient_id"),
        col("ORGANIZATION").alias("organization"),
        col("PROVIDER").alias("provider"),
        col("PAYER").alias("payer"),
        col("ENCOUNTERCLASS").alias("encounter_class"),
        col("CODE").alias("primary_diagnosis_code"),
        col("DESCRIPTION").alias("primary_diagnosis_description"),
        col("BASE_ENCOUNTER_COST").alias("base_encounter_cost"),
        col("TOTAL_CLAIM_COST").alias("total_claim_cost"),
        col("PAYER_COVERAGE").alias("payer_coverage"),
        col("REASONCODE").alias("reason_code"),
        col("REASONDESCRIPTION").alias("reason_description"),
        col("duration")
    )

    # Adjust the patient gender DataFrame to match the Tuva model
    patient_gender_structured = patient_gender_df.select(
        col("Id").alias("patient_id"),
        col("GENDER").alias("gender")
    )

    return (patients_structured, symptoms_structured, medications_structured, conditions_structured, encounters_structured, patient_gender_structured)
