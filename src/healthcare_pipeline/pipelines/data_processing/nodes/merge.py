def merge_datasets(patients_structured, symptoms_structured, medications_structured, conditions_structured, encounters_structured, patient_gender_structured):
    merged_df = patients_structured.join(patient_gender_structured, on="patient_id", how="left") \
                                   .join(symptoms_structured, on="patient_id", how="left") \
                                   .join(medications_structured, on="patient_id", how="left") \
                                   .join(conditions_structured, on="patient_id", how="left") \
                                   .join(encounters_structured, on="patient_id", how="left")
    return merged_df

def merge_two_datasets(df1, df2, on):
    return df1.join(df2, on=on, how="left")
