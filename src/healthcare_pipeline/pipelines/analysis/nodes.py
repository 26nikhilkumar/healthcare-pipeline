import pandas as pd
import matplotlib.pyplot as plt

def count_distinct_patients(merged_data_pandas: pd.DataFrame):
    distinct_patients = merged_data_pandas['patient_id'].nunique()
    print(f"Number of distinct patients: {distinct_patients}")
    return distinct_patients

def plot_distinct_medications_over_time(merged_data_pandas: pd.DataFrame):
    medication_start_column = 'encounter_start_date'

    # Check initial size
    print(f"Initial DataFrame size: {merged_data_pandas.shape}")

    # Convert to datetime
    merged_data_pandas[medication_start_column] = pd.to_datetime(merged_data_pandas[medication_start_column], errors='coerce')

    # Drop rows with missing values in medication_start_column
    medications_over_time = merged_data_pandas.dropna(subset=[medication_start_column])

    # Check size after dropping NA
    print(f"DataFrame size after dropping NA in {medication_start_column}: {medications_over_time.shape}")

    if medications_over_time.empty:
        print("No valid medication start dates available.")
        return

    # Extract year and month
    medications_over_time['year_month'] = medications_over_time[medication_start_column].dt.to_period('M')

    # Assuming the correct column name for medication description
    medication_description_column = 'medication_description'

    # Group by year_month and count unique medication descriptions
    medications_over_time_grouped = medications_over_time.groupby('year_month')[medication_description_column].nunique()

    # Check the grouped data
    # print(f"Grouped data: \n{medications_over_time_grouped}")

    if medications_over_time_grouped.empty:
        print("No data available for plotting after grouping.")
        return

    # Plotting
    plt.figure(figsize=(12, 6))
    medications_over_time_grouped.plot(kind='line', marker='o')
    plt.title('Distinct Medications Over Time')
    plt.xlabel('Year-Month')
    plt.ylabel('Number of Distinct Medications')
    plt.grid(True)
    plt.savefig("medications_over_time.png")
    plt.close()

def plot_racial_and_gender_distribution(merged_data_pandas: pd.DataFrame):
    race_distribution = merged_data_pandas['race_patients'].value_counts(normalize=True) * 100
    gender_distribution = merged_data_pandas['sex'].value_counts(normalize=True) * 100

    plt.figure(figsize=(8, 8))
    race_distribution.plot(kind='pie', autopct='%1.1f%%', startangle=140)
    plt.title('Percentage of Patients Across Racial Categories')
    plt.ylabel('')
    plt.savefig("race_distribution.png")
    plt.close()

    plt.figure(figsize=(8, 8))
    gender_distribution.plot(kind='pie', autopct='%1.1f%%', startangle=140)
    plt.title('Percentage of Patients Across Gender Categories')
    plt.ylabel('')
    plt.savefig("gender_distribution.png")
    plt.close()

def calculate_percentage_patients_with_symptoms(merged_data_pandas: pd.DataFrame):
    # Filter patients with 4 or more symptoms
    patients_with_symptoms = merged_data_pandas[merged_data_pandas['num_symptoms'] >= 4]

    # Calculate the percentage
    percentage_qualified_patients = (patients_with_symptoms['patient_id'].nunique() / 
                                     merged_data_pandas['patient_id'].nunique()) * 100
    print(f"Percentage of patients with 4 or more symptoms: {percentage_qualified_patients:.2f}%")
    return percentage_qualified_patients
