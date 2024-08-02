import great_expectations as ge
from great_expectations.data_context import DataContext

# Initialize the DataContext
context = DataContext()

# Define the expectation suite name
expectation_suite_name = "symptoms_expectations"

# Check if the expectation suite exists and remove it if it does
try:
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
except ge.exceptions.DataContextError:
    pass

# Create a new expectation suite
context.add_expectation_suite(expectation_suite_name)

# Load your data
df = ge.read_csv('../../data/01_raw/symptoms.csv')

# Convert the DataFrame to a Great Expectations dataset
df_ge = ge.dataset.PandasDataset(df)

# Set the expectation suite
df_ge._expectation_suite = context.get_expectation_suite(expectation_suite_name)

# Add expectations
df_ge.expect_column_values_to_not_be_null('PATIENT')
df_ge.expect_column_values_to_be_in_set('RACE', ['white', 'black', 'asian', 'native', 'other'])
df_ge.expect_column_values_to_be_in_set('ETHNICITY', ['hispanic', 'nonhispanic'])
df_ge.expect_column_values_to_not_be_null('AGE_BEGIN')
df_ge.expect_column_values_to_be_of_type('AGE_BEGIN', 'int')
df_ge.expect_column_values_to_not_be_null('SYMPTOMS')

# Save the expectation suite
context.save_expectation_suite(expectation_suite=df_ge.get_expectation_suite())
