import great_expectations as ge
from great_expectations.data_context import DataContext

# Initialize the DataContext
context = DataContext()

# Define the expectation suite name
expectation_suite_name = "medications_expectations"

# Check if the expectation suite exists and remove it if it does
try:
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
except ge.exceptions.DataContextError:
    pass

# Create a new expectation suite
context.add_expectation_suite(expectation_suite_name)

# Load your data
df = ge.read_csv('../../data/01_raw/medications.csv')

# Convert the DataFrame to a Great Expectations dataset
df_ge = ge.dataset.PandasDataset(df)

# Set the expectation suite
df_ge._expectation_suite = context.get_expectation_suite(expectation_suite_name)

# Add expectations
df_ge.expect_column_values_to_not_be_null('START')
df_ge.expect_column_values_to_be_of_type('START', 'datetime64[ns]')
df_ge.expect_column_values_to_be_of_type('STOP', 'datetime64[ns]')
df_ge.expect_column_values_to_not_be_null('PATIENT')
df_ge.expect_column_values_to_not_be_null('CODE')
df_ge.expect_column_mean_to_be_between('BASE_COST', 0, 500)

# Save the expectation suite
context.save_expectation_suite(expectation_suite=df_ge.get_expectation_suite())
