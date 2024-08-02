import great_expectations as ge
from great_expectations.data_context import DataContext

# Initialize the DataContext
context = DataContext()

# Define the expectation suite name
expectation_suite_name = "encounters_expectations"

# Check if the expectation suite exists and remove it if it does
try:
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
except ge.exceptions.DataContextError:
    pass

# Create a new expectation suite
context.add_expectation_suite(expectation_suite_name)

# Load your data
df = ge.read_parquet('../../data/01_raw/encounters.parquet')

# Convert the DataFrame to a Great Expectations dataset
df_ge = ge.dataset.PandasDataset(df)

# Set the expectation suite
df_ge._expectation_suite = context.get_expectation_suite(expectation_suite_name)

# Add expectations
df_ge.expect_column_values_to_not_be_null('Id')
df_ge.expect_column_values_to_be_of_type('Id', 'int')
df_ge.expect_column_values_to_not_be_null('START')
df_ge.expect_column_values_to_be_of_type('START', 'datetime64[ns]')
df_ge.expect_column_values_to_not_be_null('STOP')
df_ge.expect_column_values_to_be_of_type('STOP', 'datetime64[ns]')
df_ge.expect_column_values_to_not_be_null('PATIENT')
df_ge.expect_column_values_to_not_be_null('ORGANIZATION')
df_ge.expect_column_values_to_not_be_null('PROVIDER')
df_ge.expect_column_values_to_not_be_null('PAYER')
df_ge.expect_column_values_to_not_be_null('ENCOUNTERCLASS')
df_ge.expect_column_values_to_not_be_null('CODE')
df_ge.expect_column_values_to_not_be_null('DESCRIPTION')
df_ge.expect_column_mean_to_be_between('BASE_ENCOUNTER_COST', 100, 10000)
df_ge.expect_column_mean_to_be_between('TOTAL_CLAIM_COST', 100, 10000)
df_ge.expect_column_mean_to_be_between('PAYER_COVERAGE', 0, 10000)

# Save the expectation suite
context.save_expectation_suite(expectation_suite=df_ge.get_expectation_suite())
