from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
import great_expectations as ge
import pandas as pd

# Load your data context
context = ge.data_context.DataContext()

# Load your data
df = pd.read_csv('../../data/01_raw/patient_gender.csv')

# Define the batch request
batch_request = RuntimeBatchRequest(
    datasource_name="my_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="patient_gender",  # this can be any identifier for the dataset
    runtime_parameters={"batch_data": df},
    batch_identifiers={"default_identifier_name": "default_identifier"}
)

# Define the checkpoint configuration
checkpoint_config = {
    "name": "patients_gender_checkpoint",
    "config_version": 1.0,
    "class_name": "Checkpoint",
    "run_name_template": "%Y-%m-%d-%H-%M-%S-patients_gender-checkpoint",
    "expectation_suite_name": "patients_gender_expectations",
    "action_list": [
        {
            "name": "store_validation_result",
            "action": {
                "class_name": "StoreValidationResultAction"
            }
        },
        {
            "name": "store_evaluation_params",
            "action": {
                "class_name": "StoreEvaluationParametersAction"
            }
        },
        {
            "name": "update_data_docs",
            "action": {
                "class_name": "UpdateDataDocsAction"
            }
        }
    ]
}

# Check if the checkpoint exists and delete it
try:
    context.delete_checkpoint("patients_gender_checkpoint")
except ge.exceptions.CheckpointNotFoundError:
    pass

# Add the checkpoint configuration
context.add_checkpoint(**checkpoint_config)

# Run the checkpoint
results = context.run_checkpoint(
    checkpoint_name="patients_gender_checkpoint",
    batch_request=batch_request
)

context.build_data_docs()
context.open_data_docs()