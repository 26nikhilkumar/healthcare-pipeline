with source as (
    select * from {{ source('healthcare_pipeline', 'raw_symptoms') }}
)

select
    patient_id,
    symptom,
    frequency
from source
