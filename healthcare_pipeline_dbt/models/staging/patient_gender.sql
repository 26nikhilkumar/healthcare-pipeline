with source as (
    select * from {{ source('healthcare_pipeline', 'raw_patient_gender') }}
)

select
    patient_id,
    gender
from source
