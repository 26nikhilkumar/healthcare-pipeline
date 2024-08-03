with source as (
    select * from {{ source('healthcare_pipeline', 'raw_medications') }}
)

select
    datetime(start) as start,
    datetime(stop) as stop,
    patient_id,
    payer,
    encounter,
    code,
    description,
    base_cost,
    payer_coverage,
    dispenses,
    total_cost,
    reason_code,
    reason_description
from source
