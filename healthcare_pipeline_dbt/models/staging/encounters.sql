with source as (
    select * from {{ source('healthcare_pipeline', 'raw_encounters') }}
)

select
    id as encounter_id,
    datetime(start) as start,
    datetime(stop) as stop,
    patient_id,
    organization,
    provider,
    payer,
    encounter_class,
    code,
    description,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    reason_code,
    reason_description
from source
