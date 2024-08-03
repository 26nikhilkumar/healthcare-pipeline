with source as (
    select * from {{ source('healthcare_pipeline', 'raw_conditions') }}
)

select
    datetime(START) as start,
    datetime(STOP) as stop,
    PATIENT as patient_id,
    ENCOUNTER as encounter,
    CODE as code,
    DESCRIPTION as description,
    julianday(STOP) - julianday(START) as duration
from source
