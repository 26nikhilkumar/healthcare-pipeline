with source as (
    select * from {{ source('healthcare_pipeline', 'raw_patients') }}
)

select
    patient_id,
    first_name,
    last_name,
    sex,
    race,
    datetime(birth_date) as birth_date,
    datetime(death_date) as death_date,
    address,
    city,
    state,
    zip_code,
    birthplace
from source
