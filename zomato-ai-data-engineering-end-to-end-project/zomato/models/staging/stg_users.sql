--select 
--    user_id::number as customer_id, 
--    name as customer_name, 
--    lower(email) as email,
--    try_to_number(age) as age, 
--    gender, 
--    marital_status, 
--    occupation,
--    monthly_income as income_band, 
--    education, 
--    try_to_number(family_size) as family_size
--from {{ source('raw', 'users') }} where try_to_number(user_id) is not null

with source_data as (

    select
        user_id::number as customer_id,
        name as customer_name,
        lower(email) as email,
        try_to_number(age) as age,
        gender,
        marital_status,
        occupation,
        monthly_income as income_band,
        education,
        try_to_number(family_size) as family_size,

        row_number() over (
            partition by user_id
            order by user_id
        ) as rn

    from {{ source('raw', 'users') }}

    where try_to_number(user_id) is not null

)

select
    customer_id,
    customer_name,
    email,
    age,
    gender,
    marital_status,
    occupation,
    income_band,
    education,
    family_size

from source_data

where rn = 1