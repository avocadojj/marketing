{{ config(materialized='view') }}

WITH dim__CLIENT AS (
    SELECT DISTINCT
        age,
        job,
        marital,
        education,
        credit,
        housing,
        loan
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['age', 'job', 'marital', 'education', 'credit', 'housing', 'loan']) }} as ClientID,
    age,
    job,
    marital,
    education,
    credit,
    housing,
    loan
FROM 
    dim__CLIENT
ORDER BY ClientID ASC
