{{ config(materialized='view') }}

WITH dim__CONTACT AS (
    SELECT DISTINCT
        contact,
        month,
        day_of_week,
        duration
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['contact', 'month', 'day_of_week', 'duration']) }} as ContactID,
    contact,
    month,
    day_of_week,
    duration
FROM 
    dim__CONTACT
ORDER BY ContactID ASC
