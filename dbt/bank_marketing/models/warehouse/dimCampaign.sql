{{ config(materialized='view') }}

WITH dim__CAMPAIGN AS (
    SELECT DISTINCT
        campaign,
        pdays,
        previous,
        poutcome
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['campaign', 'pdays', 'previous', 'poutcome']) }} as CampaignID,
    campaign,
    pdays,
    previous,
    poutcome
FROM 
    dim__CAMPAIGN
ORDER BY CampaignID ASC
