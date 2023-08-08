{{ config(materialized='table') }}

SELECT
    dca.CampaignID,
    COUNT(ft.client_id) as Current_Campaign_Contacts,
    MAX(ft.pdays) as Days_Since_Last_Contact, -- Assuming pdays captures this info
    SUM(ft.previous) as Previous_Campaign_Contacts,
    dca.poutcome as Previous_Campaign_Outcome,
    CASE WHEN SUM(ft.subcribed) > 0 THEN 'Subscribed' ELSE 'Not Subscribed' END as Current_Campaign_Outcome
FROM {{ ref('dimCampaign') }} dca
LEFT JOIN {{ ref('stg__marketing') }} ft
    ON dca.CampaignID = CampaignID
GROUP BY 1,5