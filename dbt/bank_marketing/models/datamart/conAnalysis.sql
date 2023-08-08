{{ config(materialized='table') }}

SELECT
    dcon.ContactID,
    dcon.contact as Contact_Type,
    dcon.month as Contact_Month,
    dcon.day_of_week as Contact_Day_of_Week,
    dcon.duration as Contact_Duration,
    SUM(ft.subcribed) as Subscribed
FROM {{ ref('dimContact') }} dcon
LEFT JOIN {{ ref('fact__tables') }} ft
    ON dcon.ContactID = ft.ContactID
GROUP BY 1,2,3,4,5