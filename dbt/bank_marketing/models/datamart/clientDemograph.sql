{{ config(materialized='table') }}

SELECT
    dc.ClientID as ClientID,
    dc.age,
    dc.job,
    dc.marital as Marital_Status,
    dc.education as Education_Level,
    dc.credit as Has_Credit_Default,
    dc.housing as Has_Housing_Loan,
    dc.loan as Has_Personal_Loan,
    COUNT(ft.ContactID) as Total_Contacts,
    SUM(ft.subcribed) as Subscribed
FROM {{ ref('dimClient') }} dc
LEFT JOIN {{ ref('fact__tables') }} ft
    ON dc.ClientID = ft.ClientID
GROUP BY 1,2,3,4,5,6,7,8
