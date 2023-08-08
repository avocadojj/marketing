{{ config(
    materialized = 'view',
    partition_by = {
      "field": "date",
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by = 'day_of_week'
)}}

WITH fact__tables AS (
    SELECT *
    FROM {{ ref('stg__marketing') }}
)

SELECT
    ft.client_id as id,
    dc.ClientID as ClientID,
    dcon.ContactID as ContactID,
    dca.CampaignID as CampaignID,
    dec.EconomicContextID as economic_context,
    ft.subcribed,
    ft.date
FROM
    fact__tables ft
    LEFT JOIN {{ ref('dimClient') }} dc 
        ON ft.age = dc.age 
        AND ft.job = dc.job
        AND ft.marital = dc.marital
        AND ft.education = dc.education 
        AND ft.credit = dc.credit 
        AND ft.housing = dc.housing 
        AND ft.loan = dc.loan
    LEFT JOIN {{ ref('dimContact') }} dcon 
        ON ft.contact = dcon.contact 
        AND ft.month = dcon.month 
        AND ft.day_of_week =dcon.day_of_week 
        AND ft.duration = dcon.duration
    LEFT JOIN {{ ref('dimCampaign') }} dca 
        ON ft.campaign = dca.campaign 
        AND ft.pdays = dca.pdays 
        AND ft.previous = dca.previous 
        AND ft.poutcome = dca.poutcome
    LEFT JOIN {{ ref('dimEconomic') }} dec 
        ON ft.emp_var_rate =dec.emp_var_rate
        AND ft.cons_price_idx =dec.cons_price_idx
        AND ft.cons_conf_idx = dec.cons_conf_idx
        AND ft.euribor3m = dec.euribor3m
        AND ft.nr_employed = dec.nr_employed
ORDER BY ft.date ASC