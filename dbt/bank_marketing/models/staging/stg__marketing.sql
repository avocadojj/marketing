{{ config(materialized='view') }}

WITH stg__marketing AS (
    SELECT DISTINCT
    client_id,
    age,
    job,
    marital,
    education,
    credit,
    housing,
    loan,
    contact,
    month,
    day_of_week,
    duration,
    campaign,
    pdays,
    previous,
    poutcome,
    emp_var_rate,
    cons_price_idx,
    cons_conf_idx,
    euribor3m,
    nr_employed,
    subcribed,
    date
    FROM {{ source('final_project', 'marketing') }}
)

SELECT     
    client_id,
    age,
    {{ encode_JOB('job') }} as job,
    {{ encode_MARITAL('marital') }} as marital,
    {{ encode_EDUCATION('education') }} as education,
    {{ encode_CREDIT('credit') }} as credit,
    {{ encode_HOUSING('housing') }} as housing,
    {{ encode_LOAN('loan') }} as loan,
    {{ encode_CONTACT('contact') }} as contact,
    month,
    day_of_week,
    duration,
    campaign,
    pdays,
    previous,
    {{ encode_POUTCOME('poutcome') }} as poutcome,
    emp_var_rate,
    cons_price_idx,
    cons_conf_idx,
    euribor3m,
    nr_employed,
    subcribed,
    date
FROM stg__marketing