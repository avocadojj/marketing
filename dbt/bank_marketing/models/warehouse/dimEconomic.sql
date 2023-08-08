{{ config(materialized='view') }}

WITH dim__ECONOMICCONTEXT AS (
    SELECT DISTINCT
        emp_var_rate,
        cons_price_idx,
        cons_conf_idx,
        euribor3m,
        nr_employed
    FROM {{ ref('stg__marketing') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['emp_var_rate', 'cons_price_idx', 'cons_conf_idx', 'euribor3m', 'nr_employed']) }} as EconomicContextID,
    emp_var_rate,
    cons_price_idx,
    cons_conf_idx,
    euribor3m,
    nr_employed
FROM 
    dim__ECONOMICCONTEXT
ORDER BY EconomicContextID ASC
