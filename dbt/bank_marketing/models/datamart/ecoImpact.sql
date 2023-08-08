{{config(materialized='table')}}

SELECT
    dec.EconomicContextID as EconomicImpactID,
    dec.emp_var_rate as Employment_Variation_Rate,
    dec.cons_price_idx as Consumer_Price_Index,
    dec.cons_conf_idx as Consumer_Confidence_Index,
    dec.euribor3m as Euribor_3_Month_Rate,
    dec.nr_employed as Number_Of_Employees,
    SUM(CASE WHEN ft.subcribed = 1 THEN 1 ELSE 0 END) AS subs,
    SUM(CASE WHEN ft.subcribed = 0 THEN 1 ELSE 0 END) AS not_subs,
    (dec.cons_price_idx / dec.euribor3m) as Price_Index_to_Euribor_Rate,
    (dec.emp_var_rate / dec.euribor3m) as Employment_to_Euribor_Rate
FROM {{ ref('dimEconomic') }} dec
LEFT JOIN {{ ref('fact__tables') }} ft
    ON dec.EconomicContextID= ft.economic_context
GROUP BY 1,2,3,4,5,6,9,10
ORDER BY 7 DESC