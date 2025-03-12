{{ config(
    materialized='table',
    dist='key',
    dist_key='transaction_year_month',
    sort=['transaction_year_month', 'product_id']
    ) 
}}


SELECT
    transaction_id,
    product_id,
    user_id,
    transaction_timestamp,
    COALESCE(revenue,0) AS revenue,
    COALESCE(cost,0) AS cost
    EXTRACT(YEAR FROM transaction_timestamp) AS transaction_year,
    EXTRACT(MONTH FROM transaction_timestamp) AS transaction_month,
    TO_CHAR(transaction_timestamp, 'YYYY|MM') AS transaction_year_month

FROM
    {{ source('marketing_and_sales','sales_transaction') }}