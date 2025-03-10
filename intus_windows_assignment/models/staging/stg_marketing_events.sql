{{ config(
    materialized='table',
    dist='key',
    dist_key='product_id',  -- using product_id as the dist key for efficient joins
    sort=['transaction_year', 'transaction_month', 'product_id']  -- compound sort key on precomputed date parts
    ) 
}}


SELECT 
    event_id, 
    user_id,
    event_type,
    event_timestamp,
    channel,
    campaign,
    cost,
    EXTRACT(YEAR FROM event_timestamp) AS transaction_year,
    EXTRACT(MONTH FROM event_timestamp) AS transaction_month
FROM {{ source('marketing_and_sales','marketing_events') }}