{{ config(
    materialized='table',
    dist='key',
    dist_key='transaction_year_month',  -- using product_id as the dist key for efficient joins
    sort=['transaction_year_month', 'event_id']  -- compound sort key on precomputed date parts
    ) 
}}


SELECT 
    event_id, 
    user_id,
    event_type,
    event_timestamp,
    channel,
    campaign,
    COALESCE(cost,0) AS cost,
    EXTRACT(YEAR FROM event_timestamp) AS transaction_year,
    EXTRACT(MONTH FROM event_timestamp) AS transaction_month,
    TO_CHAR(event_timestamp, 'YYYY|MM') AS transaction_year_month AS transaction_year_month

FROM {{ source('marketing_and_sales','marketing_events') }}