{{ config(
    materialized='table',
    dist='all',
    sort=['product_id','category']
    ) 
}}

SELECT 
    product_id,
    product_name,
    COALESCE(category,'missing_category')
FROM {{ source('marketing_and_sales','product_catalog') }}
    