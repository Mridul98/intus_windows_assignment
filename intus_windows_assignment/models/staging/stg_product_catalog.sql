SELECT 
    product_id,
    product_name,
    category
FROM {{ source('marketing_and_sales','product_catalog') }}
    