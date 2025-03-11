SELECT
    transaction_id,
    product_id,
    user_id,
    transaction_timestamp,
    COALESCE(revenue,0) AS revenue,
    COALESCE(cost,0) AS cost
    EXTRACT(YEAR FROM transaction_timestamp) AS transaction_year,
    EXTRACT(MONTH FROM s.transaction_timestamp) AS transaction_month
FROM
    {{ source('marketing_and_sales','sales_transaction') }}