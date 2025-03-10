WITH aggregated_sales_data AS (

    SELECT 
        sales_transaction.transaction_year,
        sales_transaction.transaction_month,
        sales_transaction.user_id,
        product_catalog.product_category,
        COALESCE(SUM(sales_transaction.revenue),0) AS total_revenue,
        COALESCE(SUM(sales_transaction.cost),0) AS total_cost

    FROM {{ ref('stg_sales_transactions') }} AS sales_transaction
    LEFT JOIN {{ ref('stg_product_catalog') }} AS product_catalog
    ON sales_transaction.product_id = product_catalog.product_id
    GROUP BY 
        sales_transaction.transaction_year, 
        sales_transaction.transaction_month, 
        sales_transaction.user_id,
        product_catalog.product_category
    HAVING SUM(sales_transaction.revenue) > 0
),

aggregated_marketing_events_data AS (
    
    SELECT 
        transaction_year,
        transaction_month,
        user_id,
        COUNT(event_id) AS total_marketing_events
        COALESCE(SUM(cost),0) AS total_marketing_cost
    FROM {{ ref('stg_marketing_events') }}
    GROUP BY transaction_year, transaction_month, user_id
)

SELECT
    aggregated_sales_data.transaction_year,
    aggregated_sales_data.transaction_month,
    aggregated_sales_data.category,
    COUNT(DISTINCT aggregated_sales_data.user_id) AS unique_customers,
    aggregated_sales_data.total_revenue,
    aggregated_sales_data.total_cost,
    {{ revenue_to_cost_ratio(aggregated_sales_data.total_revenue, aggregated_sales_data.total_cost) }} AS revenue_to_cost_ratio,
    aggregated_marketing_events_data.total_marketing_cost,
    aggregated_marketing_events_data.total_marketing_events
FROM aggregated_sales_data 
LEFT JOIN aggregated_marketing_events_data
ON aggregated_sales_data.user_id = aggregated_marketing_events_data.user_id
aggregated_sales_data.transaction_year = aggregated_marketing_events_data.transaction_year
AND aggregated_sales_data.transaction_month = aggregated_marketing_events_data.transaction_month
ORDER BY 
    aggregated_sales_data.transaction_year, 
    aggregated_sales_data.transaction_month,
    aggregated_sales_data.category
