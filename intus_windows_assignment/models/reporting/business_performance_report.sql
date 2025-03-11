WITH marketing_summary AS (
    SELECT
        transaction_year,
        transaction_month,
        user_id,
        SUM(cost) AS marketing_cost,
        COUNT(event_id) AS marketing_events
    FROM {{ ref('stg_marketing_events') }}
    GROUP BY transaction_year, transaction_month, user_id
),

aggregated_sales_data AS (
    SELECT
        sales.transaction_year,
        sales.transaction_month,
        product_catalog.category,
        COUNT(DISTINCT sales.user_id) AS unique_customers,
        SUM(sales.revenue) AS total_revenue,
        SUM(sales.cost) AS total_cost,
        SUM(marketing.marketing_cost) AS total_marketing_cost,
        SUM(marketing.marketing_events) AS total_marketing_events
    FROM {{ ref('stg_sales_transactions') }} AS sales
    LEFT JOIN {{ ref('stg_product_catalog') }} AS product_category 
        ON sales.product_id = product_category.product_id
    LEFT JOIN marketing_summary AS marketing
        ON sales.user_id = marketing.user_id
        AND sales.transaction_yea = marketing.transaction_year
        AND sales.transaction_month = marketing.transaction_month
    GROUP BY 
        sales.transaction_year, 
        sales.transaction_month, 
        product_catalog.category
    ORDER BY 
        sales.transaction_year, 
        sales.transaction_month, 
        product_catalog.category
)

SELECT
    transaction_year,
    transaction_month,
    category,
    unique_customers,
    total_revenue,
    total_cost,
    {{ revenue_to_cost_ratio(total_revenue, total_cost) }} AS revenue_to_cost_ratio
    total_marketing_cost,
    total_marketing_events
FROM aggregated_sales_data
WHERE total_revenue > 0