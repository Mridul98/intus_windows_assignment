WITH aggregated_marketing_data AS (
    SELECT
        transaction_year_month,
        COUNT(event_id) AS total_marketing_events,
        SUM(cost) AS total_marketing_cost
    FROM {{ ref('stg_marketing_events') }}
    GROUP BY transaction_year_month
),

aggregated_sales_data AS (
    SELECT
        sales.transaction_year_month,
        product_catalog.category,
        COUNT(DISTINCT sales.user_id) AS unique_customers,
        SUM(sales.revenue) AS total_revenue,
        SUM(sales.cost) AS total_cost,
    FROM {{ ref('stg_sales_transactions') }} AS sales
    LEFT JOIN {{ ref('stg_product_catalog') }} AS product_catalog 
        ON sales.product_id = product_catalog.product_id
    GROUP BY 
        sales.transaction_year_month,
        product_catalog.category
),
final_result (
    SELECT 
        SPLIT_PART(sales.transaction_year_month, '|', 1)::INT AS transaction_year,
        SPLIT_PART(sales.transaction_year_month, '|', 2)::INT AS transaction_month,
        sales.category,
        sales.unique_customers,
        sales.total_revenue,
        sales.total_cost,
        marketing.total_marketing_cost,
        marketing.total_marketing_events,
        {{ revenue_to_cost_ratio(sales.total_revenue,sales.total_cost) }} AS revenue_to_cost_ratio
    FROM aggregated_sales_data AS sales 
    LEFT JOIN aggregated_marketing_data as marketing 
    ON sales.transaction_year_month = marketing.transaction_year_month
    WHERE sales.total_revenue > 0 
)

SELECT 
    transaction_year,
    transaction_month,
    category,
    unique_customers,
    total_revenue,
    total_cost,
    total_marketing_cost,
    total_marketing_events,
    revenue_to_cost_ratio
FROM final_result 
ORDER BY 
    transaction_year,
    transaction_month,
    category

