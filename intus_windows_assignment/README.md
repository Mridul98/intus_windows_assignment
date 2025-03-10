### Intus Windows dbt assignment

## The task is to refactor this query into DBT models as well as implement data quality checks
```SQL 
SELECT
EXTRACT(YEAR FROM s.transaction_timestamp) AS transaction_year,
EXTRACT(MONTH FROM s.transaction_timestamp) AS
transaction_month,
p_cat.category,
COUNT(DISTINCT s.user_id) AS unique_customers,
SUM(s.revenue) AS total_revenue,
SUM(s.cost) AS total_cost,
SUM(
SELECT SUM(me.cost)
FROM marketing_events me

WHERE EXTRACT(YEAR FROM me.event_timestamp) = EXTRACT(YEAR
FROM s.transaction_timestamp)
AND EXTRACT(MONTH FROM me.event_timestamp) = EXTRACT(MONTH
FROM s.transaction_timestamp)
) AS total_marketing_cost,
COUNT(me.event_id) AS total_marketing_events,
-- Calculating the ratio of total revenue to total cost
CASE
WHEN SUM(s.cost) > 0 THEN (SUM(s.revenue) / SUM(s.cost))
ELSE 0
END AS revenue_to_cost_ratio
FROM
sales_transactions s
LEFT JOIN product_catalog p_cat ON s.product_id = p_cat.product_id
LEFT JOIN marketing_events me ON s.user_id = me.user_id
AND EXTRACT(YEAR FROM s.transaction_timestamp) = EXTRACT(YEAR FROM
me.event_timestamp)
AND EXTRACT(MONTH FROM s.transaction_timestamp) = EXTRACT(MONTH FROM
me.event_timestamp)
GROUP BY
transaction_year, transaction_month, p_cat.category
HAVING
total_revenue > 0
ORDER BY transaction_year, transaction_month, p_cat.category;

```

## Solution:

### project structure:

```SHELL    
intus_windows_assignment/
├── README.md
├── analyses
├── dbt_project.yml
├── macros
│   └── business_metrics.sql   # this file contains necessary macro for revenue to cost ratio calculation
├── models
│   ├── reporting
│   │   ├── business_performance_report.sql # this model is the final output
│   │   └── schema.yaml # this yaml file contains the column descriptions of business_performance_metrics model
│   └── staging
│       ├── schema.yaml
│       ├── sources.yaml
│       ├── stg_marketing_events.sql    # This is the staging models for marketing events data.
│       ├── stg_product_catalog.sql     # This is the staging models for product catalog
│       └── stg_sales_transactions.sql  # This is the staging models for sales transaction
├── seeds
├── snapshots
└── tests
    └── test_data_quality.sql   # It contains test that ensures a column contains all positive values

```
