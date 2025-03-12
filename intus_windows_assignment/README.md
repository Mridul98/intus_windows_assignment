### Intus Windows dbt assignment

## The task is to refactor this query into DBT models as well as implement data quality checks
```SQL 
SELECT    Extract(year FROM s.transaction_timestamp)  AS transaction_year,
          Extract(month FROM s.transaction_timestamp) AS transaction_month,
          p_cat.category,
          Count(DISTINCT s.user_id) AS unique_customers,
          Sum(s.revenue)            AS total_revenue,
          Sum(s.cost)               AS total_cost,
          sum
              (
              select sum(me.cost)
              FROM   marketing_events me
              WHERE  extract(year FROM me.event_timestamp) = extract(year FROM s.transaction_timestamp)
              AND    extract(month FROM me.event_timestamp) = extract(month FROM s.transaction_timestamp) ) AS total_marketing_cost,
          count(me.event_id)                                                                                AS total_marketing_events,
          -- Calculating the ratio of total revenue to total cost
          CASE
                    WHEN sum(s.cost) > 0 THEN (sum(s.revenue) / sum(s.cost))
                    ELSE 0
          END AS revenue_to_cost_ratio
FROM      sales_transactions s
LEFT JOIN product_catalog p_cat
ON        s.product_id = p_cat.product_id
LEFT JOIN marketing_events me
ON        s.user_id = me.user_id
AND       extract(year FROM s.transaction_timestamp) = extract(year FROM me.event_timestamp)
AND       extract(month FROM s.transaction_timestamp) = extract(month FROM me.event_timestamp)
GROUP BY  transaction_year,
          transaction_month,
          p_cat.category
HAVING    total_revenue > 0
ORDER BY  transaction_year,
          transaction_month,
          p_cat.category;

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
# Staging table descriptions:

## stg_marketing_events: 
 This is the table that holds preprocessed marketing events related data. For redshift specific optimization, I have distributed the data across the redshift cluster based on concatenated event year and month. And I have used compound sorting based on concatenated event year and month as well as event id. 

```SQL 
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
```

### Reason behind chosing this sort key and dist key: 
Since the data from this table will be joined based on transaction year and month, it is better to define both sort key and dist key on transaction year and month combined. This will enable redshift to colocate the data based on that key.

## stg_product_catalog:
This is the table that holds preprocessed product catalog. The data is copied across all node of redshift cluster and applied sort key on product id. Here, I assume that product category sometimes will be missing, so I have imputed the missing product category with missing category.

```SQL 
{{ config(
    materialized='table',
    dist='all',
    dist_key='product_id',
    sort=['product_id','category']
    ) 
}}

SELECT 
    product_id,
    product_name,
    COALESCE(category,'missing_category')
FROM {{ source('marketing_and_sales','product_catalog') }}
    
```

### Reason behind chosing this sort key and dist key: 
The data is copied across the cluster based on the strong assumption that, this table is relatively small and doesnt get updated frequently because this table is related to products and it is less dynamic compared to the other tables that we have in the assignment. Also the join will be faster with the other table since all the redshift node will have the data readily available without any shuffling.


## stg_sales_transactions:

This table contains preprocess sales transaction data.

Like marketing events, this table data needs to be distributed based on combined transaction year and month. Because, according to the context, the join will be based on transaction year and month more frequently. The table data is sorted based on combined transaction year and month as well as product id.


```SQL 
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
```

### Reason behind chosing this sort key and dist key: 
Like marketing events, this table data needs to be distributed based on combined transaction year and month. Because, according to the context, the join will be based on transaction year and month more frequently. The table data is sorted based on combined transaction year and month as well as product id.


## business_performance_report:

```SQL 
WITH aggregated_marketing_data AS (
    SELECT
        transaction_year,
        transaction_month,
        COUNT(event_id) AS total_marketing_events,
        SUM(cost) AS total_marketing_cost
    FROM {{ ref('stg_marketing_events') }}
    GROUP BY transaction_year, transaction_month
),

aggregated_sales_data AS (
    SELECT
        sales.transaction_year,
        sales.transaction_month,
        product_catalog.category,
        COUNT(DISTINCT sales.user_id) AS unique_customers,
        SUM(sales.revenue) AS total_revenue,
        SUM(sales.cost) AS total_cost,
    FROM {{ ref('stg_sales_transactions') }} AS sales
    LEFT JOIN {{ ref('stg_product_catalog') }} AS product_catalog 
        ON sales.product_id = product_catalog.product_id
    GROUP BY 
        sales.transaction_year, 
        sales.transaction_month, 
        product_catalog.category
    ORDER BY 
        sales.transaction_year, 
        sales.transaction_month, 
        product_catalog.category
),

SELECT 
    sales.transaction_year,
    sales.transaction_month,
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
ORDER BY 
    sales.transaction_year,
    sales.transaction_month,
    sales.category
```

## SQL optimization explanation: 

Since theres a subquery that sums marketing cost by joining with user id of sales and summing it again in the outer query, this query signifies that the total marketing cost is attributed for specific year and month, not for any specific user. Because the query is ultimately grouping by transaction year and month at the end.

So I have discarded the subquery and calculated the total marketing cost and other things in seperate CTE:

```SQL
WITH aggregated_marketing_data AS (
    SELECT
        transaction_year,
        transaction_month,
        COUNT(event_id) AS total_marketing_events,
        SUM(cost) AS total_marketing_cost
    FROM {{ ref('stg_marketing_events') }}
    GROUP BY transaction_year, transaction_month
),

```

The rest of the sales metrics aggregations are being done in this CTE:

```SQL 
aggregated_sales_data AS (
    SELECT
        sales.transaction_year,
        sales.transaction_month,
        product_catalog.category,
        COUNT(DISTINCT sales.user_id) AS unique_customers,
        SUM(sales.revenue) AS total_revenue,
        SUM(sales.cost) AS total_cost,
    FROM {{ ref('stg_sales_transactions') }} AS sales
    LEFT JOIN {{ ref('stg_product_catalog') }} AS product_catalog 
        ON sales.product_id = product_catalog.product_id
    GROUP BY 
        sales.transaction_year, 
        sales.transaction_month, 
        product_catalog.category
    ORDER BY 
        sales.transaction_year, 
        sales.transaction_month, 
        product_catalog.category
),
```

Finally we join both aggregated data into a single query by doing this: 

```SQL 
SELECT 
    sales.transaction_year,
    sales.transaction_month,
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
ORDER BY 
    sales.transaction_year,
    sales.transaction_month,
    sales.category


```
### notice that I have used a macro for calculating revenue to cost ratio by taking total revenue and total cost in the above query. I have also joined the data based on combined transaction year month column based on which the data is colocated and distributed across redshift cluster.

## MACROS: 

### For calculating revenue to cost ratio: 

```SQL 
{% macro revenue_to_cost_ratio(total_revenue, total_cost) %}

-- calculate revenue to cost ratio using total revenue and total cost
-- parameters:
--      total_revenue: total revenue amount 
--      total_cost: total cost amount. If the cost equals zero then this macro will return zero

    CASE
        WHEN {{ total_cost }} > 0 THEN ({{ total_revenue }} / {{ total_cost }})
        ELSE 0
    END
    
{% endmacro %}
```

## Data quality checks:

### To check whether we dont have any non-negative numbers in the business metrics, I have created a test that checks any non-negative numbers due to calculation mishap
```SQL 
{% test is_positive(model,column_name) %}

    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} < 0

{% endtest %}
```

### I have also implemented another singular DBT data test that detects any calculation related mishap / anomalies as well
```SQL 

{% test check_valid_revenue_to_cost_ratio() %}
    -- this test determines  theres is no positive revenue to cost ratio given that total revenue
    -- or / and total cost is zero
    SELECT
        revenue_to_cost_ratio
    FROM {{ ref('business_performance_report') }}
    WHERE (total_revenue = 0 OR total_cost = 0)
    AND revenue_to_cost_ratio > 0
    UNION ALL
    SELECT 
        revenue_to_cost_ratio
    FROM {{ ref('business_performance_report') }}
    WHERE (total_revenue = 0 AND total_cost = 0)
    AND revenue_to_cost_ratio > 0

{% endtest %}
```

# The DBT code can be orchestrated by running this command: ``` dbt build --select +business_performance_report```

# Note: To get to know the tests that I have written for each column of the DBT models, please have a look into the schema.yaml for each layers.