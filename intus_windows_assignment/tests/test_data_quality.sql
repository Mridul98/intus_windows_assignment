{% test is_positive(model,column_name) %}

    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} < 0

{% endtest %}

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