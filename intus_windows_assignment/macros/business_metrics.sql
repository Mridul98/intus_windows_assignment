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