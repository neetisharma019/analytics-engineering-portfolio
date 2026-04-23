{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='fail'
    )
}}

WITH enriched_orders AS (

    SELECT * FROM {{ ref('int_orders_enriched') }}
    {{ incremental_filter('order_date') }}

),

customers AS (

    SELECT * FROM {{ ref('dim_customers') }}

),

final AS (

    SELECT
        e.order_id,
        e.customer_id,
        e.product_id,
        e.order_date,
        e.status,
        e.quantity,
        e.unit_price,
        e.discount_pct,
        e.channel,
        e.promo_code,

        -- Revenue metrics (the business logic lives HERE)
        e.revenue,
        e.gross_revenue,
        e.discount_amount,
        e.cost,
        e.gross_profit,
        c.tier as customer_tier,
        e.product_name,
        e.category,
        e.brand,
        CURRENT_TIMESTAMP AS dbt_updated_at

    FROM enriched_orders e
    LEFT JOIN customers c
        ON e.customer_id = c.customer_id

)

SELECT * FROM final