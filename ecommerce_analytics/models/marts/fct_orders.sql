WITH orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

customers AS (

    SELECT * FROM {{ ref('dim_customers') }}

),

products AS (

    SELECT * FROM {{ ref('dim_products') }}

),

final AS (

    SELECT
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_date,
        CASE UPPER(TRIM(o.status))
            WHEN 'COMP'      THEN 'Completed'
            WHEN 'COMPLETED' THEN 'Completed'
            WHEN 'PENDING'   THEN 'Pending'
            WHEN 'CANCELLED' THEN 'Cancelled'
            WHEN 'REFUND'    THEN 'Refunded'
            WHEN 'REFUNDED'  THEN 'Refunded'
            ELSE 'Unknown'
        END AS status,
        o.quantity,
        p.list_price AS unit_price,
        COALESCE(o.discount_pct, 0) AS discount_pct,
        CASE UPPER(TRIM(o.channel))
            WHEN 'WEB'        THEN 'Web'
            WHEN 'MOBILE'     THEN 'Mobile'
            WHEN 'MOBILE APP' THEN 'Mobile'
            ELSE 'Other'
        END AS channel,
        o.promo_code,

        -- Revenue metrics (the business logic lives HERE)
        ROUND(
            o.quantity * p.list_price * (1 - COALESCE(o.discount_pct, 0))
        , 2) AS revenue,

        ROUND(o.quantity * p.list_price, 2) AS gross_revenue,

        ROUND(
            o.quantity * p.list_price * COALESCE(o.discount_pct, 0)
        , 2) AS discount_amount,

        ROUND(o.quantity * p.cost_price, 2) AS cost,

        ROUND(
            (o.quantity * p.list_price * (1 - COALESCE(o.discount_pct, 0)))
            - (o.quantity * p.cost_price)
        , 2)  AS gross_profit,
        c.tier as customer_tier,
        p.product_name,
        p.category,
        p.brand

    FROM orders o
    LEFT JOIN customers c
        ON o.customer_id = c.customer_id
    LEFT JOIN products p
        ON o.product_id = p.product_id

)

SELECT * FROM final