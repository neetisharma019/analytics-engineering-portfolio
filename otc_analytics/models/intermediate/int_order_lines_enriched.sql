with order_lines as (
    select * from {{ ref('stg_order_lines') }}
),

sales_orders as (
    select * from {{ ref('stg_sales_orders') }}
),

shipments as (
    select * from {{ ref('stg_shipments') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

main as (
    select
        -- keys
        ol.line_id,
        ol.order_id,
        ol.product_id,

        -- order attributes
        so.customer_id,
        so.order_date,
        so.requested_ship_date,
        so.order_status,
        so.sales_rep,
        so.channel,
        so.currency,
        so.order_discount_pct,
        so.po_number,

        -- shipment attributes
        sh.shipment_id,
        sh.ship_date,
        sh.carrier,
        sh.status                                           as shipment_status,
        sh.tracking_number,
        sh.ship_to_city,
        sh.ship_to_state,
        sh.ship_to_zip,
        sh.weight_lbs,
        -- line attributes
        ol.quantity,
        ol.unit_price,
        ol.line_discount_pct,
        ol.line_total,
        ol.has_unparseable_quantity,

        -- revenue calculations
        round(ol.quantity * ol.unit_price, 2)                               as gross_revenue,

        round(
            ol.quantity * ol.unit_price * (ol.line_discount_pct / 100), 2
        )                                                                   as discount_amount,

        round(
            ol.quantity * ol.unit_price * (1 - ol.line_discount_pct / 100), 2
        )                                                                   as revenue,

        round(ol.quantity * p.cost_price, 2)                               as cost,

        round(
            ol.quantity * ol.unit_price * (1 - ol.line_discount_pct / 100)
            - ol.quantity * p.cost_price,
            2
        )                                                                   as gross_profit,

        -- delivery metrics
        case
            when sh.ship_date <= so.requested_ship_date
                then TRUE
            else FALSE
        end                                                                 as is_on_time_delivery,

        datediff('day', so.order_date, sh.ship_date)                       as days_to_ship

    from order_lines ol
    left join sales_orders so
        on ol.order_id = so.order_id
    left join shipments sh
        on ol.order_id = sh.order_id
    left join products p
        on ol.product_id = p.product_id
)

select * from main
