{{
    config(
        materialized = 'table'
    )
}}

with order_lines as (
    select * from {{ ref('int_order_lines_enriched') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        -- surrogate keys
        ol.line_id,
        ol.order_id,
        ol.product_id,
        ol.customer_id,
        ol.shipment_id,

        -- dates
        ol.order_date,
        ol.requested_ship_date,
        ol.ship_date,

        -- order attributes
        ol.order_status,
        ol.sales_rep,
        ol.channel,
        ol.currency,
        ol.po_number,

        -- customer attributes
        dc.company_name,
        dc.industry,
        dc.city                                             as customer_city,
        dc.state                                            as customer_state,
        dc.country                                          as customer_country,

        -- region derived from customer billing state
        case
            when dc.state in ('CT','ME','MA','NH','NJ','NY','PA','RI','VT')
                then 'Northeast'
            when dc.state in ('AL','AR','DE','DC','FL','GA','KY','LA','MD',
                              'MS','NC','OK','SC','TN','TX','VA','WV')
                then 'Southeast'
            when dc.state in ('IL','IN','IA','KS','MI','MN','MO','NE',
                              'ND','OH','SD','WI')
                then 'Midwest'
            when dc.state in ('AK','AZ','CA','CO','HI','ID','MT','NV',
                              'NM','OR','UT','WA','WY')
                then 'West'
            else 'Other'
        end                                                 as region,

        -- product attributes
        dp.product_name,
        dp.category,
        dp.sku,
        dp.unit_of_measure,
        dp.gross_margin_pct,

        -- shipment attributes
        ol.carrier,
        ol.shipment_status,
        ol.ship_to_city,
        ol.ship_to_state,

        -- line detail
        ol.quantity,
        ol.unit_price,
        ol.line_discount_pct,

        -- revenue metrics
        ol.gross_revenue,
        ol.discount_amount,
        ol.revenue,
        ol.cost,
        ol.gross_profit,

        -- delivery metrics
        ol.is_on_time_delivery,
        ol.days_to_ship,

        -- data quality flag
        ol.has_unparseable_quantity

    from order_lines ol
    left join dim_customers dc
        on ol.customer_id = dc.customer_id
    left join dim_products dp
        on ol.product_id = dp.product_id
)

select * from final
