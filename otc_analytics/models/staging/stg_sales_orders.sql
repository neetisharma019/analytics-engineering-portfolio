with source as (
    select * from {{ ref('raw_sales_orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,

        -- mixed formats: 'YYYY-MM-DD' and 'MM/DD/YYYY'
        coalesce(
            try_to_date(order_date, 'YYYY-MM-DD'),
            try_to_date(order_date, 'MM/DD/YYYY')
        )                                                   as order_date,

        coalesce(
            try_to_date(requested_ship_date, 'YYYY-MM-DD'),
            try_to_date(requested_ship_date, 'MM/DD/YYYY')
        )                                                   as requested_ship_date,

        -- mixed casing: 'CONFIRMED', 'confirmed', 'Confirmed', 'shipped', 'Shipped',
        --               'Invoiced', 'INVOICED', 'CANCELLED', 'on hold'
        case upper(trim(order_status))
            when 'CONFIRMED'  then 'CONFIRMED'
            when 'SHIPPED'    then 'SHIPPED'
            when 'INVOICED'   then 'INVOICED'
            when 'CANCELLED'  then 'CANCELLED'
            when 'ON HOLD'    then 'ON_HOLD'
            else upper(trim(order_status))
        end                                                 as order_status,

        trim(sales_rep)                                     as sales_rep,
        initcap(trim(channel))                              as channel,
        upper(trim(currency))                               as currency,
        coalesce(discount_pct::number(5,2), 0)             as order_discount_pct,
        nullif(trim(po_number), '')                         as po_number,
        nullif(trim(notes), '')                             as notes

    from source
)

select * from cleaned
