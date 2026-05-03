{{
    config(
        materialized = 'table'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

-- one row per customer: earliest order date
first_orders as (
    select
        customer_id,
        min(order_date)     as first_order_date
    from {{ ref('stg_sales_orders') }}
    group by customer_id
),

final as (
    select
        -- keys
        c.customer_id,

        -- company & contact
        c.company_name,
        c.industry,
        c.contact_name,
        c.email,
        c.phone,

        -- address
        c.billing_address,
        c.city,
        c.state,
        c.country,

        -- commercial
        c.payment_terms,
        c.credit_limit,
        c.status,

        -- dates
        c.created_date,
        fo.first_order_date,

        -- derived
        datediff('day', fo.first_order_date, current_date())    as days_since_first_order

    from customers c
    left join first_orders fo
        on c.customer_id = fo.customer_id
)

select * from final
