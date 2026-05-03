{{
    config(
        materialized = 'table'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        product_id,
        product_name,
        category,
        sku,
        unit_price,
        cost_price,
        unit_of_measure,
        status,
        lead_time_days,
        created_date,

        -- null-guarded: unit_price is null for consulting lines (PROD-008)
        case
            when unit_price is not null and unit_price != 0
                then round((unit_price - cost_price) / unit_price * 100, 2)
            else null
        end                                                 as gross_margin_pct

    from products
)

select * from final
