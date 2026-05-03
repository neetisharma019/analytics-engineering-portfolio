with source as (
    select * from {{ ref('raw_products') }}
),

cleaned as (
    select
        product_id,

        -- inconsistent casing: "hardware server unit", "PROFESSIONAL SERVICES BUNDLE"
        initcap(trim(product_name))                         as product_name,

        initcap(trim(category))                             as category,
        upper(trim(sku))                                    as sku,

        -- PROD-008 (Consulting Hours) has null unit_price; cost_price is the floor
        unit_price::number(12,2)                            as unit_price,
        cost_price::number(12,2)                            as cost_price,
        upper(trim(unit_of_measure))                        as unit_of_measure,
        -- mixed casing: 'ACTIVE', 'active', 'Active'
        upper(trim(status))                                 as status,
        lead_time_days::int                                 as lead_time_days,
        created_date::date                                  as created_date

    from source
)

select * from cleaned
