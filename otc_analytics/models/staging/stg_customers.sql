with source as (
    select * from {{ ref('raw_customers') }}
),

cleaned as (
    select
        customer_id,

        -- inconsistent casing: "medsupply llc", "COASTAL RETAIL GROUP", etc.
        initcap(trim(company_name))                         as company_name,

        initcap(trim(industry))                             as industry,
        initcap(trim(contact_name))                         as contact_name,
        lower(trim(email))                                  as email,
        nullif(trim(phone), '')                             as phone,
        trim(billing_address)                               as billing_address,
        initcap(trim(city))                                 as city,
        upper(trim(state))                                  as state,

        -- normalize: 'US', 'USA', 'United States' → 'US'
        case upper(trim(country))
            when 'USA'           then 'US'
            when 'UNITED STATES' then 'US'
            else upper(trim(country))
        end                                                 as country,

        upper(trim(payment_terms))                          as payment_terms,
        credit_limit::number(12,2)                          as credit_limit,

        -- mixed casing: 'ACTIVE', 'active', 'Active', 'INACTIVE'
        upper(trim(status))                                 as status,

        -- mixed formats: 'YYYY-MM-DD' and 'MM/DD/YYYY'
        coalesce(
            try_to_date(created_date, 'YYYY-MM-DD'),
            try_to_date(created_date, 'MM/DD/YYYY')
        )                                                   as created_date

    from source
)

select * from cleaned
