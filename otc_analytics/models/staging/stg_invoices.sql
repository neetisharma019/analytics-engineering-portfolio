with source as (
    select * from {{ ref('raw_invoices') }}
),

cleaned as (
    select
        invoice_id,
        order_id,
        customer_id,

        coalesce(
            try_to_date(invoice_date, 'YYYY-MM-DD'),
            try_to_date(invoice_date, 'MM/DD/YYYY')
        )                                                   as invoice_date,

        coalesce(
            try_to_date(due_date, 'YYYY-MM-DD'),
            try_to_date(due_date, 'MM/DD/YYYY')
        )                                                   as due_date,

        invoice_amount::number(14,2)                        as invoice_amount,
        tax_amount::number(14,2)                            as tax_amount,
        total_amount::number(14,2)                          as total_amount,
        upper(trim(payment_terms))                          as payment_terms,

        -- mixed casing + synonyms:
        --   'PAID'/'paid'/'Paid'           → PAID
        --   'OVERDUE'                      → OVERDUE
        --   'OUTSTANDING'/'outstanding'    → OUTSTANDING
        --   'Partial'/'partial'            → PARTIAL
        case upper(trim(payment_status))
            when 'PAID'        then 'PAID'
            when 'OVERDUE'     then 'OVERDUE'
            when 'OUTSTANDING' then 'OUTSTANDING'
            when 'PARTIAL'     then 'PARTIAL'
            else upper(trim(payment_status))
        end                                                 as payment_status,

        coalesce(days_overdue::int, 0)                      as days_overdue,
        nullif(trim(notes), '')                             as notes

    from source
)

select * from cleaned
