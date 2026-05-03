{{
    config(
        materialized = 'table'
    )
}}

with invoices as (
    select * from {{ ref('int_invoices_enriched') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        -- keys
        inv.invoice_id,
        inv.order_id,
        inv.customer_id,

        -- customer attributes
        dc.company_name,
        dc.industry,
        dc.state                                            as customer_state,
        dc.country                                          as customer_country,

        -- order context
        inv.order_date,
        inv.sales_rep,
        inv.channel,
        inv.currency,
        inv.order_status,
        inv.po_number,

        -- invoice attributes
        inv.invoice_date,
        inv.due_date,
        inv.payment_terms,
        inv.payment_status                                  as invoice_status,
        inv.tax_amount,
        inv.total_amount,
        inv.last_payment_date,
        inv.payment_count,

        -- financial metrics
        inv.invoice_amount,
        inv.amount_paid,
        inv.outstanding_amount,

        -- aging metrics
        inv.days_to_due,
        inv.days_outstanding,
        inv.is_overdue

    from invoices inv
    left join dim_customers dc
        on inv.customer_id = dc.customer_id
)

select * from final
