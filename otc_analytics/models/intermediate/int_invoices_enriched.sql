with invoices as (
    select * from {{ ref('stg_invoices') }}
),

sales_orders as (
    select * from {{ ref('stg_sales_orders') }}
),

-- aggregate non-duplicate payments to a single row per invoice
payment_totals as (
    select
        invoice_id,
        sum(amount)                                         as amount_paid,
        count(*)                                            as payment_count,
        max(payment_date)                                   as last_payment_date
    from {{ ref('stg_payments') }}
    --where not is_duplicate_flag
    group by invoice_id
),

main as (
    select
        -- keys
        inv.invoice_id,
        inv.order_id,
        inv.customer_id,

        -- order attributes
        so.order_date,
        so.sales_rep,
        so.channel,
        so.currency,
        so.order_status,
        so.po_number,

        -- invoice attributes
        inv.invoice_date,
        inv.due_date,
        inv.invoice_amount,
        inv.tax_amount,
        inv.total_amount,
        inv.payment_terms,
        inv.payment_status,
        inv.notes,

        -- payment rollup
        coalesce(pt.amount_paid, 0)                         as amount_paid,
        coalesce(pt.payment_count, 0)                       as payment_count,
        pt.last_payment_date,

        -- derived financials
        inv.invoice_amount - coalesce(pt.amount_paid, 0) as outstanding_amount,

        -- invoice aging
        datediff('day', inv.invoice_date, inv.due_date)     as days_to_due,

        case
            when inv.payment_status != 'PAID'
                then datediff('day', inv.invoice_date, current_date())
            else null
        end                                                 as days_outstanding,

        -- overdue flag: past due date and not fully paid
        case
            when inv.due_date < current_date()
                 and inv.payment_status != 'PAID'
                then true
            else false
        end                                                 as is_overdue

    from invoices inv
    left join sales_orders so
        on inv.order_id = so.order_id
    left join payment_totals pt
        on inv.invoice_id = pt.invoice_id
)

select * from main
