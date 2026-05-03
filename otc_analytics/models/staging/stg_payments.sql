with source as (
    select * from {{ ref('raw_payments') }}
),

cleaned as (
    select
        payment_id,
        invoice_id,
        customer_id,

        coalesce(
            try_to_date(payment_date, 'YYYY-MM-DD'),
            try_to_date(payment_date, 'MM/DD/YYYY')
        )                                                   as payment_date,

        amount::number(14,2)                                as amount,

        -- normalize: 'Wire Transfer'/'wire transfer'/'wire' → WIRE_TRANSFER
        --            'ACH'/'ach'                            → ACH
        --            'CHECK'/'check'                        → CHECK
        case upper(trim(payment_method))
            when 'WIRE TRANSFER' then 'WIRE_TRANSFER'
            when 'WIRE'          then 'WIRE_TRANSFER'
            when 'ACH'           then 'ACH'
            when 'CHECK'         then 'CHECK'
            else nullif(upper(trim(payment_method)), '')
        end                                                 as payment_method,

        nullif(trim(reference_number), '')                  as reference_number,
        nullif(trim(bank_ref), '')                          as bank_ref,
        nullif(trim(notes), '')                             as notes,

        -- PMT-017: zero-amount duplicate; PMT-018: explicit duplicate note in source
        case
            when amount = 0 AND upper(notes) ilike '%DUPLICATE%' then true
            when upper(notes) ilike '%DUPLICATE%'    then true
            else false
        end                                                 as is_duplicate_flag

        from source
        qualify row_number() over (
            partition by invoice_id, amount::number(14,2)
            order by payment_id
        ) = 1                                                   

)

select * from cleaned
