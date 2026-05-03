with source as (
    select * from {{ ref('raw_shipments') }}
),

cleaned as (
    select
        shipment_id,
        order_id,

        ship_date::date                                     as ship_date,

        -- normalize: FedEx/FEDEX/fedex → FedEx
        --            FedEx Freight/FEDEX FREIGHT → FedEx Freight
        --            UPS/ups → UPS
        --            USPS → USPS
        --            freight co. → Other
        case
            when upper(trim(carrier)) in ('FEDEX FREIGHT', 'FEDEX FREIGHT INC')
                then 'FedEx Freight'
            when upper(trim(carrier)) like 'FEDEX%'
                then 'FedEx'
            when upper(trim(carrier)) = 'UPS'
                then 'UPS'
            when upper(trim(carrier)) = 'USPS'
                then 'USPS'
            else 'Other'
        end                                                 as carrier,

        -- preserve raw value for auditing
        trim(carrier)                                       as carrier_raw,

        nullif(trim(tracking_number), '')                   as tracking_number,
        initcap(trim(ship_to_name))                         as ship_to_name,
        trim(ship_to_address)                               as ship_to_address,
        initcap(trim(ship_to_city))                         as ship_to_city,
        upper(trim(ship_to_state))                          as ship_to_state,
        trim(ship_to_zip)                                   as ship_to_zip,

        -- mixed casing: 'DELIVERED', 'delivered', 'Delivered', 'In Transit', 'PENDING'
        upper(trim(status))                                 as status,

        nullif(weight_lbs::number(8,2), 0)                 as weight_lbs,
        nullif(trim(notes), '')                             as notes

    from source
)

select * from cleaned
