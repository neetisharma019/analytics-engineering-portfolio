with source as (
    select * from {{ ref('raw_order_lines') }}
),

cleaned as (
    select
        line_id,
        order_id,
        product_id,

        -- string quantities: 'two' (OL-007), 'three' (OL-029); rest are numeric strings
        case upper(trim(quantity))
            when 'ONE'   then 1
            when 'TWO'   then 2
            when 'THREE' then 3
            when 'FOUR'  then 4
            when 'FIVE'  then 5
            else try_to_number(trim(quantity))
        end                                                 as quantity,

        -- consulting lines (OL-022, OL-034, OL-048) have null unit_price in source
        unit_price::number(12,2)                            as unit_price,
        coalesce(line_discount_pct::number(5,2), 0)        as line_discount_pct,

        -- recalculate line_total when missing; trust source value when present
        coalesce(
            line_total::number(14,2),
            case
                when unit_price is not null
                    then round(
                        case upper(trim(quantity))
                            when 'ONE'   then 1
                            when 'TWO'   then 2
                            when 'THREE' then 3
                            when 'FOUR'  then 4
                            when 'FIVE'  then 5
                            else try_to_number(trim(quantity))
                        end
                        * unit_price::number(12,2)
                        * (1 - coalesce(line_discount_pct::number(5,2), 0) / 100),
                    2)
            end
        )                                                   as line_total,

        -- flag rows where quantity or unit_price could not be resolved
        case
            when case upper(trim(quantity))
                     when 'ONE' then 1 when 'TWO' then 2 when 'THREE' then 3
                     when 'FOUR' then 4 when 'FIVE' then 5
                     else try_to_number(trim(quantity))
                 end is null                                then true
            else false
        end                                                 as has_unparseable_quantity,

        nullif(trim(notes), '')                             as notes

    from source
)

select * from cleaned
