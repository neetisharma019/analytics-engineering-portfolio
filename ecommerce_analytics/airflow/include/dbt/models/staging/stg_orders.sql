{{ config(materialized='view') }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_orders') }}

),

renamed AS (

    SELECT
        id              AS order_id,
        cust_id         AS customer_id,
        prod_id         AS product_id,
        order_dt::DATE  AS order_date,
        CASE UPPER(TRIM(stat))
            WHEN 'COMP' THEN 'COMPLETED'
            ELSE UPPER(TRIM(stat))
        END                      AS status,
        qty::INTEGER             AS quantity,
        disc_pct::FLOAT          AS discount_pct,
        UPPER(TRIM(channel))     AS channel,
        promo_cd        AS promo_code

    FROM source
    WHERE id IS NOT NULL

)

SELECT * FROM renamed
