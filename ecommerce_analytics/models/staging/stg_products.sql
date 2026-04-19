{{ config(materialized='view') }}

WITH source AS (

    SELECT * FROM {{ source('main_raw', 'raw_products') }}

),

renamed AS (

    SELECT
        id                          AS product_id,
        TRIM(prod_nm)               AS product_name,
        UPPER(TRIM(ctgry))          AS category,
        UPPER(TRIM(brnd))           AS brand,
        lst_price::FLOAT            AS list_price,
        cst_price::FLOAT            AS cost_price,
        is_active::BOOLEAN          AS is_active,
        launch_dt::DATE             AS launch_date

    FROM source
    WHERE id IS NOT NULL

)

SELECT * FROM renamed
