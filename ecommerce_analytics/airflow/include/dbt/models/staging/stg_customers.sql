{{ config(materialized='view') }}

WITH source AS (

    SELECT * FROM {{ source('raw', 'raw_customers') }}

),

renamed AS (

    SELECT
        id                          AS customer_id,
        UPPER(LEFT(TRIM(first_nm), 1)) || LOWER(SUBSTR(TRIM(first_nm), 2))  AS first_name,
        UPPER(LEFT(TRIM(last_nm), 1))  || LOWER(SUBSTR(TRIM(last_nm), 2))  AS last_name,
        LOWER(TRIM(email_addr))     AS email,
        UPPER(TRIM(cntry_cd))       AS country_code,
        strptime(reg_dt,['%Y-%m-%d', '%d/%m/%Y'])::DATE              AS registered_date,
        UPPER(TRIM(tier))           AS tier,
        mrktng_opt_in::BOOLEAN      AS is_marketing_opted_in

    FROM source
    WHERE id IS NOT NULL

)

SELECT * FROM renamed
