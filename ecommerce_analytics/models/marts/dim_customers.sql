WITH customers AS (

    SELECT * FROM {{ ref('stg_customers') }}

),

final AS (

    SELECT
        customer_id,
        first_name || ' ' || last_name AS full_name,
        first_name,
        last_name,
        email,
        country_code,
        registered_date as registration_date,

        -- Standardize tier casing (raw data has 'GOLD', 'gold', 'Gold')
        tier,
        is_marketing_opted_in,

        -- Derived field: how long have they been a customer?
        DATEDIFF('day', registration_date, CURRENT_DATE) AS days_since_signup,

        -- Derived field: are they a new customer (< 90 days)?
        CASE
            WHEN DATEDIFF('day', registration_date, CURRENT_DATE) <= 90
            THEN TRUE ELSE FALSE
        END                                     AS is_new_customer

    FROM customers

)

SELECT * FROM final