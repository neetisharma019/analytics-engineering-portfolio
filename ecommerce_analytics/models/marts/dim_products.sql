WITH products AS (

    SELECT * FROM {{ ref('stg_products') }}

),

final AS (

    SELECT
        product_id,
        product_name,

        -- Standardize category casing
        category,
        brand,
        list_price,
        cost_price,
        is_active,
        launch_date,

        -- Derived: how much money per unit after cost
        ROUND(list_price - cost_price, 2)       AS gross_margin,

        -- Derived: margin as a percentage of list price
        ROUND(
            (list_price - cost_price) / list_price * 100
        , 2)                                    AS margin_pct

    FROM products

)

SELECT * FROM final