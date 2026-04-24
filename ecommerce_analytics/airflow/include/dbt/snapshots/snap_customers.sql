{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['tier', 'country_code', 'is_marketing_opted_in']
    )
}}

SELECT * FROM {{ref('stg_customers')}}

{% endsnapshot %}