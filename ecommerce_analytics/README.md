# ecommerce_analytics

A production-style dbt pipeline modeling e-commerce data into a trusted analytics layer — built with staging and mart separation, data quality tests, and full column-level documentation.

[![dbt](https://img.shields.io/badge/dbt-Core-orange)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Local-yellow)](https://duckdb.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Production-blue)](https://www.snowflake.com/)

---

## The Problem

Three source tables land daily from a Shopify + backend system — customers, orders, and products. Every team is calculating metrics differently. Finance, Product, and the CEO dashboard disagree on revenue every single month.

This pipeline creates one reliable analytics layer that every team pulls from.

---

## Architecture

```
Raw source tables (raw schema)
    raw.raw_customers   — 20 customer records, inconsistent formats
    raw.raw_orders      — 40 orders Jan–May 2024, mixed statuses
    raw.raw_products    — 15 products, varied category casing

        ↓  staging layer (views)

    stg_customers       — renamed columns, cast types, trim whitespace
    stg_orders          — cast quantities/discounts, normalize status + channel
    stg_products        — standardize categories, cast prices, boolean flags

        ↓  mart layer (tables)

    dim_customers       — one row per customer, business attributes + derived fields
    dim_products        — one row per product, margin calculations
    fct_orders          — grain: one row per order, all revenue metrics, FK to dims
```

---

## Star Schema

```
           dim_customers
           ─────────────
           customer_id (PK)
           customer_name
           email
           country
           customer_tier
           marketing_opt_in
           days_since_signup
                 │ FK
                 │
dim_products     fct_orders
─────────────    ──────────────────────
product_id (PK)  order_id (PK)
product_name     customer_id (FK)
category         product_id (FK)
brand            order_date
list_price        status
cost_price        quantity
gross_margin_pct  unit_price
is_active         discount_pct
launch_date       revenue
     │ FK          gross_revenue
     └──────────── discount_amount
                   cost
                   gross_profit
                   channel
                   promo_code
```

---

## Revenue Metric Definitions

All revenue metrics are defined in `fct_orders` and are consistent across every downstream report.

| Metric | Formula |
|---|---|
| `revenue` | `quantity × unit_price × (1 - discount_pct)` |
| `gross_revenue` | `quantity × unit_price` |
| `discount_amount` | `gross_revenue - revenue` |
| `cost` | `quantity × cost_price` |
| `gross_profit` | `revenue - cost` |

`unit_price` is captured at order time from `dim_products.list_price` — preserving historical accuracy when prices change.

---

## Data Quality Tests

dbt tests run across all staging and mart models.

**Staging tests (stg_orders):**
- `order_id` — unique, not_null
- `status` — accepted_values: COMP, COMPLETED, PENDING, CANCELLED, REFUND, REFUNDED
- `customer_id` — not_null
- `quantity` — not_null

**Mart tests (fct_orders):**
- `order_id` — unique, not_null
- `status` — accepted_values: Completed, Pending, Cancelled, Refunded, Unknown
- `customer_id` — relationships → dim_customers.customer_id
- `product_id` — relationships → dim_products.product_id

Run all tests:
```bash
dbt test
```

---

## Project Structure

```
ecommerce_analytics/
├── seeds/
│   ├── raw_customers.csv
│   ├── raw_orders.csv
│   └── raw_products.csv
│
├── models/
│   ├── staging/
│   │   ├── sources.yml          ← registers raw source tables
│   │   ├── schema.yml           ← staging tests + docs
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── stg_products.sql
│   │
│   └── marts/
│       ├── schema.yml           ← mart tests + docs
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       └── fct_orders.sql
│
├── dbt_project.yml
└── README.md
```

---

## How to Run

**Prerequisites:** dbt Core installed, DuckDB adapter configured

```bash
# 1. Install dbt-duckdb
pip install dbt-core dbt-duckdb

# 2. Load seed data into raw schema
dbt seed

# 3. Run all models (staging → marts)
dbt run

# 4. Run all data quality tests
dbt test

# 5. Run everything in one command
dbt build

# 6. Generate and serve documentation
dbt docs generate
dbt docs serve
```

**Profiles.yml** (lives at `~/.dbt/profiles.yml`, never in the repo):
```yaml
ecommerce_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ~/path/to/dev.duckdb
      threads: 4
```

---

## Key Design Decisions

**Why LEFT JOIN in fct_orders?**
Some orders may have NULL customer_id (guest checkouts) or a deleted product. LEFT JOIN ensures no order rows are silently dropped from the fact table.

**Why unit_price lives in fct_orders, not dim_products?**
Prices change over time. Capturing `unit_price` at order time preserves historical accuracy — a $39.99 January order stays $39.99 even if the product reprices in March.

**Why status and channel are not separate dim tables?**
Both have fewer than 10 values and no independent attributes. They're degenerate dimensions — storing them directly in the fact table is correct Kimball practice.

**Why staging = views, marts = tables?**
Staging models are cleaned source copies — making them views avoids redundant storage. Mart models are queried by dashboards and analysts — materializing as tables makes them fast to query without re-computing joins every time.

---

## Warehouse

Built locally on **DuckDB** (dev). Production target: **Snowflake**.

Switching to Snowflake requires only a `profiles.yml` change — no model code changes needed.

---

*Part of the Analytics Engineering Portfolio · [View all projects](../README.md)*
