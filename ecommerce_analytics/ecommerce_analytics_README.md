# ShopNova E-Commerce Analytics

A production-style analytics engineering pipeline built with **dbt + Snowflake**, modeling a multi-channel e-commerce business across the full transformation stack — raw ingestion → staged models → intermediate business logic → mart-layer facts and dimensions → daily orchestration with Apache Airflow on Astronomer.

[![dbt](https://img.shields.io/badge/dbt-Core_1.8-orange)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-dev-yellow)](https://duckdb.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-prod-blue)](https://www.snowflake.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.x_Astronomer-red)](https://www.astronomer.io/)

---

## The Problem

Three source tables land daily from a Shopify + backend system — customers, orders, and products. Every team is calculating metrics differently. Finance, Product, and the CEO dashboard disagree on revenue every single month.

This pipeline creates one trusted analytics layer with a single definition of revenue, margin, and customer segments that every downstream consumer pulls from.

---

## Architecture

```
Raw Seeds (Snowflake · OTC_RAW schema)
         │
         ▼
┌──────────────────────────────────────┐
│           Staging Layer              │  Views · no business logic
│  stg_orders · stg_customers          │  Rename · cast · normalize casing
│  stg_products · stg_promotions       │  1:1 with source tables
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│         Intermediate Layer           │  Views · complex joins live here
│       int_orders_enriched            │  Revenue / cost / margin calcs
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│            Mart Layer                │  Tables · optimized for BI queries
│  fct_orders      (incremental)       │
│  dim_customers · dim_products        │
│  dim_promotions                      │
└──────────────────┬───────────────────┘
                   │
                   ▼
        Snapshots (SCD Type 2)
        snap_customers
```

---

## Star Schema

```
           dim_customers                    dim_promotions
           ─────────────                    ──────────────
           customer_id (PK)                 promo_code (PK)
           customer_name                    discount_pct
           country                          promo_type
           customer_tier                    start_date / end_date
           marketing_opt_in
                 │ FK
                 │
dim_products     │         fct_orders
─────────────    └──────── ──────────────────────
product_id (PK)            order_id (PK)
product_name               customer_id (FK)
category                   product_id (FK)
brand                       order_date
list_price                  status
cost_price                  quantity · unit_price
gross_margin_pct            discount_pct
is_active                   revenue
     │ FK                   gross_revenue
     └─────────────────────  discount_amount
                             cost · gross_profit
                             channel · promo_code
```

---

## Metric Definitions

All revenue metrics are defined once — in `int_orders_enriched` — and surfaced through `fct_orders`. No metric is computed in two places.

| Metric | Formula |
|---|---|
| `revenue` | `qty × unit_price × (1 - disc_pct)` |
| `gross_revenue` | `qty × unit_price` |
| `discount_amount` | `gross_revenue - revenue` |
| `cost` | `qty × cost_price` |
| `gross_profit` | `revenue - cost` |

`unit_price` is captured at order time — preserving historical accuracy when prices change.

---

## Key Engineering Decisions

**Incremental `fct_orders`** — Uses dbt's `is_incremental()` macro with a custom `incremental_filter` macro to append only new rows on each run. Unique key deduplication via `unique_key='order_id'` with `on_schema_change='fail'` to catch breaking changes early. Full refresh available via `--full-refresh` flag.

**Intermediate layer owns complex logic** — `int_orders_enriched` centralizes all revenue, cost, and margin calculations. Mart models stay thin — they select from the intermediate layer and join to dimensions. This makes logic changes a single-file edit.

**SCD Type 2 customer history** — `snap_customers` tracks changes to `customer_tier`, `country`, and `marketing_opt_in` using dbt's `check` strategy. Every change gets a `dbt_valid_from` / `dbt_valid_to` timestamp — enabling point-in-time customer segmentation queries.

**Custom `generate_schema_name` macro** — Overrides dbt's default behavior so models land in clean schemas (`staging`, `intermediate`, `marts`, `snapshots`) in both dev and prod — no environment prefixes cluttering Snowflake.

**Dual-target profiles** — Local development runs on DuckDB (zero cost, zero credentials, instant). Production target points to Snowflake. Same model code, same tests, different execution engine.

---

## Data Quality

Tests run across all staging and mart models using **dbt core tests** + **dbt_expectations**:

```yaml
# Example: fct_orders schema.yml
- name: revenue
  tests:
    - dbt_expectations.expect_column_values_to_be_between:
        min_value: 0
        max_value: 100000
- name: order_id
  tests:
    - unique
    - not_null
    - dbt_expectations.expect_column_values_to_match_regex:
        regex: '^ORD[0-9]{3}$'
```

Test failures are written to queryable tables (`store_failures: true`) — no need to re-run the pipeline to debug which rows failed.

---

## Orchestration

Daily pipeline via **Apache Airflow on Astronomer** (Docker-based):

```
Schedule: 0 6 * * *  (06:00 UTC daily)

dbt build → seeds → staging → intermediate → marts → snapshots → tests
```

Airflow captures full dbt output per run including row counts and test results. Failed tests surface in Airflow logs with the specific rows that failed, queryable directly in Snowflake's `test_failures` schema.

---

## Project Structure

```
ecommerce_analytics/
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_promotions.sql
│   │   ├── sources.yml
│   │   └── schema.yml
│   ├── intermediate/
│   │   ├── int_orders_enriched.sql
│   │   └── schema.yml
│   └── marts/
│       ├── fct_orders.sql            ← incremental
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       ├── dim_promotions.sql
│       └── schema.yml
├── snapshots/
│   └── snap_customers.sql            ← SCD Type 2
├── macros/
│   ├── incremental_filter.sql
│   └── generate_schema_name.sql
├── seeds/
│   └── (raw CSV source files)
├── airflow/
│   ├── dags/dbt_pipeline.py
│   └── docker-compose.override.yml
├── dbt_project.yml
└── packages.yml
```

---

## Stack

| Layer | Tool |
|---|---|
| Transformation | dbt Core 1.8 |
| Dev warehouse | DuckDB |
| Prod warehouse | Snowflake |
| Orchestration | Apache Airflow 2.x (Astronomer) |
| Testing | dbt core + dbt_expectations |
| Packages | dbt_utils, dbt_expectations |

---

## Running the Project

```bash
# Install adapters
pip install dbt-duckdb dbt-snowflake

# Install dbt packages
dbt deps

# Load raw seed data
dbt seed

# Build everything: models + tests
dbt build

# Incremental run only (appends new orders)
dbt run --select fct_orders

# Force full rebuild of incremental model
dbt run --select fct_orders --full-refresh

# Rebuild customer snapshots
dbt snapshot

# Serve lineage + docs
dbt docs generate && dbt docs serve
```

Target switch — same code, different warehouse:

```bash
dbt build --target dev        # DuckDB (default)
dbt build --target snowflake  # Snowflake prod
```
