# NovaTech Seeds — Order-to-Cash Analytics

A production-style analytics engineering pipeline built with **dbt + Snowflake**, modeling the full Order-to-Cash (O2C) process for a B2B wholesale distributor. Covers the end-to-end revenue cycle — from sales order creation through shipment, invoicing, and cash collection — with operational KPIs used by Finance, Supply Chain, and Sales leadership.

[![dbt](https://img.shields.io/badge/dbt-Core_1.8-orange)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-prod-blue)](https://www.snowflake.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.x_Astronomer-red)](https://www.astronomer.io/)

---

## Business Context

Order-to-Cash is the financial backbone of any B2B business. It spans every step between a customer placing an order and cash arriving in the bank. When any stage breaks down — an order that doesn't ship on time, an invoice that goes unpaid, a payment that doesn't reconcile — finance teams are flying blind.

This pipeline models the full O2C cycle from 6 raw operational source tables into a clean, tested analytics layer, producing the KPIs that Finance and Operations review weekly.

---

## The O2C Process Modeled

```
Customer Places Order  →  Order Fulfilled & Shipped  →  Invoice Generated  →  Payment Collected
    (raw_sales_orders)       (raw_shipments)               (raw_invoices)        (raw_payments)
         │                        │                              │                     │
         └──── raw_order_lines ───┘                              └────────────────────┘
                  (line items)
```

---

## Architecture

```
Raw Seeds (Snowflake · OTC_RAW schema)
  raw_sales_orders · raw_order_lines · raw_customers
  raw_products · raw_shipments · raw_invoices · raw_payments
         │
         ▼
┌──────────────────────────────────────┐
│           Staging Layer              │  Views · 1:1 source tables
│  stg_sales_orders · stg_order_lines  │  Type casting · casing normalization
│  stg_customers · stg_products        │  No business logic
│  stg_shipments · stg_invoices        │
│  stg_payments                        │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│         Intermediate Layer           │  Views · complex O2C joins
│  int_order_financials                │  Revenue + line-item aggregation
│  int_invoice_payments                │  Invoice-to-payment matching
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│            Mart Layer                │  Tables · Finance + Ops BI layer
│  fct_order_lines    (grain: line)    │
│  fct_invoice_aging  (grain: invoice) │
│  dim_customers                       │
│  dim_products                        │
└──────────────────────────────────────┘
```

---

## KPIs Produced

### Days Sales Outstanding (DSO)

Measures how long it takes to collect cash after invoicing. Industry benchmark for B2B: < 45 days.

```sql
DSO = (Total Outstanding AR / Total Credit Sales) × Number of Days
```

Tracked per customer, region, and sales rep — surfaced in `fct_invoice_aging`.

### On-Time Delivery Rate

```sql
On-Time % = Orders delivered on or before requested_delivery_dt / Total shipped orders
```

Calculated in `fct_order_lines` by joining shipment dates against requested delivery dates.

### Invoice Aging Buckets

Outstanding invoices classified by how long they've been open:

| Bucket | Definition |
|---|---|
| Current | Not yet due |
| 1–30 days overdue | Past due < 30 days |
| 31–60 days overdue | Past due 31–60 days |
| 60+ days overdue | Past due > 60 days |

### Revenue Metrics (per order line)

| Metric | Formula |
|---|---|
| `line_revenue` | `qty × unit_price × (1 - disc_pct)` |
| `gross_revenue` | `qty × unit_price` |
| `discount_amount` | `gross_revenue - line_revenue` |
| Order-level rollups | Aggregated from line grain to order grain |

---

## Key Engineering Decisions

**Intermediate layer owns O2C joins** — `int_order_financials` joins sales orders, order lines, and products to produce revenue-enriched order data. `int_invoice_payments` links invoices to payments with partial payment handling. Mart models are thin consumers of these intermediates — no raw table joins in marts.

**Separate intermediate models per O2C stage** — Financial logic (revenue, discounts) and AR logic (invoice aging, payment matching) live in separate intermediate models. This prevents a single monolithic join that's hard to test and maintain.

**Staging normalizes inconsistent source data** — Raw operational data has mixed casing throughout (`ACH`, `ach`, `Wire`, `wire`; `SHIPPED`, `Shipped`, `shipped`; `paid`, `UNPAID`, `Partial`). Staging models apply `UPPER(TRIM(...))` and `CASE` normalization so mart models never deal with formatting issues.

**Invoice aging is point-in-time** — Aging buckets in `fct_invoice_aging` are calculated relative to `CURRENT_DATE`, making them accurate on every dbt run without needing a separate date spine.

**Snowflake-native advanced features** — The project leverages Snowflake Streams + Tasks for CDC-style change detection, Zero-Copy Cloning for dev/prod environment parity, and Time Travel for point-in-time debugging and data recovery.

---

## Data Quality

Tests defined in `schema.yml` across all staging and mart models:

**Staging tests:**
- `not_null` + `unique` on all primary keys (`order_id`, `invoice_id`, `payment_id`, etc.)
- `accepted_values` on normalized status fields
- `relationships` tests — every `order_id` in invoices must exist in sales orders

**Mart tests:**
- `fct_order_lines` — revenue >= 0, quantity > 0, discount_pct between 0 and 1
- `fct_invoice_aging` — invoice_amt > 0, aging bucket values are valid
- `dim_customers` — unique customer_id, not_null on key business attributes

Failed test rows are written to Snowflake's `test_failures` schema via `store_failures: true`.

---

## Snowflake Features Used

| Feature | Usage |
|---|---|
| **Streams** | Detect new/changed rows in `raw_sales_orders` without full table scans |
| **Tasks** | CRON-scheduled task triggers processing when stream has data |
| **Zero-Copy Cloning** | Spin up dev environment from prod snapshot instantly |
| **Time Travel** | Point-in-time query and data recovery (90-day retention) |
| **Table Clustering** | `fct_invoice_aging` clustered on `invoice_dt` for fast aging queries |
| **Warehouse Auto-Suspend** | Compute warehouse suspends after 60s of inactivity |

---

## Project Structure

```
otc_analytics/
├── models/
│   ├── staging/
│   │   ├── stg_sales_orders.sql
│   │   ├── stg_order_lines.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_shipments.sql
│   │   ├── stg_invoices.sql
│   │   ├── stg_payments.sql
│   │   ├── sources.yml
│   │   └── schema.yml
│   ├── intermediate/
│   │   ├── int_order_financials.sql
│   │   ├── int_invoice_payments.sql
│   │   └── schema.yml
│   └── marts/
│       ├── fct_order_lines.sql
│       ├── fct_invoice_aging.sql
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       └── schema.yml
├── macros/
│   ├── incremental_filter.sql
│   └── generate_schema_name.sql
├── seeds/
│   ├── raw_sales_orders.csv
│   ├── raw_order_lines.csv
│   ├── raw_customers.csv
│   ├── raw_products.csv
│   ├── raw_shipments.csv
│   ├── raw_invoices.csv
│   └── raw_payments.csv
├── dbt_project.yml
└── packages.yml
```

---

## Stack

| Layer | Tool |
|---|---|
| Transformation | dbt Core 1.8 |
| Warehouse | Snowflake |
| Orchestration | Apache Airflow 2.x (Astronomer) |
| CDC / Streaming | Snowflake Streams + Tasks |
| Testing | dbt core tests + dbt_expectations |
| Packages | dbt_utils, dbt_expectations |

---

## Running the Project

```bash
# Install adapter
pip install dbt-snowflake

# Install dbt packages
dbt deps

# Load raw seed data into Snowflake
dbt seed

# Build full pipeline: models + tests
dbt build

# Run individual mart
dbt run --select fct_invoice_aging

# Generate and serve docs + lineage
dbt docs generate && dbt docs serve
```

Requires a `~/.dbt/profiles.yml` with Snowflake credentials (never committed to repo):

```yaml
otc_analytics:
  target: snowflake
  outputs:
    snowflake:
      type: snowflake
      account: <your_account>
      user: <your_user>
      password: <env_var or keyfile>
      role: ACCOUNTADMIN
      database: ANALYTICS
      warehouse: COMPUTE_WH
      schema: staging
      threads: 4
```

---

## Sample Output: Invoice Aging Summary

| Aging Bucket | Invoice Count | Total AR Outstanding |
|---|---|---|
| Current | 4 | $19,839 |
| 1–30 days overdue | 1 | $4,290 |
| 31–60 days overdue | 0 | $0 |
| 60+ days overdue | 2 | $13,795 |

*Based on seed data as of June 2024*

---

*Part of the Analytics Engineering Portfolio · [View all projects](../README.md)*
