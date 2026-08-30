# AtliQ Commerce — End-to-End Data Engineering Capstone

## Project Overview

This project implements an end-to-end cloud data engineering solution for **AtliQ Commerce**, an online retail business.

The objective is to separate transactional processing from analytical reporting by moving operational data from an **Azure SQL OLTP database** into a modern analytical platform using a Medallion architecture.

The solution implements:

- Azure SQL as the operational source system
- Azure Data Factory for ingestion and orchestration
- Azure Data Lake Storage Gen2 for Bronze and Gold storage
- Azure Databricks and PySpark for Silver transformations
- Delta Lake for reliable and idempotent data processing
- dbt Core for Gold dimensional modelling and data-quality testing
- Microsoft Fabric / Power BI for analytical reporting
- Git and GitHub for source control
- GitHub Actions for CI
- Audit logging and failure monitoring for operational reliability

---

## Business Requirements

The analytical platform supports the following business questions:

1. **Revenue Trend**
   - Gross revenue by month.

2. **Top Products**
   - Gross revenue by product and product category.

3. **Top Cities**
   - Gross revenue by customer city.

4. **New vs Returning Customers**
   - Customer activity based on signup cohort versus order month.

The Microsoft Fabric report also provides interactive slicers for:

- Date range
- Product category

---

## Solution Architecture

```text
                         SOURCE SYSTEMS
                                │
                ┌───────────────┴───────────────┐
                │                               │
        Azure SQL Database                  CSV Files
                │                     ┌─────────┴─────────┐
                │                     │                   │
                │          supplier_price_list.csv  marketing_spend.csv
                │
                ▼
        Azure Data Factory
     Metadata-Driven Ingestion
                │
                ▼
     Azure Data Lake Storage Gen2
              BRONZE
             Parquet
                │
                ▼
        Azure Databricks
              SILVER
       PySpark + Delta Lake
     Cleaning / MERGE / DQ
                │
                ▼
             dbt Core
               GOLD
           Star Schema
                │
                ▼
       External Delta Tables
          ADLS Gen2 /gold
                │
         OneLake Shortcut
                │
                ▼
        Microsoft Fabric
          Semantic Model
                │
                ▼
          Power BI Report
```

---

## Technology Stack

| Layer | Technology |
|---|---|
| Operational Database | Azure SQL Database |
| Orchestration | Azure Data Factory |
| Data Lake | Azure Data Lake Storage Gen2 |
| Bronze Format | Apache Parquet |
| Processing | Azure Databricks |
| Transformation Language | PySpark / SQL |
| Silver Format | Delta Lake |
| Governance | Unity Catalog |
| Dimensional Modelling | dbt Core |
| Gold Format | Delta Lake |
| Reporting | Microsoft Fabric / Power BI |
| Version Control | Git / GitHub |
| CI/CD | GitHub Actions |
| Programming | Python, SQL, PySpark, DAX |

---

## Source Data

### Azure SQL Tables

| Table | Purpose | Load Type | Watermark |
|---|---|---|---|
| `customers` | Customer master data | Full | — |
| `products` | Product catalogue | Full | — |
| `orders` | Order headers | Incremental | `updated_at` |
| `order_items` | Order line items | Incremental | `created_at` |
| `payments` | Payment transactions | Incremental | `updated_at` |

### CSV Sources

| File | Purpose |
|---|---|
| `supplier_price_list.csv` | Supplier cost information by product |
| `marketing_spend.csv` | Marketing expenditure by date, channel and campaign |

A Python simulator is provided to generate additional transactional activity:

```bash
python simulator/daily_order_simulator.py --orders 8
```

---

## OLTP vs OLAP Design

### OLTP Layer

Azure SQL is used for operational transaction processing.

Characteristics:

- Normalized relational design
- Frequent inserts and updates
- Optimized for operational transactions
- Contains customers, products, orders, order items and payments

### OLAP Layer

The analytical platform is optimized for reporting and aggregation.

Characteristics:

- Medallion architecture
- Delta Lake storage
- Dimensional star schema
- Historical analytical workloads
- Microsoft Fabric reporting

This separation prevents analytical workloads from placing unnecessary load on the operational Azure SQL database.

---

## Bronze Layer — Azure Data Factory

Azure Data Factory implements metadata-driven ingestion.

The control table:

```text
etl.control_table
```

contains:

- table name
- source schema
- load type
- watermark column
- last successful watermark

Incremental extraction uses watermark columns.

Conceptually:

```sql
WHERE watermark_column > last_loaded_at
```

The watermark is updated only after a successful source copy.

Incremental data is written into date-partitioned folders such as:

```text
bronze/orders/ingest_date=YYYY-MM-DD/
bronze/order_items/ingest_date=YYYY-MM-DD/
bronze/payments/ingest_date=YYYY-MM-DD/
```

Full-refresh datasets overwrite their current Bronze snapshots.

---

## Silver Layer — Azure Databricks

The Silver layer is implemented using PySpark and Delta Lake in:

```text
dbw_atliq_capstone.silver
```

### Full-Refresh Tables

The following tables are cleaned and overwritten:

- `customers`
- `products`
- `supplier_price_list`
- `marketing_spend`

Typical transformations include:

- trimming text
- standardizing data types
- removing duplicate records
- rejecting null business keys
- converting date and timestamp fields

### Incremental Tables

The following tables use Delta MERGE.

#### Orders

Business key:

```text
order_id
```

Existing records are updated only when the incoming `updated_at` value is newer.

#### Payments

Business key:

```text
payment_id
```

Existing records are updated only when the incoming `updated_at` value is newer.

#### Order Items

Business key:

```text
order_item_id
```

Order items use insert-only MERGE logic because they are treated as immutable after creation.

This design prevents duplicate records when the same Bronze batch is processed more than once.

---

## Gold Layer — dbt Core

The analytical Gold layer is built using dbt Core.

Catalog:

```text
dbw_atliq_capstone
```

Production schema:

```text
gold
```

Physical Gold storage:

```text
abfss://lakehouse@atliqcommercestorage.dfs.core.windows.net/gold
```

### Gold Star Schema

```text
                         dim_date
                            1
                            │
                            *
                       fact_sales
                       /         \
                      *           *
                     /             \
                    1               1
          dim_customer          dim_product
```

### fact_sales

Grain:

```text
One row per order item
```

Important columns include:

- `order_item_id`
- `order_id`
- `customer_id`
- `product_id`
- `order_date`
- `quantity`
- `item_price`
- `gross_revenue`
- `status`

Gross revenue is calculated as:

```text
quantity × item_price
```

### dim_customer

Contains:

- customer ID
- customer name
- city
- signup date
- signup cohort

### dim_product

Contains:

- product ID
- product name
- category
- unit price
- supplier cost
- unit margin

### dim_date

Provides the calendar dimension used for time-series analysis.

---

## Data Quality Testing

dbt tests are used to validate the Gold analytical model.

Examples include:

- `fact_sales.order_item_id` is not null
- `fact_sales.order_item_id` is unique
- `fact_sales.customer_id` is not null
- `fact_sales.product_id` is not null
- `fact_sales.order_date` is not null
- customer foreign keys resolve to `dim_customer`
- product foreign keys resolve to `dim_product`
- order dates resolve to `dim_date`
- dimension business keys are unique
- order status values are restricted to accepted values

Tests execute as part of:

```bash
dbt build
```

---

## Nightly Automation

The complete nightly workflow is orchestrated using Azure Data Factory and Databricks Jobs.

```text
ADF
 │
 ├── Capture pipeline run timestamp
 │
 ├── Read metadata control table
 │
 ├── Ingest Azure SQL tables
 │
 ├── Ingest CSV sources
 │
 └── Execute Databricks transformation job
             │
             ▼
       Silver PySpark Load
             │
             ▼
          dbt Build
             │
             ▼
          Gold Tables
```

The Databricks transformation job executes:

```text
silver_load
     ↓
gold_dbt_build
```

The ADF pipeline supplies the required `run_date` to the Databricks Silver notebook.

---

## Idempotency

The pipeline is designed to be safe when executed repeatedly.

### Bronze

The same daily ingestion partition can be overwritten safely.

### Silver

Incremental tables use Delta MERGE based on business keys.

### Gold

Gold tables are rebuilt from de-duplicated Silver tables.

### Idempotency Validation

The complete nightly pipeline can be executed twice without generating additional source transactions.

The following validation query should return identical results after both executions:

```sql
SELECT
    COUNT(*) AS fact_rows,
    SUM(gross_revenue) AS total_gross_revenue
FROM dbw_atliq_capstone.gold.fact_sales;
```

If both values remain identical, the pipeline is not double-counting records.

---

## Microsoft Fabric Reporting

Gold Delta tables are exposed to Microsoft Fabric through a OneLake shortcut.

The Fabric semantic model uses the following relationships:

```text
fact_sales.customer_id → dim_customer.customer_id
fact_sales.product_id  → dim_product.product_id
fact_sales.order_date  → dim_date.date_day
```

All relationships use a many-to-one star-schema pattern.

### Dashboard Visuals

The report contains:

- Monthly Gross Revenue Trend
- Top Products by Gross Revenue
- Revenue by Customer City
- New vs Returning Customers
- Date Range slicer
- Product Category slicer

### Revenue Definition

Gross Revenue:

```text
quantity × item_price
```

Net Revenue excludes:

- Cancelled orders
- Returned orders

Returned Revenue is reported separately.

---

## Git and CI/CD

The project is version-controlled using Git and GitHub.

GitHub Actions executes automated dbt validation when a pull request targets the `main` branch.

```text
Pull Request
     ↓
GitHub Actions
     ↓
Checkout Repository
     ↓
Set Up Python
     ↓
Install dbt-databricks
     ↓
dbt debug --target ci
     ↓
dbt build --target ci
     ↓
PASS / FAIL
```

### CI Isolation

CI uses a separate schema:

```text
dbw_atliq_capstone.ci
```

Production uses:

```text
dbw_atliq_capstone.gold
```

CI also uses a separate physical storage location:

```text
abfss://lakehouse@atliqcommercestorage.dfs.core.windows.net/ci
```

Production Gold uses:

```text
abfss://lakehouse@atliqcommercestorage.dfs.core.windows.net/gold
```

This prevents CI tests from modifying production analytical tables.

---

## GitHub Secrets

The CI workflow retrieves Databricks credentials from GitHub repository secrets.

Required secrets:

```text
DATABRICKS_HOST
DATABRICKS_HTTP_PATH
DATABRICKS_TOKEN
```

No Databricks access tokens or Azure SQL passwords are stored directly in the repository.

---

## Monitoring and Audit Logging

Operational monitoring is implemented using:

```text
etl.pipeline_run_audit
```

The audit framework records:

- pipeline run ID
- pipeline name
- source/activity name
- run start time
- run end time
- rows read
- rows written
- execution status
- failure message

Supporting stored procedures include:

```text
etl.usp_audit_run_start
etl.usp_audit_run_complete
```

Recent audit information can be viewed through:

```text
etl.vw_pipeline_run_audit_recent
```

Failure notifications are configured for the nightly transformation workflow so pipeline failures can be investigated before stale data is presented to users.

---

## Repository Structure

```text
atliq-commerce-data-engineering/
│
├── .github/
│   └── workflows/
│       └── dbt-ci.yml
│
├── sql/
│   ├── 01_schema_ddl.sql
│   ├── 02_insert_customers.sql
│   ├── 03_insert_products.sql
│   ├── 04_insert_orders.sql
│   ├── 05_insert_order_items.sql
│   ├── 06_insert_payments.sql
│   ├── 07_etl_control_table.sql
│   └── 08_pipeline_audit.sql
│
├── data/
│   ├── supplier_price_list.csv
│   └── marketing_spend.csv
│
├── simulator/
│   └── daily_order_simulator.py
│
├── adf/
│   ├── pipeline/
│   │   └── PL_Atliq_Nightly.json
│   │
│   ├── trigger/
│   │   └── TR_Nightly_Atliq.json
│   │
│   ├── dataset/
│   │   ├── DS_AzureSQL_Generic.json
│   │   ├── DS_ADLS_Parquet.json
│   │   └── DS_ADLS_CSV_Source.json
│   │
│   └── linkedService/
│       ├── LS_AzureSQL.json
│       ├── LS_ADLS.json
│       └── LS_Databricks.json
│
├── databricks/
│   ├── notebooks/
│   │   └── 02_load_silver.ipynb
│   │
│   └── jobs/
│       └── job_atliq_nightly_transform.yml
│
├── atliq_gold/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   └── models/
│       ├── staging/
│       └── gold/
│
├── fabric/
│
├── docs/
│   ├── fabric/
│   │   ├── AtliQ_Commerce_Dashboard.pdf
│   │   ├── AtliQ_Commerce_Dashboard.png
│   │   └── semantic_model.png
│   │
│   └── screenshots/
│       ├── github-ci.png
│       ├── adf-nightly-trigger.png
│       └── databricks-job.png
│
├── .env.example
├── .gitignore
└── README.md
```

---

## Environment Variables

The repository contains:

```text
.env.example
```

as a template.

The real:

```text
.env
```

file is excluded from Git.

Required Azure SQL variables:

```text
AZ_SQL_SERVER
AZ_SQL_DB
AZ_SQL_USER
AZ_SQL_PASSWORD
```

Required Databricks variables:

```text
DATABRICKS_HOST
DATABRICKS_HTTP_PATH
DATABRICKS_TOKEN
```

---

## Local Setup

### 1. Activate the Python virtual environment

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 2. Verify Python

```powershell
python --version
```

### 3. Verify dbt

```powershell
dbt --version
```

### 4. Move into the dbt project

```powershell
cd atliq_gold
```

### 5. Test Databricks connectivity

```powershell
dbt debug
```

### 6. Build and test the production model

```powershell
dbt build
```

---

## Testing the CI Target Locally

Set the required environment variables in PowerShell.

Example:

```powershell
$env:DATABRICKS_HOST="YOUR_DATABRICKS_HOST"
$env:DATABRICKS_HTTP_PATH="YOUR_DATABRICKS_HTTP_PATH"
$env:DATABRICKS_TOKEN="YOUR_DATABRICKS_TOKEN"
$env:DBT_LOCATION_ROOT="abfss://lakehouse@atliqcommercestorage.dfs.core.windows.net/ci"
```

Do not store the real token in this README or commit it to Git.

Then run:

```powershell
dbt debug --target ci --profiles-dir .
```

Followed by:

```powershell
dbt build --target ci --profiles-dir .
```

A successful CI test should complete with:

```text
ERROR=0
```

---

## Running the Transaction Simulator

From the project root:

```powershell
python simulator\daily_order_simulator.py --orders 8
```

After new transactions are created, execute the nightly pipeline to validate incremental ingestion.

---

## Security Controls

The repository implements the following controls:

- `.env` is excluded from Git
- `.venv` is excluded from Git
- dbt generated `target/` files are excluded
- Databricks PATs are not committed
- Azure SQL passwords are not committed
- GitHub Actions retrieves credentials from GitHub Secrets
- CI uses an isolated database schema
- CI uses an isolated ADLS location
- ADF linked-service JSON files must not contain embedded passwords or tokens

---

## Project Evidence

| Evidence | Location |
|---|---|
| Fabric Dashboard | `docs/fabric/AtliQ_Commerce_Dashboard.png` |
| Fabric Dashboard PDF | `docs/fabric/AtliQ_Commerce_Dashboard.pdf` |
| Fabric Semantic Model | `docs/fabric/semantic_model.png` |
| GitHub CI Result | `docs/screenshots/github-ci.png` |
| ADF Nightly Trigger | `docs/screenshots/adf-nightly-trigger.png` |
| Databricks Transformation Job | `docs/screenshots/databricks-job.png` |

---

## Key Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

- OLTP and OLAP separation
- Metadata-driven ETL
- Watermark-based incremental ingestion
- Medallion architecture
- Parquet
- Delta Lake
- Delta MERGE
- Idempotent data pipelines
- PySpark transformations
- Unity Catalog
- Dimensional modelling
- Star schemas
- dbt Core
- Automated data-quality testing
- Microsoft Fabric
- Direct Lake / OneLake shortcuts
- Git version control
- GitHub Actions CI
- Secrets management
- Monitoring
- Audit logging

---

## Author

**Name:** Jonathan Kisha  
**Project:** AtliQ Commerce End-to-End Data Engineering Capstone  
**Programme:** CodeBasics Data Engineering for Data Analysts Course

<!-- CI validation test -->
