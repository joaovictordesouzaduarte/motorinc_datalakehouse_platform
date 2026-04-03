# Motorinc Data Lakehouse Platform - Agent Documentation

## Project Overview

This project implements a **data lakehouse** on AWS using the **medallion architecture** (Bronze → Silver → Gold). Transactional data is replicated from a PostgreSQL OLTP database into S3 via AWS DMS, then transformed and modeled with dbt, and queried with Amazon Athena.

**Repository**: `/mnt/c/opt/projects/motorinc_datalakehouse_platform`

---

## Architecture

| Layer | Service | Description |
|-------|---------|-------------|
| Source | RDS PostgreSQL 17.4 | OLTP database (customers, orders, products, employees, offices) |
| Replication | AWS DMS | CDC + full load from RDS to S3 bronze, partitioned by hour |
| Storage | S3 | Bronze (raw), Silver (ods), Gold (dwh), Athena results, Config |
| Catalog | AWS Glue | Metadata store for raw Hive tables and Iceberg tables |
| Transform | dbt on ECS Fargate | Runs bronze → silver → gold models on a schedule |
| Query | Amazon Athena | SQL engine over S3, reads Glue catalog |
| Scheduling | EventBridge Scheduler | Triggers ECS Fargate task every 15 minutes |
| Secrets | Secrets Manager | Stores RDS credentials, consumed by DMS and ECS |
| Registry | ECR | Hosts the dbt Docker image |

### Medallion Layers

- **Bronze (raw)** — raw CDC output from DMS, Hive-partitioned by `year/month/day/hour`, registered in Glue under `raw` schema
- **Silver (ods)** — deduplicated, latest-row-per-key tables stored as Iceberg in Glue under `ods` schema
- **Gold (dwh)** — business aggregates (e.g. sales by office and date) stored as Iceberg in Glue under `dwh` schema

---

## S3 Bucket Structure

| Bucket | Purpose | Path Pattern |
|--------|---------|--------------|
| `datalake-1-bronze-*` | Raw CDC data from DMS | `commercial/erp/public/<table>/YYYY/MM/DD/HH/` |
| `datalake-2-silver-*` | Deduplicated staging tables | `commercial/erp/<table>/` |
| `datalake-3-gold-*` | Business aggregates | `commercial/sales/dataset/` |
| `aws-athena-query-results-*` | Athena query staging | `tables/` |

---

## Project Structure

```
motorinc_datalakehouse_platform/
├── infrastructure/              # CloudFormation templates
│   ├── s3.yml                  # S3 buckets
│   ├── rds.yml                 # RDS PostgreSQL + Secrets Manager
│   ├── dms.yml                 # DMS replication instance, endpoints, task
│   ├── glue.yml                # Glue databases
│   ├── athena.yml              # Athena workgroup
│   ├── iam_roles.yml           # IAM roles
│   ├── iam_users.yml           # IAM users
│   ├── container.yml           # ECR + ECS cluster + task definition
│   ├── event.yml               # EventBridge scheduler
│   └── ecs.env                 # dbt runtime env vars (gitignored)
├── motorinc_dlh/               # dbt project (Athena + medallion)
│   ├── dbt_project.yml         # Project config
│   ├── profiles.yml            # dbt profile (in docker/)
│   ├── vars.yml                # dbt variables
│   ├── macros/
│   │   ├── add_raw_partition.sql         # Pre-hook: ADD PARTITION for raw Hive tables
│   │   ├── generate_schema_name.sql      # Custom schema naming
│   │   └── macro_silver_erp_location.sql
│   └── models/
│       ├── bronze/             # Views that register DMS partitions
│       ├── silver/             # Iceberg incremental models (stg_*)
│       └── gold/               # Iceberg business aggregates
├── docker/                     # dbt + Athena Docker image
├── scripts/
│   ├── run_dbt.sh              # bronze → silver → gold runner
│   ├── load_sales.py           # Order simulator for RDS
│   ├── create_raw_hive_table.sql
│   └── create_ods_iceberg_tables.sql
└── .github/workflows/
    └── deploy_cf.yml          # CI/CD for CloudFormation
```

---

## dbt Models

### Bronze Layer (raw.*)
Views that register the latest DMS hourly partitions in Glue.

| Model | Table | Purpose |
|-------|-------|---------|
| `raw_customers_partition.sql` | raw.customers | Activates current partition |
| `raw_employees_partition.sql` | raw.employees | Activates current partition |
| `raw_offices_partition.sql` | raw.offices | Activates current partition |
| `raw_orderdetails_partition.sql` | raw.orderdetails | Activates current partition |
| `raw_orders_partition.sql` | raw.orders | Activates current partition |
| `raw_products_partition.sql` | raw.products | Activates current partition |

**Important**: Bronze models use a `pre_hook` that runs `add_raw_partition()` macro to add the partition for the current hour before querying.

### Silver Layer (ods.*)
Iceberg incremental models with deduplication (row_number = 1).

| Model | Unique Key | Source Table |
|-------|------------|--------------|
| `stg_customers.sql` | customernumber | raw.customers |
| `stg_employees.sql` | employeenumber | raw.employees |
| `stg_offices.sql` | officecode | raw.offices |
| `stg_orderdetails.sql` | ordernumber, productcode | raw.orderdetails |
| `stg_orders.sql` | ordernumber | raw.orders |
| `stg_products.sql` | productcode | raw.products |

**Incremental Strategy**: `merge` with `unique_key`
**Table Type**: Iceberg
**Format**: Parquet

### Gold Layer (dwh.*)

| Model | Unique Key | Description |
|-------|------------|-------------|
| `load_sales_dataset.sql` | locality_officecode, calendar_sales_date | Sales aggregates by office, date, and size interval |

---

## Key Macros

### `add_raw_partition(table_name)`
Adds a partition to a raw Hive table based on the current run time.

**Location**: `motorinc_dlh/macros/add_raw_partition.sql`

**Logic**:
- Uses `run_started_at` minus 3 hours (Brazil timezone)
- Can be overridden with vars: `partition_date`, `partition_year`, `partition_month`, `partition_day`, `partition_hour`
- Partition format: `year_stream='YYYY', month_stream='MM', day_stream='DD', hour_stream='HH'`

**S3 Path**: `s3://datalake-1-bronze-us-east-1-086997587178-sandbox-dlh/commercial/erp/public/<table>/<year>/<month>/<day>/<hour>/`

---

## Raw Hive Tables Schema

**Location**: `scripts/create_raw_hive_table.sql`

| Table | Partition Keys | Location |
|-------|----------------|----------|
| raw.customers | year_stream, month_stream, day_stream, hour_stream | `commercial/erp/public/customers/` |
| raw.employees | year_stream, month_stream, day_stream, hour_stream | `commercial/erp/public/employees/` |
| raw.offices | year_stream, month_stream, day_stream, hour_stream | `commercial/erp/public/offices/` |
| raw.orderdetails | year_stream, month_stream, day_stream, hour_stream | `commercial/erp/public/orderdetails/` |
| raw.orders | year_stream, month_stream, day_stream, hour_stream | `commercial/erp/public/orders/` |
| raw.products | year_stream, month_stream, day_stream, hour_stream | `commercial/erp/public/products/` |

---

## DMS Configuration

**Location**: `infrastructure/dms.yml`

**Key Settings**:
- **Migration Type**: `cdc` (full-load-and-cdc)
- **S3 Settings**:
  - `ParquetVersion`: PARQUET_2_0
  - `BucketFolder`: `commercial/erp`
  - `DatePartitionSequence`: YYYYMMDDHH
  - `DatePartitionDelimiter`: SLASH
  - `DatePartitionTimezone`: America/Sao_Paulo
- **ExtraConnectionAttributes**: `dataFormat=parquet;timestampColumnName=extract_at;addColumnName=true;includeOpForFullLoad=true`

---

## dbt Profile Configuration

**Location**: `docker/profiles.yml`

```yaml
motorinc_dlh:
  target: dev
  outputs:
    dev:
      type: athena
      work_group: data-viz
      s3_staging_dir: s3://aws-athena-query-results-us-east-1-086997587178-sandbox-dlh/dbt/
      region_name: us-east-1
      database: awsdatacatalog
      schema: raw
```

**Environment Variables**:
- `ATHENA_WORK_GROUP` (default: data-viz)
- `S3_STAGING_DIR` (default: s3://aws-athena-query-results-.../dbt/)
- `AWS_REGION` (default: us-east-1)

---

## dbt Commands

### Full Refresh (all layers)
```bash
dbt run --full-refresh
```

### Full Refresh Silver + Gold only (skip Bronze views)
```bash
dbt run --full-refresh --select silver.* gold.*
```

### Full Refresh specific tables
```bash
dbt run --full-refresh --select stg_customers stg_employees stg_offices stg_orderdetails stg_orders stg_products load_sales_dataset
```

### Run with specific partition date
```bash
dbt run --vars '{"partition_date": "2026-01-01", "partition_hour": "00"}'
```

---

## Infrastructure Deployment

Stacks are deployed via GitHub Actions using commit tags:

| Tag | Stack |
|-----|-------|
| `[s3]` | S3 buckets |
| `[rds]` | RDS + Secrets Manager |
| `[roles]` | IAM roles |
| `[athena]` | Athena workgroup |
| `[users]` | IAM users |
| `[glue]` | Glue databases |
| `[dms]` | DMS replication |
| `[container]` | ECR + ECS |
| `[event]` | EventBridge scheduler |

**Deploy Order**: `s3` → `rds` → `roles` → `athena` → `users` → `glue` → `dms` → `container` → `event`

---

## Known Issues & Troubleshooting

### ICEBERG_MISSING_METADATA Error
**Symptom**: `ICEBERG_MISSING_METADATA: Metadata not found in metadata location`

**Solutions**:
1. Delete the table from Glue Catalog and let dbt recreate it:
   ```bash
   aws glue delete-table --database <schema> --name <table>
   ```
2. Or drop via Athena:
   ```sql
   DROP TABLE IF EXISTS <schema>.<table>;
   ```
3. Clean up orphaned metadata in S3 staging bucket

### Empty columns in raw tables (customernumber, customername, etc.)
**Symptom**: Columns show NULL despite data existing in source

**Possible Causes**:
1. DMS not capturing all columns properly
2. Schema mismatch between Hive table definition and Parquet file
3. Partition pointing to wrong location

**Investigation**:
```sql
-- Check raw data
SELECT * FROM raw.customers LIMIT 1;

-- Check S3 file directly
SELECT * FROM s3object FROM s3object_to_json(...) 
```

---

## Environment

- **dbt Version**: 1.11.7
- **dbt-athena-adapter**: 1.10.0
- **Python**: 3.12
- **AWS Region**: us-east-1
- **Account**: 086997587178
- **Environment**: sandbox-dlh
