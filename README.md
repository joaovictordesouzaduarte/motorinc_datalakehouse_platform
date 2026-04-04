# Data Lakehouse Pipeline

This repository implements a **data lakehouse** on AWS: transactional data is replicated from a PostgreSQL OLTP into S3, then transformed and modeled with dbt, and queried with Athena. Infrastructure is provisioned with CloudFormation and deployed via GitHub Actions.


![Big Picture](/src/big_picture.png)

---

## VIDEO

https://www.loom.com/share/e73e225458564c059047a5fb7884d973

## Architecture

### QuickSight Dashboard

The gold layer feeds an Amazon QuickSight dashboard that visualizes sales KPIs. The dashboard below shows total quantity ordered (24.62K) broken down by country (donut chart) and sales amount by territory and city (stacked bar chart), covering regions such as NA, Japan, EMEA, and APAC.

![QuickSight Dashboard](/src/screenshot_quicksight.png)

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

### Medallion layers

- **Bronze** — raw CDC output from DMS, Hive-partitioned by `year/month/day/hour`, registered in Glue under `raw`
- **Silver** — deduplicated, latest-row-per-key tables stored as Iceberg in Glue under `ods`
- **Gold** — business aggregates (e.g. sales by office and date) stored as Iceberg in Glue under `dwh`

---

## Project structure

```
├── infrastructure/          # CloudFormation templates
│   ├── s3.yml               # S3 buckets (bronze, silver, gold, athena, config)
│   ├── rds.yml              # RDS PostgreSQL + Secrets Manager
│   ├── dms.yml              # DMS replication instance, endpoints, task
│   ├── glue.yml             # Glue databases
│   ├── athena.yml           # Athena workgroup
│   ├── iam_roles.yml        # IAM roles (DMS, ECS, EventBridge)
│   ├── iam_users.yml        # IAM user with S3/Glue/Athena access
│   ├── container.yml        # ECR repository, ECS cluster and task definition
│   ├── event.yml            # EventBridge scheduler
│   └── ecs.env              # dbt runtime env vars (gitignored)
├── motorinc_dlh/            # dbt project (Athena + medallion)
│   ├── dbt_project.yml      # project config: bronze/silver/gold → Glue schemas + S3 data dirs
│   ├── vars.yml             # dbt variables
│   ├── README.md
│   ├── .gitignore           # ignores dbt_packages, logs, target
│   ├── macros/
│   │   ├── add_raw_partition.sql      # pre-hook: ADD PARTITION for raw Hive tables
│   │   ├── generate_schema_name.sql   # custom schema naming
│   │   └── macro_silver_erp_location.sql
│   ├── models/
│   │   ├── bronze/          # Glue `raw`: register latest DMS hourly partitions
│   │   │   ├── raw_customers_partition.sql
│   │   │   ├── raw_employees_partition.sql
│   │   │   ├── raw_offices_partition.sql
│   │   │   ├── raw_orderdetails_partition.sql
│   │   │   ├── raw_orders_partition.sql
│   │   │   └── raw_products_partition.sql
│   │   ├── silver/          # Iceberg `ods`: deduped staging (stg_*)
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_employees.sql
│   │   │   ├── stg_offices.sql
│   │   │   ├── stg_orderdetails.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_products.sql
│   │   └── gold/            # Iceberg `dwh`: business aggregates
│   │       └── load_sales_dataset.sql
│   ├── analyses/            # ad-hoc analyses (empty; .gitkeep)
│   ├── seeds/               # CSV seeds (empty; .gitkeep)
│   ├── snapshots/           # SCD snapshots (empty; .gitkeep)
│   ├── tests/               # data tests (empty; .gitkeep)
│   ├── logs/                # dbt logs (generated)
│   └── target/              # compiled SQL, manifest (generated; gitignored)
├── docker/                  # dbt + Athena Docker image (ECS)
│   ├── Dockerfile
│   ├── requirements-dbt.txt # dbt-core + dbt-athena-community (pip)
│   ├── profiles.yml         # Athena profile (env vars)
│   ├── run_container.sh     # run image locally
│   └── README.md
├── scripts/
│   ├── run_dbt.sh           # bronze → silver → gold (default container CMD)
│   └── load_sales.py        # Order simulator — continuously inserts orders into RDS to feed Full Load + CDC
└── .github/workflows/
    └── deploy_cf.yml        # CI/CD pipeline for CloudFormation deployments
```

---

## Infrastructure deployment

Stacks are deployed individually via GitHub Actions by including a tag in the commit message:

```
git commit -m "update bucket lifecycle [s3]"
```

| Tag | Stack deployed |
|-----|---------------|
| `[s3]` | S3 buckets |
| `[rds]` | RDS + Secrets Manager |
| `[roles]` | IAM roles |
| `[dms]` | DMS replication |
| `[glue]` | Glue databases |
| `[athena]` | Athena workgroup |
| `[users]` | IAM users |
| `[container]` | ECR + ECS cluster + task definition |
| `[event]` | EventBridge scheduler |

> **Deploy order matters (recommended):** `s3` → `rds` → `roles` → `athena` → `users` → `glue` → `dms` → `container` (ecs) → `event` bridge

Example de deploy por tag (um commit por stack):

```
git commit -m "deploy s3 [s3]"
git commit -m "deploy rds [rds]"
git commit -m "deploy roles [roles]"
git commit -m "deploy athena [athena]"
git commit -m "deploy users [users]"
git commit -m "deploy glue [glue]"
git commit -m "deploy dms [dms]"
git commit -m "deploy container (ecs/ecr) [container]"
git commit -m "deploy event bridge [event]"
```

Required GitHub secrets/variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_KEY_ID`, `AWS_DEFAULT_REGION`, `AWS_ACCOUNT_ID`, `AWS_ENV`, `AWS_SOLUTION`.

---

## How the pipeline runs

1. **Order simulator** (`scripts/load_sales.py`) inserts random orders into RDS continuously, generating CDC events.
2. **DMS** captures changes and writes Parquet files to the bronze S3 bucket, partitioned by hour.
3. **EventBridge** triggers the ECS Fargate task every 15 minutes.
4. **dbt** runs three steps:
   - **Bronze** — registers the latest hourly partition in Glue so new data is queryable.
   - **Silver** — deduplicates and merges rows into Iceberg `ods` tables.
   - **Gold** — aggregates silver tables into `dwh` datasets.
5. **Athena** queries `raw`, `ods`, and `dwh` at any point for reporting or ad-hoc analysis.

---

## Docker Image Deployment (ECR)

If the Docker image is not yet available in ECR, you must build and push it before the ECS task can run. This step is required after modifying the Dockerfile or requirements.

### Steps to build and push

1. **Authenticate Docker to ECR**:
   ```bash
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
   ```

2. **Create the ECR repository (if it doesn't exist)**:
   ```bash
   aws ecr create-repository --repository-name motorinc-datalakehouse --region us-east-1
   ```

3. **Build the Docker image**:
   ```bash
   cd docker
   docker build -t motorinc-datalakehouse .
   ```

4. **Tag the image**:
   ```bash
   docker tag motorinc-datalakehouse:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/motorinc-datalakehouse:latest
   ```

5. **Push to ECR**:
   ```bash
   docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/motorinc-datalakehouse:latest
   ```

### Local testing

To run the container locally:
```bash
cd docker
./run_container.sh
```

> **Note**: Ensure the `.env` file is present in the project root with valid AWS credentials before running locally.
