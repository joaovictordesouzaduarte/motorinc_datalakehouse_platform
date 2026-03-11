# Data Lakehouse Pipeline

This repository implements a **data lakehouse** on AWS: transactional data is replicated from a PostgreSQL OLTP into S3, then transformed and modeled with dbt, and queried with Athena. Infrastructure is provisioned with CloudFormation and deployed via GitHub Actions.

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
├── motorinc_dlh/            # dbt project (models, macros, tests)
├── docker/                  # Dockerfile and entrypoint for the dbt image
├── scripts/
│   └── load_sales_v4.py     # Order simulator — continuously inserts orders into RDS to feed CDC
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

> **Deploy order matters:** `s3` → `rds` → `roles` → `dms` → `glue` → `athena` → `users` → `container` → `event`

Required GitHub secrets/variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_KEY_ID`, `AWS_DEFAULT_REGION`, `AWS_ACCOUNT_ID`, `AWS_ENV`, `AWS_SOLUTION`.

---

## How the pipeline runs

1. **Order simulator** (`scripts/load_sales_v4.py`) inserts random orders into RDS continuously, generating CDC events.
2. **DMS** captures changes and writes Parquet files to the bronze S3 bucket, partitioned by hour.
3. **EventBridge** triggers the ECS Fargate task every 15 minutes.
4. **dbt** runs three steps:
   - **Bronze** — registers the latest hourly partition in Glue so new data is queryable.
   - **Silver** — deduplicates and merges rows into Iceberg `ods` tables.
   - **Gold** — aggregates silver tables into `dwh` datasets.
5. **Athena** queries `raw`, `ods`, and `dwh` at any point for reporting or ad-hoc analysis.
