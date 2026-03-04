# Data Lakehouse pipeline

This repository implements a **data lakehouse** on AWS: transactional data is replicated from a PostgreSQL OLTP into S3, then transformed and modeled with dbt, and queried with Athena. The goal is to centralize raw replication, cleansed layers, and analytical datasets in one place, using managed services and minimal custom ETL.

---

## Motivation

The project aims to build a modern analytics stack that:

- **Keeps the OLTP as the single source of truth** — no direct reporting queries against the operational database.
- **Replicates changes continuously** — full load for initial sync, then ongoing replication (CDC) so the lake stays up to date.
- **Separates storage and compute** — data lives in S3; Glue and Athena provide the catalog and SQL engine without managing databases or clusters.
- **Layers data by quality and purpose** — raw (bronze), cleansed and deduplicated (silver), and business-level aggregates (gold), following a medallion-style architecture.

dbt orchestrates the transformations and is run inside a Docker image on ECS, so the pipeline can be scheduled or triggered without maintaining a dedicated dbt host.

---

## Architecture

### OLTP: PostgreSQL on RDS

The source system is a PostgreSQL database on Amazon RDS. It holds the operational data: customers, orders, order details, products, employees, offices, and related tables. This database remains the system of record; the lakehouse is a read-only replica for analytics.

### Replication: AWS DMS

**AWS Database Migration Service (DMS)** is used to replicate data from RDS to S3:

- **Full load** — initial one-time copy of the tables into the data lake.
- **Ongoing replication (CDC)** — continuous capture of inserts, updates, and deletes via the database transaction log, so new and changed rows are written to S3 as they occur.

DMS writes to S3 in a partitioned structure (e.g. by year, month, day, hour). The raw layer in the lake is essentially a copy of the OLTP state over time, with partition metadata registered in the Glue Data Catalog so Athena can query it.

### Data lake: S3 and Glue

**Amazon S3** is the storage layer. Buckets are organized by layer:

- **Bronze (raw)** — landing zone for DMS. Data is stored in partition folders (e.g. `year_stream`, `month_stream`, `day_stream`, `hour_stream`). Tables are defined in the Glue Data Catalog under a `raw` schema so Athena can run SQL against them.
- **Silver** — cleansed, deduplicated tables (one row per business key, latest version). Stored as Apache Iceberg tables in S3, registered in Glue under an `ods` schema.
- **Gold** — business-level aggregates and datasets (e.g. sales by office and date). Also Iceberg in S3, registered under a `dwh` schema.

**AWS Glue Data Catalog** holds the table definitions (schemas, partition keys, and locations) for raw Hive-style tables and for Iceberg tables. There is no separate database server; Glue is the metadata store that Athena uses to resolve table names and paths.

### Query: Athena

**Amazon Athena** is the SQL engine. It reads table metadata from the Glue catalog and executes queries against data in S3. Workgroups and result locations are configured so that dbt (and any other client) can run DDL and DML (e.g. `CREATE TABLE`, `MERGE`) for Iceberg tables and `ALTER TABLE ... ADD PARTITION` for the raw layer. Credentials are provided by the ECS task role when dbt runs in the cloud, or by local AWS configuration when developing.

---

## How the pipeline runs

1. **DMS** continuously writes changed rows from RDS into the bronze bucket, in partition folders.
2. **dbt** runs on a schedule or on demand (e.g. via ECS):
   - **Bronze step** — registers the current partition (year, month, day, hour) for each raw table in Glue, so new data is visible to queries.
   - **Silver step** — runs incremental Iceberg models that read from `raw.*`, deduplicate by business key (keeping the latest row per key), and merge into `ods.*` (e.g. `stg_customers`, `stg_orders`, `stg_products`).
   - **Gold step** — runs incremental models that join silver tables and write aggregated datasets (e.g. `sales_dataset`) into `dwh.*`.
3. **Athena** (or any tool that uses the Glue catalog) can then query `raw`, `ods`, and `dwh` tables for reporting and ad‑hoc analysis.

The dbt project lives in the `motorinc_dlh` folder; the Docker image packages it plus a run script and a dbt profile that reads connection settings from environment variables. See [docker/README.md](docker/README.md) for building the image, pushing to ECR, and running it on ECS.
