# dbt + Athena image for ECS

Image that runs the **motorinc_dlh** dbt project on ECS Fargate against Athena.

---

## Image contents

| Path | Description |
|------|-------------|
| `/app/motorinc_dlh` | dbt project (WORKDIR). |
| `/app/profiles/profiles.yml` | Profile `motorinc_dlh`; reads `AWS_REGION`, `S3_STAGING_DIR`, `ATHENA_WORK_GROUP` from env. |
| `/app/scripts/run_dbt.sh` | Default CMD: bronze → silver → gold. |

- Python 3.11-slim, dbt-core 1.11.x, dbt-athena-community 1.10.x.

---

## Build (from repository root)

```bash
docker build -f docker/Dockerfile -t dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest .
```

---

## ECS: environment variables

Set in the task definition. No env file required.

| Variable | Purpose |
|----------|---------|
| `AWS_REGION` | Athena/Glue region (default: `us-east-1`). |
| `S3_STAGING_DIR` | S3 URI for Athena results (e.g. `s3://bucket/dbt/`). |
| `ATHENA_WORK_GROUP` | Athena workgroup (default: `data-viz`). |

Task role must allow Athena, Glue, and S3 (bronze/silver/gold + Athena results bucket).

---

## Push to ECR

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 086997587178.dkr.ecr.us-east-1.amazonaws.com
docker tag dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest 086997587178.dkr.ecr.us-east-1.amazonaws.com/dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest
docker push 086997587178.dkr.ecr.us-east-1.amazonaws.com/dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest
```

---

## Default command

`/app/scripts/run_dbt.sh` runs in order:

1. **Bronze** — `dbt run --select bronze` (register raw partitions in Glue).
2. **Silver** — `dbt run --select silver` (incremental Iceberg `stg_*` in `ods`).
3. **Gold** — `dbt run --select gold` (incremental Iceberg `sales_dataset` in `dwh`).

Override the container command in the task definition to run a subset (e.g. `dbt run --select silver`).
