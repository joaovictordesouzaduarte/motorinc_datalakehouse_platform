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

## Local install (venv; avoid PEP 668 / “externally-managed-environment”)

Run from the **repository root**, not only inside `docker/`, so paths match the Dockerfile.

```bash
cd /path/to/motorinc_datalakehouse_platform
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -r docker/requirements-dbt.txt
```

Use **`python -m pip`** (not bare `pip`) so you always install into the interpreter that is active. If `which pip` does not end in `.venv/bin/pip`, the venv is not active or another `pip` is first on `PATH`.

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
