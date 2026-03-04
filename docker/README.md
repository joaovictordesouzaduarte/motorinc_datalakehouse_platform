# dbt + Athena image for ECS

## Build

**You must run from the repository root** (not from inside `docker/`), so that the build context includes `motorinc_dlh/`, `scripts/`, and `docker/`:

```bash
cd /path/to/dev-repository
docker build -f docker/Dockerfile -t dbt-repository-cf-us-east-1-086997587178-sandbox-dlh .
```

To push to ECR (after authenticating):

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 086997587178.dkr.ecr.us-east-1.amazonaws.com
docker tag dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest 086997587178.dkr.ecr.us-east-1.amazonaws.com/dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest
docker push 086997587178.dkr.ecr.us-east-1.amazonaws.com/dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest
```

## Run locally

The image includes a default `profiles.yml` that reads from **env vars**. Set at least:

- `AWS_REGION` – e.g. `us-east-1`
- `S3_STAGING_DIR` – e.g. `s3://your-athena-results-bucket/dbt/`

Example (task role or local AWS credentials):

```bash
docker run --rm \
  -e AWS_REGION=us-east-1 \
  -e S3_STAGING_DIR=s3://your-athena-query-results-bucket/dbt/ \
  your-ecr-repo/dbt-athena:latest
```

To use your own `profiles.yml` instead, mount it:

```bash
docker run --rm -v "$HOME/.dbt:/app/profiles" -e AWS_REGION=us-east-1 -e S3_STAGING_DIR=s3://... \
  your-ecr-repo/dbt-athena:latest
```

## ECS

- **Task role**: IAM role with Athena, Glue, and S3 permissions (no access keys needed).
- **Env vars**: The task definition in `iac-repository/container.yml` sets `AWS_REGION`, `S3_STAGING_DIR` (full URI), and `ATHENA_WORK_GROUP` so the container always receives them. Redeploy the CloudFormation stack after updating `container.yml`.
- **Command**: default runs the full pipeline script; override to run a single step if needed.
