#!/usr/bin/env bash
# Run dbt image locally. S3_STAGING_DIR must be full URI (s3://bucket/prefix/).
# For ECS, env vars are set in the task definition (see iac-repository/container.yml).

set -e
IMAGE="${1:-dbt-repository-cf-us-east-1-086997587178-sandbox-dlh:latest}"

docker run -it --rm \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e AWS_REGION="${AWS_REGION:-us-east-1}" \
  -e S3_STAGING_DIR="${S3_STAGING_DIR:-s3://aws-athena-query-results-us-east-1-086997587178-sandbox-dlh/dbt/}" \
  -e ATHENA_WORK_GROUP="${ATHENA_WORK_GROUP:-data-viz}" \
  "$IMAGE"
