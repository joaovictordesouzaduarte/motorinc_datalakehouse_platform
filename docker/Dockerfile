# dbt + Athena adapter image for ECS
# Build from REPOSITORY ROOT:  docker build -f docker/Dockerfile -t your-ecr-repo/dbt-athena:latest .
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/profiles

# Install system deps (optional: for AWS CLI if you need it)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dbt and Athena adapter
COPY docker/requirements-dbt.txt /app/requirements-dbt.txt
RUN pip install --no-cache-dir -r /app/requirements-dbt.txt

# Copy dbt project (motorinc_dlh)
COPY motorinc_dlh /app/motorinc_dlh

# Copy run script (bronze partitions → silver → gold)
COPY scripts/run_dbt.sh /app/scripts/run_dbt.sh
RUN chmod +x /app/scripts/run_dbt.sh

# Default profiles.yml (uses env vars: AWS_REGION, S3_STAGING_DIR; override by mounting /app/profiles)
RUN mkdir -p /app/profiles
COPY docker/profiles.yml /app/profiles/profiles.yml

WORKDIR /app/motorinc_dlh

# Default: run full pipeline (bronze partitions → silver → gold). Override in ECS task definition if needed.
ENTRYPOINT ["/bin/bash"]
CMD ["/app/scripts/run_dbt.sh"]
