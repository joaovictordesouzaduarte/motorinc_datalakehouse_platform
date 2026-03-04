#!/usr/bin/env bash
set -e

# Project dir inside the container (same as Dockerfile WORKDIR)
PROJECT_DIR="${DBT_PROJECT_DIR:-/app/motorinc_dlh}"
cd "$PROJECT_DIR"

echo "==> 1/3 Bronze: activating partitions (raw_*_partition)"
dbt run --select bronze

echo "==> 2/3 Silver: running silver models"
dbt run --select silver

echo "==> 3/3 Gold: running gold models"
dbt run --select gold

echo "==> Done: bronze -> silver -> gold completed successfully."
