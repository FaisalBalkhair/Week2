# Week 2 — ETL & EDA Project

## Setup

## Create virtual environment:

uv venv -p 3.11

## Activate environment (if not already activated):

source .venv/bin/activate

## Install dependencies:

uv pip install -r pyproject.toml

## Then run:

uv run scripts/run_day1_load.py
uv run scripts/run_day2_clean.py
uv run scripts/run_day3_build_analytics.py
uv run scripts/run_etl.py

## Expected Output

After running the ETL script, the following files will be created:
data/processed/analytics_table.parquet
data/processed/orders_clean.parquet
data/processed/orders.parquet
data/processed/users.parquet
data/processed/run_meta.json

## Run EDA Notebook

Open:
notebooks/eda.ipynb

### Running the notebook will generate figures in:

reports/figures/
