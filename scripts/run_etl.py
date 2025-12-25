from pathlib import Path
import pandas as pd
import logging
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
sys.path.insert(0, str(SRC))

from data_workflow.etl import ETLConfig, run_etl

cfg = ETLConfig(
    root = ROOT,
    raw_orders= ROOT / "data" / "raw" / "orders.csv",
    raw_users= ROOT / "data" / "raw" / "users.csv",
    out_orders_clean = ROOT / "processed" / "orders_clean.parquet",
    out_analytics= ROOT / "data" / "processed" / "analytics_table.parquet",
    out_users= ROOT / "data" / "processed" / "users.parquet",
    run_meta= ROOT / "data" / "processed" / "run_meta.json",
)



if __name__ == '__main__':
    run_etl(cfg)