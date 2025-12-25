
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import pandas as pd
import logging


from data_workflow.quality import require_columns, assert_non_empty, assert_unique_key
from data_workflow.transforms import(
    enforce_schema, parse_datetime, 
    add_time_parts, winsorize, add_outlier_flag, normalize_text,
    apply_mapping, add_missing_flags,

    )
from data_workflow.joins import safe_left_join
from data_workflow.io import write_parquet

@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

def load_inputs(config: ETLConfig) -> dict:
    
    orders = pd.read_csv(config.raw_orders, dtype={"order_id": "string", "user_id": "string"}, 
                              na_values=["", "NA", "N/A", "null", "None"],
                              keep_default_na=True)
    users = pd.read_csv(config.raw_users, dtype={"user_id": "string"},
                              na_values=["", "NA", "N/A", "null", "None"],
                              keep_default_na=True)
    
    return orders, users



def transforms(orders: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    
    require_columns(orders, ["order_id","user_id","amount","quantity","created_at","status"])
    require_columns(users, ["user_id","country","signup_date"])
    assert_non_empty(orders, name="orders")
    assert_non_empty(users, name="users")   
    assert_unique_key(users, key="user_id")

    orders_transformation = enforce_schema(orders)

    status_normalized = normalize_text(orders_transformation["status"])
    mapping = {
        "paid": "paid",
        "refunded": "refund",
        "refund": "refund",
        }
    
    orders_transformation = orders_transformation.assign(status_clean = apply_mapping(status_normalized, mapping))
    orders_transformation = add_missing_flags( orders_transformation, cols=["amount", "quantity"])
    orders_transformation = parse_datetime(orders_transformation, "created_at", utc=True)
    orders_transformation = add_time_parts(orders_transformation, "created_at")
    joined = safe_left_join(orders_transformation, users, validate="many_to_one", on="user_id")
    joined = add_outlier_flag(joined, col="amount", k=1.5)

    return joined



def load_outputs(analytics:pd.DataFrame, users:pd.DataFrame, cfg:ETLConfig)-> None:
    write_parquet(analytics, cfg.out_analytics)
    write_parquet(users, cfg.out_users)


def write_run_meta(cfg, analytics): #!!!
    missing_created_at = int(analytics["created_at"].isna().sum())
    country_match_rate = 1.0 - float(analytics["country"].isna().mean())
    
    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows_out": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }
    
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


logger = logging.getLogger(__name__)

def run_etl(cfg: ETLConfig) -> None:
    logger.info("Starting ETL process")
    orders, users = load_inputs(cfg)
    logger.info("Inputs loaded successfully")
    
    analytics = transforms(orders, users)
    logger.info("Transformations applied successfully")
    
    load_outputs(analytics, users, cfg)
    logger.info("Outputs written successfully")
    
    write_run_meta(cfg, analytics)
    logger.info("ETL process completed")