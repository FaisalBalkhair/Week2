import sys
import logging
from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from data_workflow.config import make_paths
from data_workflow.io import read_parquet, write_parquet
from data_workflow.transforms import enforce_schema, missingness_report, add_missing_flags, normalize_text, parse_datetime, add_time_parts, winsorize, add_outlier_flag
from data_workflow.quality import require_columns, assert_non_empty, assert_in_range, assert_unique_key
from data_workflow.joins import safe_left_join

logger = logging.getLogger(__name__)
Paths = make_paths(ROOT)

def main() -> None:

    #Load cleaned orders data
    orders = pd.read_parquet(Paths.processed / "orders_clean.parquet")
    users = pd.read_parquet(Paths.processed / "users.parquet")

    #Validates inputs
    require_columns(orders, ["order_id","user_id","amount","quantity","created_at","status"])
    require_columns(users, ["user_id","country","signup_date"])

    assert_non_empty(orders, name="orders")
    assert_non_empty(users, name="users")   
    assert_unique_key(users, key="user_id")

    #Parses created_at and adds time parts
    orders = parse_datetime(orders, "created_at")
    orders = add_time_parts(orders, "created_at")

    #Joins orders
    joined = safe_left_join(orders, users, validate="many_to_one", on="user_id")

    #Winsorizes amount and adds outlier flag
    joined = joined.assign(amount_winsorizes = winsorize(joined["amount"], lo=0.01, hi=0.99))
    joined = add_outlier_flag(joined, col="amount")

    #Writes analytics_table.parquet

    write_parquet(joined, Paths.processed / "analytics_table.parquet")
    logger.info("Analytics table written to: %s", Paths.processed / "analytics_table.parquet")


    # read_parquetd = pd.read_parquet(Paths.processed / "analytics_table.parquet")
    # print(read_parquetd.head(15))

if __name__ == '__main__':
    main()