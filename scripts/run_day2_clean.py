import sys
import logging
from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from data_workflow.config import make_paths
from data_workflow.io import read_orders_csv, write_parquet
from data_workflow.transforms import enforce_schema, missingness_report, add_missing_flags, normalize_text
from data_workflow.quality import require_columns, assert_non_empty, assert_in_range

logger = logging.getLogger(__name__)
Paths = make_paths(ROOT)




def main() -> None:
    #Load raw CSV
    orders = read_orders_csv(Paths.raw / 'orders.csv')  

    #Run quality checks
    require_columns(orders, ["order_id","user_id","amount","quantity","created_at","status"])
    assert_non_empty(orders, name="orders")


    #enforce schema
    orders = enforce_schema(orders)



    #Creates missingness report
    miss_report = missingness_report(orders)
    reports_dir = ROOT / "reports"
    miss_report.to_csv(reports_dir /  "missingness_orders.csv")

    logger.info("Missingness report written to: %s", reports_dir /  "missingness_orders.csv")

    #Normalizes status field
    orders = orders.assign(status = normalize_text(orders["status"]))

    #Adds missing flags for `amount` and `quantity`
    orders = add_missing_flags( orders, cols=["amount", "quantity"])

    #Validates ranges (amount >= 0, quantity >= 0)
    assert_in_range(orders["amount"], lo=0, name="amount")
    assert_in_range(orders["quantity"], lo=0, name="quantity")


    #Writes parquet
    write_parquet(orders, Paths.processed / "orders_clean.parquet")
    
    logger.info("Clean orders written to: %s", Paths.processed / "orders_clean.parquet")
    logger.info("Final row count: %d", len(orders))


    
    # read = pd.read_parquet(Paths.processed / "orders_clean.parquet")
    # print(read.head())

if __name__ == "__main__":
    main()
