from pathlib import Path
import sys
import logging


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from data_workflow.config import make_paths
from data_workflow.io import read_orders_csv, write_parquet, read_parquet, read_users_csv
from data_workflow.transforms import enforce_schema

logger = logging.getLogger(__name__)


paths = make_paths(ROOT)
def main() -> None:
    #Run: ETL
    orders_csv = read_orders_csv(paths.raw / 'orders.csv') #E: EXrract
    users_csv = read_users_csv(paths.raw / 'users.csv') #E: Extract

    orders_csv = enforce_schema(orders_csv) #T: Transform

    write_parquet(orders_csv, paths.processed / 'orders.parquet') #L: Load
    write_parquet(users_csv, paths.processed / 'users.parquet') #L: Load

    logger.info("Row Count: %d", len(orders_csv))
    logger.info("Paths: %s", paths.processed / 'orders.parquet')



if __name__ == '__main__':
    main()