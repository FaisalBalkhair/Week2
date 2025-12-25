from pathlib import Path
import pandas as pd


def read_orders_csv(path: Path) -> pd.DataFrame:
    """Read orders data from a CSV file."""
    orders_file = pd.read_csv(path, dtype={"order_id": "string", "user_id": "string"}, 
                              na_values=["", "NA", "N/A", "null", "None"],
                              keep_default_na=True)
    
    return orders_file


def read_users_csv(path: Path) -> pd.DataFrame:
    """Read users data from a CSV file."""
    users_files = pd.read_csv(path, dtype={"user_id": "string"},
                              na_values=["", "NA", "N/A", "null", "None"],
                              keep_default_na=True)
    return users_files




def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to a Parquet file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def read_parquet(path: Path) -> pd.DataFrame:
    """Read DataFrame from a Parquet file."""
    return pd.read_parquet(path)