
import pandas as pd


def safe_left_join(left:pd.DataFrame, right:pd.DataFrame, validate : str, on=None , suffixes:tuple[str, str] = ("", "_r")):
    joined = left.merge(right, how="left", on=on, validate=validate, suffixes=suffixes)
    assert len(joined) == len(left), "Row count changed (possible join explosion)"
    return joined
    
    