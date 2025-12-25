import pandas as pd

def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing_cols = [col for col in cols if col not in df.columns]
    
    assert not missing_cols, f"Missing required columns: {missing_cols}"


def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None: #!!!!
    assert not df.empty , f"{name} is empty."
    


def assert_unique_key(df: pd.DataFrame, key: str, *, allow_na: bool = False) -> None:
    assert key in df.columns, f"Key column '{key}' not found in DataFrame."

    
    if not allow_na:
        assert df[key].notna().all(), f"Key column '{key}' contains missing values."

    
    assert not df[key].duplicated().any(), f"Duplicate values found in key column: {key}"
    

def assert_in_range(s: pd.Series, lo=None, hi=None, name: str = "value") -> None: #!!!!
    s_valid = s[s.notna()]
    if lo is not None:
        assert (s_valid >= lo).all(), f"{name} contains values below the minimum of {lo}."

    if hi is not None:
        assert (s_valid <= hi).all(), f"{name} contains values above the maximum of {hi}."
        

    