import pandas as pd

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    '''Enforce data types for orders DataFrame.'''
    df = df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount= pd.to_numeric(df["amount"],errors='coerce').astype("Float64"),
        quantity = pd.to_numeric(df["quantity"],errors='coerce').astype("Int64"),
    )
    return df



def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    '''Generate a missingness report for the DataFrame.'''
    missing_report = pd.DataFrame({
        "n_missing" : df.isna().sum(),
        "p_missing": df.isna().mean() * 100
    })
    missing_report = missing_report.sort_values(by='p_missing', ascending=False)

    return missing_report


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    '''Add missingness flag columns for specified columns.'''
    for col in cols:
        flag_col = f"{col}__isna"
        df = df.assign(**{flag_col : df[col].isna()})
    return df

    


def normalize_text(s: pd.Series) -> pd.Series: #!!!!!!
    '''Normalize text by stripping whitespace, converting to lowercase, and collapsing spaces.'''
    return (s
            .str.strip()
            .str.casefold()
            .str.replace(r'\s+', ' ', regex=True))



def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series: #!!!!
    '''Apply a mapping to a Series, leaving unmapped values unchanged.'''
    mapped = s.map(mapping, na_action='ignore')
    return mapped




def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame: #!!!!!
    '''Deduplicate DataFrame by keeping the latest record based on a timestamp column.'''
    df_sorted = df.sort_values(by=ts_col)
    df_deduped = (df_sorted
                  .drop_duplicates(subset=key_cols, keep='last')
                  .reset_index(drop=True))
    return df_deduped
    # df_sorted = df.sort_values(by=ts_col)
    # df_sorted = df_sorted.drop_duplicates(keep='last')




#Day3 additions
 
def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    '''Parse a column as datetime, with optional UTC conversion.'''
    df = df.assign(**{col: pd.to_datetime(df[col], errors='coerce', utc=utc)})
    return df




def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame: #!!!!
    '''Add time part columns (day, year, month, day of week, hour) from a timestamp column.'''
    df = df.assign(
        **{
            f"{ts_col}_day": df[ts_col].dt.day,
            f"{ts_col}_year": df[ts_col].dt.year,
            f"{ts_col}_month": df[ts_col].dt.month,
            f"{ts_col}_dow": df[ts_col].dt.dayofweek,
            f"{ts_col}_hour": df[ts_col].dt.hour,
        }
    )
    return df   


def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    '''Calculate IQR bounds for outlier detection.'''
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k*iqr), float(q3 + k*iqr)



def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    '''Winsorize a Series by capping values at specified quantiles.'''
    lower = s.quantile(lo)
    upper = s.quantile(hi)
    return s.clip(lower=lower, upper=upper)

#optional

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    '''Add an outlier flag column based on IQR bounds.'''
    lo, hi = iqr_bounds(df[col], k=k)
    flag_col = f"{col}__is_outlier"
    df = df.assign(**{flag_col: (df[col] < lo) | (df[col] > hi)})
    return df
