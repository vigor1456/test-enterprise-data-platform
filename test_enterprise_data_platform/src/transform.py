import pandas as pd

REQUIRED_COLUMNS = {"id": int, "value": int, "ts": int}

def validate_schema(df: pd.DataFrame) -> pd.DataFrame:
    missing = set(REQUIRED_COLUMNS.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    for col, typ in REQUIRED_COLUMNS.items():
        try:
            df[col] = df[col].astype(typ)
        except Exception as e:
            raise ValueError(f"Type cast failed for {col}: {e}")
    return df

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["id"]).copy()

def enrich(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["value_x3"] = df["value"] * 3
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = validate_schema(df)
    df = deduplicate(df)
    df = enrich(df)
    return df
