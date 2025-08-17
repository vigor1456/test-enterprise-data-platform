import pandas as pd

def fetch_local_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)
