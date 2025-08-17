import pandas as pd

def validate_and_enrich(df: pd.DataFrame) -> pd.DataFrame:
    required = ["id","facility_id","date","tests","cases","deaths"]
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Types and sanity checks
    df = df.copy()
    df["id"] = df["id"].astype(int)
    df["tests"] = df["tests"].astype(int)
    df["cases"] = df["cases"].astype(int)
    df["deaths"] = df["deaths"].astype(int)
    # Parse dates
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().any():
        raise ValueError("Invalid dates found")

    # Basic quality metrics
    df["positivity_rate"] = df.apply(lambda r: (r["cases"]/r["tests"]) if r["tests"]>0 else 0.0, axis=1)
    df["case_fatality_rate"] = df.apply(lambda r: (r["deaths"]/r["cases"]) if r["cases"]>0 else 0.0, axis=1)

    # Clamp outliers to realistic bounds
    df["positivity_rate"] = df["positivity_rate"].clip(0, 1)
    df["case_fatality_rate"] = df["case_fatality_rate"].clip(0, 0.2)

    # Deduplicate on (facility_id, date)
    df = df.sort_values(["facility_id","date","id"]).drop_duplicates(subset=["facility_id","date"], keep="last")
    return df
