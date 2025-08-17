import io
import time
from typing import List, Dict, Any, Optional
import requests
import pandas as pd

def fetch_source(source_url: Optional[str]) -> pd.DataFrame:
    """Fetch from API or fallback to local CSV (simulated)."""
    if source_url:
        resp = requests.get(source_url, timeout=30)
        resp.raise_for_status()
        # Expecting JSON list of objects; adjust as needed
        data = resp.json()
        return pd.DataFrame(data)
    # Fallback: generate synthetic daily dataset
    now = int(time.time())
    data = [
        {"id": i, "value": i * 2, "ts": now}
        for i in range(1, 11)
    ]
    return pd.DataFrame(data)
