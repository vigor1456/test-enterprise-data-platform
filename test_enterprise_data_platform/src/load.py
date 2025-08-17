from typing import Any
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine(host: str, port: int, user: str, password: str, db: str):
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, future=True)

DDL = """
CREATE TABLE IF NOT EXISTS fact_values (
    id INTEGER PRIMARY KEY,
    value INTEGER NOT NULL,
    ts BIGINT NOT NULL,
    value_x3 INTEGER NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);
"""

UPSERT = """
INSERT INTO fact_values (id, value, ts, value_x3)
VALUES (:id, :value, :ts, :value_x3)
ON CONFLICT (id) DO UPDATE SET
  value = EXCLUDED.value,
  ts = EXCLUDED.ts,
  value_x3 = EXCLUDED.value_x3;
"""

def init_schema(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL))

def upsert(engine, df: pd.DataFrame):
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        for r in records:
            conn.execute(text(UPSERT), r)
