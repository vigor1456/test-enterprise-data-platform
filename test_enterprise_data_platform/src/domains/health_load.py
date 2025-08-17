from sqlalchemy import text
from src.load import get_engine
import pandas as pd

DDL = """
CREATE TABLE IF NOT EXISTS fact_health_surveillance (
    id INTEGER PRIMARY KEY,
    facility_id TEXT NOT NULL,
    date DATE NOT NULL,
    tests INTEGER NOT NULL,
    cases INTEGER NOT NULL,
    deaths INTEGER NOT NULL,
    positivity_rate DOUBLE PRECISION NOT NULL,
    case_fatality_rate DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_surv_fac_date ON fact_health_surveillance(facility_id, date);
"""

UPSERT = """
INSERT INTO fact_health_surveillance
(id, facility_id, date, tests, cases, deaths, positivity_rate, case_fatality_rate)
VALUES (:id, :facility_id, :date, :tests, :cases, :deaths, :positivity_rate, :case_fatality_rate)
ON CONFLICT (id) DO UPDATE SET
  facility_id = EXCLUDED.facility_id,
  date = EXCLUDED.date,
  tests = EXCLUDED.tests,
  cases = EXCLUDED.cases,
  deaths = EXCLUDED.deaths,
  positivity_rate = EXCLUDED.positivity_rate,
  case_fatality_rate = EXCLUDED.case_fatality_rate;
"""

def load_health(df: pd.DataFrame, settings):
    engine = get_engine(settings.db_host, settings.db_port, settings.db_user, settings.db_password, settings.db_name)
    with engine.begin() as conn:
        conn.execute(text(DDL))
        for rec in df.to_dict(orient="records"):
            conn.execute(text(UPSERT), rec)
    return len(df)
