import time
from prefect import flow, task
from src.config import settings
from src.domains.health_ingest import fetch_local_csv
from src.domains.health_transform import validate_and_enrich
from src.domains.health_load import load_health
from src.monitoring import push_metrics_with_extras

@task(retries=3, retry_delay_seconds=10)
def ingest(path: str):
    return fetch_local_csv(path)

@task
def transform(df):
    return validate_and_enrich(df)

@task
def load(df):
    return load_health(df, settings)

@task
def metrics(status: str, rows: int, started_at: float, extras: dict):
    duration = time.time() - started_at
    try:
        push_metrics_with_extras(settings.pushgateway_url, job="health_pipeline", status=status, rows=rows, duration_s=duration, extras=extras)
    except Exception:
        pass

@flow(name="health_surveillance_pipeline")
def run_health_pipeline(csv_path: str = "data/health_surveillance_daily.csv"):
    started = time.time()
    rows = 0
    extras = {}
    try:
        raw = ingest.submit(csv_path).result()
        cleaned = transform.submit(raw).result()
        rows = load.submit(cleaned).result()
        # example quality metrics
        extras = {
            "null_rows": 0,
            "facilities": cleaned["facility_id"].nunique(),
        }
        metrics.submit("success", rows, started, extras)
    except Exception:
        metrics.submit("failure", rows, started, extras)
        raise

if __name__ == "__main__":
    run_health_pipeline()
