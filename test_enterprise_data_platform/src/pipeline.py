import time
from datetime import datetime
from prefect import flow, task
from src.config import settings
from src.ingest import fetch_source
from src.transform import transform
from src.load import get_engine, init_schema, upsert
from src.monitoring import push_metrics

@task(retries=3, retry_delay_seconds=10)
def ingest_task(source_url: str):
    return fetch_source(source_url)

@task
def transform_task(df):
    return transform(df)

@task
def load_task(df):
    engine = get_engine(
        settings.db_host, settings.db_port, settings.db_user, settings.db_password, settings.db_name
    )
    init_schema(engine)
    upsert(engine, df)
    return len(df)

@task
def metrics_task(status: str, rows: int, started_at: float):
    duration = time.time() - started_at
    try:
        push_metrics(settings.pushgateway_url, job="daily_pipeline", status=status, rows=rows, duration_s=duration)
    except Exception:
        # Non-fatal: metrics push failure should not break the pipeline
        pass

@flow(name="daily_data_pipeline")
def run_pipeline():
    started_at = time.time()
    rows = 0
    try:
        raw = ingest_task.submit(settings.source_url).result()
        cleaned = transform_task.submit(raw).result()
        rows = load_task.submit(cleaned).result()
        metrics_task.submit("success", rows, started_at)
    except Exception as e:
        metrics_task.submit("failure", rows, started_at)
        raise

if __name__ == "__main__":
    run_pipeline()
