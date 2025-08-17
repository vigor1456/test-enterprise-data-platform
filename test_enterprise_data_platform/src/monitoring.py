import time
from typing import Optional
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

def push_metrics(pushgateway_url: str, job: str, status: str, rows: int, duration_s: float):
    registry = CollectorRegistry()
    g_status = Gauge('pipeline_status', 'Pipeline status (1=success,0=failure)', registry=registry)
    g_rows = Gauge('pipeline_rows_processed', 'Rows processed', registry=registry)
    g_duration = Gauge('pipeline_duration_seconds', 'End-to-end duration (s)', registry=registry)

    g_status.set(1 if status == 'success' else 0)
    g_rows.set(rows)
    g_duration.set(duration_s)

    push_to_gateway(pushgateway_url, job=job, registry=registry)


def push_metrics_with_extras(pushgateway_url: str, job: str, status: str, rows: int, duration_s: float, extras: dict | None = None):
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
    registry = CollectorRegistry()
    g_status = Gauge('pipeline_status', 'Pipeline status (1=success,0=failure)', registry=registry)
    g_rows = Gauge('pipeline_rows_processed', 'Rows processed', registry=registry)
    g_duration = Gauge('pipeline_duration_seconds', 'End-to-end duration (s)', registry=registry)
    g_status.set(1 if status == 'success' else 0)
    g_rows.set(rows)
    g_duration.set(duration_s)
    if extras:
        for k, v in extras.items():
            Gauge(f'pipeline_{k}', f'Extra metric: {k}', registry=registry).set(float(v))
    push_to_gateway(pushgateway_url, job=job, registry=registry)
