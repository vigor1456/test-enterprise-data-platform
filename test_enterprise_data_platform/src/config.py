from pydantic import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "appuser"
    db_password: str = "apppass"
    db_name: str = "appdb"

    # Metrics
    pushgateway_url: str = "http://localhost:9091"

    # Source
    source_url: Optional[str] = None  # e.g., 'https://api.example.com/daily-data'

settings = Settings()
