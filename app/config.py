"""Central configuration for the Financial Reports Service.

All values are overridable via environment variables so the same code runs
unchanged across docker-compose, tests and future deployments.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # --- Redis / queue ---
    redis_url: str = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    stream_name: str = os.environ.get("REPORTS_STREAM", "reports:tasks")
    consumer_group: str = os.environ.get("REPORTS_CONSUMER_GROUP", "report-workers")
    pending_claim_timeout_ms: int = int(
        os.environ.get("PENDING_CLAIM_TIMEOUT_MS", str(5 * 60 * 1000))
    )

    # --- Admission control / backpressure (spec 14) ---
    max_queue_depth: int = int(os.environ.get("MAX_QUEUE_DEPTH", "1000"))

    # --- Worker ---
    # Tasks a single worker process keeps in flight. Defaults to 1 so the
    # process behaves sequentially unless explicitly opted in; the shared
    # Redis rate limiter still caps SEC traffic across all workers.
    worker_concurrency: int = max(1, int(os.environ.get("WORKER_CONCURRENCY", "1")))

    # Worker-process Prometheus endpoint. The worker increments most of the
    # job/SEC/PDF metrics, and the API cannot expose another process's
    # registry, so the worker serves its own. Set to 0 to disable.
    worker_metrics_port: int = int(os.environ.get("WORKER_METRICS_PORT", "9100"))

    # --- SEC integration (spec 15, 16) ---
    sec_user_agent: str = os.environ.get(
        "SEC_USER_AGENT", "FinancialReportsService/1.0 (dev; admin@example.com)"
    )
    sec_rate_limit_per_second: float = float(
        os.environ.get("SEC_RATE_LIMIT_PER_SECOND", "8")
    )
    require_sec_user_agent: bool = _env_bool("REQUIRE_SEC_USER_AGENT", True)
    environment: str = os.environ.get("ENVIRONMENT", "development")

    sec_submissions_base_url: str = "https://data.sec.gov/submissions"
    sec_archives_base_url: str = "https://www.sec.gov/Archives/edgar/data"
    sec_company_tickers_url: str = "https://www.sec.gov/files/company_tickers.json"

    # --- Retry policy (spec 18) ---
    retry_max_attempts: int = int(os.environ.get("SEC_RETRY_MAX_ATTEMPTS", "5"))
    retry_base_delay_seconds: float = float(
        os.environ.get("SEC_RETRY_BASE_DELAY_SECONDS", "1.0")
    )
    retry_max_delay_seconds: float = float(
        os.environ.get("SEC_RETRY_MAX_DELAY_SECONDS", "60.0")
    )

    # --- Storage ---
    storage_root: str = os.environ.get("STORAGE_ROOT", "/data/reports")

    # --- CIK mapping refresh (spec 3.2) ---
    cik_mapping_path: str = os.environ.get(
        "CIK_MAPPING_PATH", "/data/cik_mapping.json"
    )
    cik_mapping_refresh_seconds: int = int(
        os.environ.get("CIK_MAPPING_REFRESH_SECONDS", str(24 * 60 * 60))
    )

    # --- Filing selection policy (spec 6) ---
    include_amended_by_default: bool = _env_bool("INCLUDE_AMENDED_BY_DEFAULT", False)

    def validate_for_production(self) -> None:
        """Fail fast if required production configuration is missing.

        Spec 16: a descriptive SEC User-Agent must not be optional in
        production.
        """
        if self.environment == "production" and self.require_sec_user_agent:
            placeholder_markers = ("example.com", "dev;")
            if any(marker in self.sec_user_agent for marker in placeholder_markers):
                raise RuntimeError(
                    "SEC_USER_AGENT must be set to a real organization name and "
                    "monitored contact email before running in production."
                )


settings = Settings()
