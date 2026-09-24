"""Metrics (spec section 24.4).

Uses `prometheus_client` counters/gauges/histograms so the /metrics
endpoint can be scraped from both the API and worker processes.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

jobs_accepted_total = Counter("jobs_accepted_total", "Report jobs accepted")
jobs_completed_total = Counter("jobs_completed_total", "Report jobs completed")
jobs_failed_total = Counter("jobs_failed_total", "Report jobs failed")
jobs_retried_total = Counter("jobs_retried_total", "Report jobs retried")

sec_requests_total = Counter("sec_requests_total", "SEC HTTP requests made")
sec_429_total = Counter("sec_429_total", "SEC HTTP 429 responses received")
sec_request_latency_seconds = Histogram(
    "sec_request_latency_seconds", "SEC HTTP request latency"
)
pdf_conversion_latency_seconds = Histogram(
    "pdf_conversion_latency_seconds", "PDF conversion latency"
)

queue_depth = Gauge("queue_depth", "Current report queue depth")
active_workers = Gauge("active_workers", "Number of active worker processes")
