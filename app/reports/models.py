"""Report/task data model persisted alongside the state machine."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

from app.reports.identity import FilingIdentity, ReportIdentity
from app.reports.state import ReportState


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ReportTask:
    task_id: str
    identity: ReportIdentity
    state: ReportState = ReportState.ACCEPTED
    include_amended: bool = False
    filing: Optional[FilingIdentity] = None
    artifact_path: Optional[str] = None
    error: Optional[str] = None
    attempts: int = 0
    created_at: str = field(default_factory=_utcnow_iso)
    updated_at: str = field(default_factory=_utcnow_iso)

    def to_json(self) -> str:
        payload = asdict(self)
        return json.dumps(payload)

    @classmethod
    def from_json(cls, raw: str) -> "ReportTask":
        payload = json.loads(raw)
        payload["identity"] = ReportIdentity(**payload["identity"])
        if payload.get("filing"):
            payload["filing"] = FilingIdentity(**payload["filing"])
        payload["state"] = ReportState(payload["state"])
        return cls(**payload)
