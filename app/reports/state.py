"""Report task state machine (spec section 12)."""
from __future__ import annotations

from enum import Enum


class ReportState(str, Enum):
    ACCEPTED = "accepted"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"  # future state, reserved
    CANCELLED = "cancelled"  # future state, reserved


_ALLOWED_TRANSITIONS: dict[ReportState, set[ReportState]] = {
    ReportState.ACCEPTED: {ReportState.QUEUED},
    ReportState.QUEUED: {ReportState.PROCESSING},
    ReportState.PROCESSING: {
        ReportState.COMPLETED,
        ReportState.FAILED,
        ReportState.RETRYING,
    },
    ReportState.RETRYING: {ReportState.QUEUED},
    ReportState.COMPLETED: set(),
    ReportState.FAILED: {ReportState.RETRYING},
    ReportState.CANCELLED: set(),
}


def can_transition(current: ReportState, target: ReportState) -> bool:
    # Transition to self is explicitly allowed
    return (current == target) or (target in _ALLOWED_TRANSITIONS.get(current, set()))


class InvalidStateTransition(Exception):
    pass


def assert_transition(current: ReportState, target: ReportState) -> None:
    if not can_transition(current, target):
        raise InvalidStateTransition(f"Cannot transition {current} -> {target}")
