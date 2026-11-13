from __future__ import annotations

from datetime import UTC, datetime, timedelta

from .models import SessionRecord, SessionStatus


class InvalidSessionTransitionError(Exception):
    def __init__(self, session_id: str, current: str, target: str) -> None:
        super().__init__(f"invalid session transition {current} -> {target} for {session_id}")
        self.session_id = session_id
        self.current = current
        self.target = target


ALLOWED_TRANSITIONS: dict[SessionStatus, frozenset[SessionStatus]] = {
    "active": frozenset({"handoff_pending", "solved", "closed"}),
    "handoff_pending": frozenset({"human_active", "solved", "closed"}),
    "human_active": frozenset({"solved", "closed"}),
    "solved": frozenset({"active", "handoff_pending", "closed"}),
    "closed": frozenset(),
}


def now_utc() -> datetime:
    return datetime.now(UTC)


def can_transition(current: SessionStatus, target: SessionStatus) -> bool:
    return current == target or target in ALLOWED_TRANSITIONS[current]


def transition_session(
    session: SessionRecord,
    target: SessionStatus,
    *,
    reason: str | None = None,
    now: datetime | None = None,
) -> bool:
    """Move a session to a new lifecycle state and update its timestamps.

    Returns True when the stored state changed. Repeating the same transition is
    idempotent so retries and multi-instance sweeps cannot fail a valid close.
    """

    now = now or now_utc()
    current = session.status
    if not can_transition(current, target):
        raise InvalidSessionTransitionError(session.id, current, target)
    if current == target:
        return False

    if current == "solved" and target == "active":
        session.reopened_count += 1
        session.solved_at = None
        session.satisfaction_prompted_at = None

    session.status = target
    session.status_reason = reason
    session.updated_at = now

    if target == "handoff_pending":
        session.handoff_requested_at = session.handoff_requested_at or now
        session.handoff_reason = reason or session.handoff_reason
    elif target == "solved":
        session.solved_at = now
        session.closed_at = None
        session.close_reason = None
    elif target == "closed":
        session.closed_at = now
        session.close_reason = reason or session.close_reason
    return True


def idle_close_target(
    session: SessionRecord,
    *,
    idle_close_seconds: int,
    solved_close_seconds: int,
    now: datetime | None = None,
) -> tuple[SessionStatus, str] | None:
    now = now or now_utc()
    if session.status == "active":
        if now - session.last_activity_at >= timedelta(seconds=idle_close_seconds):
            return "closed", "idle_timeout"
    elif (
        session.status == "solved"
        and session.solved_at is not None
        and now - session.solved_at >= timedelta(seconds=solved_close_seconds)
    ):
        return "closed", "solved_timeout"
    return None


def can_reopen(session: SessionRecord, *, reopen_window_seconds: int, now: datetime | None = None) -> bool:
    if session.status != "solved" or session.solved_at is None:
        return False
    now = now or now_utc()
    return now - session.solved_at <= timedelta(seconds=reopen_window_seconds)


def is_human_owned(status: SessionStatus) -> bool:
    return status in {"handoff_pending", "human_active"}


__all__ = [
    "ALLOWED_TRANSITIONS",
    "InvalidSessionTransitionError",
    "can_reopen",
    "can_transition",
    "idle_close_target",
    "is_human_owned",
    "now_utc",
    "transition_session",
]
