import pytest

from app.errors import InvalidStateTransitionError
from app.statemachine import TERMINAL_STATUSES, TaskStatus, ensure_transition


def test_legal_transitions():
    ensure_transition(TaskStatus.PENDING, TaskStatus.RUNNING)
    ensure_transition(TaskStatus.PENDING, TaskStatus.CANCELLED)
    ensure_transition(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)
    ensure_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
    ensure_transition(TaskStatus.RUNNING, TaskStatus.TIMED_OUT)
    ensure_transition(TaskStatus.PENDING, TaskStatus.ABORTED)


def test_illegal_transition_skipping_running():
    with pytest.raises(InvalidStateTransitionError) as exc:
        ensure_transition(TaskStatus.PENDING, TaskStatus.SUCCEEDED)
    assert exc.value.category == "invalid_state_transition"
    assert exc.value.current == "pending"
    assert exc.value.target == "succeeded"
    assert exc.value.reason == "transition_not_allowed"


def test_terminal_states_cannot_transition():
    for terminal in TERMINAL_STATUSES:
        with pytest.raises(InvalidStateTransitionError) as exc:
            ensure_transition(terminal, TaskStatus.RUNNING)
        assert exc.value.reason == "current_status_is_terminal"


def test_running_cannot_return_to_pending():
    with pytest.raises(InvalidStateTransitionError) as exc:
        ensure_transition(TaskStatus.RUNNING, TaskStatus.PENDING)
    assert exc.value.reason == "transition_not_allowed"
