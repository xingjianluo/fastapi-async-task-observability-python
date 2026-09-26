import pytest

from app.downstream import call_downstream
from app.errors import DownstreamError, DownstreamTimeout, ErrorCategory

pytestmark = pytest.mark.anyio


async def test_success():
    result = await call_downstream("hello", timeout=1.0)
    assert result == {"echo": "hello", "ok": True}


async def test_timeout_normalized():
    with pytest.raises(DownstreamTimeout) as exc:
        await call_downstream("x", delay=1.0, timeout=0.05)
    assert exc.value.category == ErrorCategory.DOWNSTREAM_TIMEOUT
    assert "exceeded_timeout_seconds" in exc.value.reason


async def test_failure_normalized_not_leaked():
    with pytest.raises(DownstreamError) as exc:
        await call_downstream("x", fail=True, timeout=1.0)
    assert exc.value.category == ErrorCategory.DOWNSTREAM_ERROR
    # 底层异常细节不进入对外消息
    assert "internal db exploded" not in exc.value.message
    # 但保留为 cause 供内部排查
    assert isinstance(exc.value.cause, RuntimeError)


async def test_timeout_log_contains_correlation_id(log_records):
    from app.context import TraceContext, use_context

    with use_context(TraceContext(correlation_id="corr-test-123")):
        with pytest.raises(DownstreamTimeout):
            await call_downstream("x", delay=1.0, timeout=0.01)
    assert any("corr-test-123" in r for r in log_records)
    assert any("downstream_timeout" in r for r in log_records)
    assert any("normalize_error" in r for r in log_records)
