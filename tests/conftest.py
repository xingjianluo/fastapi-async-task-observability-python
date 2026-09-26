import logging

import pytest

from app.logging import StructuredFormatter, get_logger


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def log_records():
    """收集 app 结构化日志（JSON 字符串），用于断言关联标识与判定依据。"""
    records: list[str] = []

    class Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(self.format(record))

    collector = Collector()
    collector.setFormatter(StructuredFormatter())
    app_logger = get_logger()
    app_logger.addHandler(collector)
    try:
        yield records
    finally:
        app_logger.removeHandler(collector)
