import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from driutils.json_logger import ErrorType, LogEntry, json_formatter, log_extras


def _parse(output: str) -> dict:
    """Parse formatter output back to a dict, reversing the loguru brace escaping."""
    return json.loads(output.strip().replace("{{", "{").replace("}}", "}"))


def _make_record(
    message: str = "test message",
    level: str = "INFO",
    name: str = "my.module",
    line: int = 42,
    thread_id: int = 12345,
    extra: dict | None = None,
    exception=None,
) -> dict:
    """Build a minimal fake loguru record."""

    class _Level:
        def __init__(self, name):
            self.name = name

    class _Thread:
        def __init__(self, id_):
            self.id = id_

    return {
        "time": datetime(2026, 6, 9, 13, 0, 0, tzinfo=timezone.utc),
        "message": message,
        "level": _Level(level),
        "name": name,
        "line": line,
        "thread": _Thread(thread_id),
        "extra": extra or {},
        "exception": exception,
    }


class TestLogEntry:
    def test_optional_fields_default_to_none(self) -> None:
        """All service-specific fields should be None when not supplied."""
        entry = LogEntry(time="t", msg="m", level="INFO", service="svc", loc="a:1", thread=1)
        assert entry.ingestion_batch_id is None
        assert entry.api_path is None
        assert entry.api_method is None
        assert entry.api_status_code is None
        assert entry.error_type is None

    def test_extra_fields_forbidden(self) -> None:
        """Unknown fields should be rejected."""
        with pytest.raises(ValidationError):
            LogEntry(
                time="t", msg="m", level="INFO", service="svc", loc="a:1", thread=1,
                unknown_field="bad",
            )

    def test_service_specific_fields_accepted(self) -> None:
        """Known service-specific fields should be accepted."""
        entry = LogEntry(
            time="t", msg="m", level="INFO", service="svc", loc="a:1", thread=1,
            ingestion_batch_id="abc-123",
            api_path="/v1/data",
        )
        assert entry.ingestion_batch_id == "abc-123"
        assert entry.api_path == "/v1/data"


class TestErrorType:
    def test_all_fields_default_to_none(self) -> None:
        error = ErrorType()
        assert error.type is None
        assert error.msg is None
        assert error.stacktrace is None

    def test_fields_set_correctly(self) -> None:
        error = ErrorType(type="ValueError", msg="bad value", stacktrace="Traceback...")
        assert error.type == "ValueError"
        assert error.msg == "bad value"


class TestLogExtras:
    def test_valid_field_does_not_raise(self) -> None:
        with log_extras({"ingestion_batch_id": "abc-123"}):
            pass

    def test_invalid_field_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown log fields"):
            with log_extras({"not_a_real_field": "value"}):
                pass


class TestJsonFormatter:
    def test_output_is_valid_json(self) -> None:
        record = _make_record()
        output = json_formatter(record, service_name="test-service")
        parsed = _parse(output)
        assert isinstance(parsed, dict)

    def test_core_fields_present(self) -> None:
        record = _make_record()
        parsed = _parse(json_formatter(record, service_name="test-service").strip())
        for field in ("time", "msg", "level", "service", "loc", "error_type", "thread"):
            assert field in parsed

    def test_no_exception_error_type_is_null(self) -> None:
        record = _make_record()
        parsed = _parse(json_formatter(record, service_name="svc").strip())
        assert parsed["error_type"] is None

    def test_exception_populates_error_type(self) -> None:
        try:
            raise ValueError("something went wrong")
        except ValueError:
            import sys
            exc_info = sys.exc_info()

        record = _make_record(exception=exc_info)
        parsed = _parse(json_formatter(record, service_name="svc").strip())
        assert parsed["error_type"]["type"] == "ValueError"
        assert parsed["error_type"]["msg"] == "something went wrong"
        assert parsed["error_type"]["stacktrace"] is not None
