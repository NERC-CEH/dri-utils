import traceback
from collections.abc import Iterator
from contextlib import contextmanager

from loguru import logger
from pydantic import BaseModel, ConfigDict


class ErrorType(BaseModel):
    """Error attributes"""

    type: str | None = None
    msg: str | None = None
    stacktrace: str | None = None


class LogEntry(BaseModel):
    """Core log entry"""

    model_config = ConfigDict(extra="forbid")

    time: str
    msg: str
    level: str
    service: str
    loc: str
    error_type: ErrorType | None = None
    thread: int

    # Ingestion-specific
    ingestion_batch_id: str | None = None

    # API-specific
    api_path: str | None = None
    api_method: str | None = None
    api_status_code: str | None = None


@contextmanager
def log_extras(extras: dict) -> Iterator[dict]:
    """Set service-specific fields on the log record.

    Only fields defined on LogEntry are permitted.

    Args:
        extras: mapping of LogEntry field names to values.

    Raises:
        ValueError if key doesn't exist in the model
    """
    valid = set(LogEntry.model_fields.keys())
    invalid = set(extras.keys()) - valid
    if invalid:
        raise ValueError(f"Unknown log fields: {invalid}. Must be one of: {sorted(valid)}")
    with logger.contextualize(**extras):
        yield


def json_formatter(record: dict, service_name: str) -> str:
    """Serialize a loguru record to the standard JSON log format.

    Args:
        record: the loguru record object.
        service_name: the name of the service emitting the log.

    Returns:
        A newline-terminated JSON string.
    """
    error_type = None
    if record["exception"] is not None:
        exc_type, exc_value, exc_tb = record["exception"]
        error_type = ErrorType(
            type=exc_type.__name__ if exc_type else None,
            msg=str(exc_value) if exc_value else None,
            stacktrace="".join(traceback.format_tb(exc_tb)).strip() if exc_tb else None,
        )

    entry = LogEntry(
        time=record["time"].replace(tzinfo=None).isoformat(),
        msg=record["message"],
        level=record["level"].name,
        service=service_name,
        loc=f"{record['name']}:{record['line']}",
        error_type=error_type,
        thread=record["thread"].id,
        **record["extra"],
    )

    return (entry.model_dump_json() + "\n").replace("{", "{{").replace("}", "}}")
