import json
import os
import traceback

SERVICE_NAME = os.environ.get("SERVICE_NAME", "unknown-service")


def json_formatter(record) -> str:
    # Handle error messages
    error_type = None
    if record["exception"] is not None:
        exc_type, exc_value, exc_tb = record["exception"]
        error_type = {
            "type": exc_type.__name__ if exc_type else None,
            "msg": str(exc_value) if exc_value else None,
            "stacktrace": "".join(traceback.format_tb(exc_tb)).strip() if exc_tb else None,
        }
    
    # placeholder extras for service specifc logs

    log_entry = {
        "time": record["time"].isoformat(),
        "msg": record["message"],
        "level": record["level"].name,
        "service": SERVICE_NAME,
        "loc": f"{record['name']}:{record['line']}",
        "error_type": error_type,
        "thread": record['thread'].id,
        "extras": dict(record["extra"]) or None,
    }

    return (json.dumps(log_entry, default=str) + "\n").replace("{", "{{").replace("}", "}}")
