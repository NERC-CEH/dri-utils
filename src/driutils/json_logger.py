import json
import traceback


def json_formatter(record: dict, service_name: str) -> str:
    """The core JSON logger object.
    
    This object is used throughout our services to create consistent, and
    machine readable logs.

    Any logs that are specific to a service are within that service deployment
    and should be added to the 'extras' attribute with the service name key.

    Args:
        record: the loguru record object
        service: the service the logs are running for
    
    Returns:
        The log attributes
    """
    # Handle error messages
    error_type = None
    if record["exception"] is not None:
        exc_type, exc_value, exc_tb = record["exception"]
        error_type = {
            "type": exc_type.__name__ if exc_type else None,
            "msg": str(exc_value) if exc_value else None,
            "stacktrace": "".join(traceback.format_tb(exc_tb)).strip() if exc_tb else None,
        }
    
    # Create core log structure
    log_entry = {
        "time": record["time"].replace(tzinfo=None).isoformat(),
        "msg": record["message"],
        "level": record["level"].name,
        "service": service_name,
        "loc": f"{record['name']}:{record['line']}",
        "error_type": error_type,
        "thread": record['thread'].id,
        "extras": dict(record["extra"]) or None,
    }

    # Add new line for each record, and remove single curly brackets ready
    # for the loguru format callable
    return (json.dumps(log_entry, default=str) + "\n").replace("{", "{{").replace("}", "}}")
