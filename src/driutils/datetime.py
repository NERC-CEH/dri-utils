from datetime import date, datetime
from typing import List, Optional, Tuple, Union

from dateutil.rrule import rrule


def validate_iso8601_duration(duration: str) -> bool:
    """Validate if the given string is a valid ISO 8601 duration.

    Args:
        duration: The duration string to validate.

    Returns:
        True if the duration is valid, False otherwise.
    """

    try:
        import isodate
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            (
                "Datetime utilities were not installed. Reinstall with",
                " 'pip install driutils[datetime]' to use isodate functionality",
            )
        )

    try:
        isodate.parse_duration(duration)
        return True
    except isodate.ISO8601Error:
        return False


def steralize_date_range(
    start_date: Union[date, datetime], end_date: Optional[Union[date, datetime]] = None
) -> Tuple[Union[date, datetime], datetime]:
    """
    Configures and validates start and end dates.

    Args:
        start_date: The start date.
        end_date: The end date. If None, defaults to start_date.

    Returns:
        A tuple containing the start date and the end date.

    Raises:
        UserWarning: If the start date is after the end date.
    """
    # If end_date is not provided, set it to start_date
    if end_date is None:
        end_date = start_date

    # Ensure the start_date is not after the end_date
    if start_date > end_date:
        raise UserWarning(f"Start date must come before end date: {start_date} > {end_date}")

    # If start_date is of type date, convert it to datetime with time at start of the day
    if isinstance(start_date, date) and not isinstance(start_date, datetime):
        start_date = datetime.combine(start_date, datetime.min.time())

    # If end_date is of type date, convert it to datetime to include the entire day
    if isinstance(end_date, date) and not isinstance(end_date, datetime):
        end_date = datetime.combine(end_date, datetime.max.time())

    return start_date, end_date


def chunk_date_range(start_date: datetime, end_date: datetime, chunk: rrule) -> List[Tuple[datetime, datetime]]:
    """Break date range into specified chunks of time.

    Args:
        start_date: start date
        end_date: end date
        chunk: time period to chunk the data into


    Examples:
        >>> start_date = datetime(2010, 5, 5, 0, 0, 0)
        >>> end_date = datetime(2012, 12, 26, 0, 0, 0)
        >>> print(chunk_date_range(start_date, end_date, YEARLY))
        [(datetime.datetime(2010, 5, 5, 0, 0), datetime.datetime(2011, 5, 5, 0, 0)),
        (datetime.datetime(2011, 5, 5, 0, 0), datetime.datetime(2012, 5, 5, 0, 0)),
        (datetime.datetime(2012, 5, 5, 0, 0), datetime.datetime(2012, 12, 26, 0, 0))]

    Returns:
        A list of datetime tuples chunked by chunk."""
    years = []
    rule = rrule(freq=chunk, dtstart=start_date, until=end_date)

    for x in rule.between(start_date, end_date, inc=True):
        years.append(x)

    years.append(end_date)

    return list(zip(years, years[1:]))
