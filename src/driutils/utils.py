"""Utility methods that don't belong elsewhere"""

from typing import List, Optional, Union
from urllib.parse import urlparse


def remove_protocol_from_url(url: str) -> str:
    """Remove the protocol from a URL.

    Args:
        url: URL to remove protocol from

    Returns:
        URL with protocol removed

    Examples:
        >>> remove_protocol_from_url("https://www.example.com")
        "www.example.com"
    """
    endpoint_url = urlparse(url)
    # Remove the protocol scheme by setting it to an empty string
    endpoint_url = "".join(endpoint_url[1:])
    return endpoint_url


def ensure_list(items: Optional[Union[str, List[str]]] = None) -> List[str]:
    """
    Ensures that a list is always received

    Args:
        items: A single string or a list of strings
            If None, defaults to an empty list.

    Returns:
        A list of strings.
    """
    if items is None or items == "":
        # If no items are provided, return an empty list
        items = []
    elif isinstance(items, str):
        # If a single string is provided, convert it to a list
        items = [items]

    return items
