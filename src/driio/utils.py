"""Utility methods that don't belong elsewhere"""

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
