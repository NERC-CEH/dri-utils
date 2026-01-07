"A module to interact with the metadata ingest tool."

from enum import Enum

import requests
from loguru import logger


class MetadataUpdateMethod(str, Enum):
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class MetadataAPIUpdater:
    """Manage requests to the metadata API."""

    def __init__(self, host: str) -> None:
        self.host = host

    def convert_json_to_jsonlines(self, json: str) -> bytes:
        """Convert a json encoded string to a jsonlines bytes object.

        Args:
            json: the json to convert

        Returns:
            a jsonlines bytes object
        """
        return (json + "\n").encode("utf-8")

    def update_metadata(
        self, payload: bytes, component: str, mode: str, parameters: dict = {}
    ) -> requests.models.Response:
        """Update the metadata for a specified component.

        The payload must be in jsonlines format.

        Args:
            payload: the data to post
            component: the component to update
            mode: update or delete
            parameters: any query parameters for the call

        Raises:
            HTTPError if returned status code is bad (4XX/5XX)

        Returns:
            JSON API response.
        """
        url = f"{self.host}/api/{component}/publish"
        headers = {"Content-Type": "application/jsonlines"}

        if mode == "DELETE":
            parameters["delete"] = "true"

        response = requests.post(url=url, headers=headers, data=payload, params=parameters)
        logger.info(f"Trying to access: {response.url}")

        if response.status_code == requests.codes.ok:
            logger.info(f"Payload sent to metadata ingest tool for component {component}.")
            return response.json()

        else:
            logger.error(
                f"Component {component} cannot be updated. "
                f"Received response code {response.status_code}: {response.text}"
            )
            response.raise_for_status()
