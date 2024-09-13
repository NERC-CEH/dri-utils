from abc import ABC, abstractmethod
from typing import Any, List, Optional, Self

import duckdb
from duckdb import DuckDBPyConnection

from driutils.utils import remove_protocol_from_url


class ReaderInterface(ABC):
    """Abstract implementation for a IO reader"""

    _connection: Any
    """Reference to the connection object"""

    def __enter__(self) -> Self:
        """Creates a connection when used in a context block"""
        return self

    def __exit__(self, *args) -> None:
        """Closes the connection when exiting the context"""
        self._connection.close()

    def __del__(self):
        """Closes the connection when deleted"""
        self._connection.close()

    @abstractmethod
    def read(self, *args, **kwargs) -> Any:
        """Reads data from a source"""


class DuckDBReader(ReaderInterface):
    """Abstract implementation of a DuckDB Reader"""

    _connection: DuckDBPyConnection
    """A connection to DuckDB"""

    def __init__(self) -> None:
        self._connection = duckdb.connect()

    def read(self, query: str, params: Optional[List] = None) -> DuckDBPyConnection:
        """Requests to read a file

        Args:
            query: The query to send.
            params: The parameters to supplement the query.
        """

        return self._connection.execute(query, params)

    def close(self) -> None:
        """Close the connection"""
        self._connection.close()


class DuckDBS3Reader(DuckDBReader):
    """Concrete Implementation of a DuckDB reader for reading
    data from an S3 endpoint"""

    def __init__(self, auth_type: str, endpoint_url: Optional[str] = None, use_ssl: bool = True) -> None:
        """Initializes

        Args:
            auth_type: The type of authentication to request. May
            be one of ["auto", "sts", "custom_endpoint"]
            endpoint_url: Custom s3 endpoint
            use_ssl: Flag for using ssl (https connections).
        """

        super().__init__()

        auth_type = auth_type.lower()

        VALID_AUTH_METHODS = ["auto", "sts", "custom_endpoint"]

        if auth_type not in VALID_AUTH_METHODS:
            raise ValueError(f"Invalid `auth_type`, must be one of {VALID_AUTH_METHODS}")

        self._connection.execute("""
            INSTALL httpfs;
            LOAD httpfs;
            SET force_download = true;
            SET http_keep_alive = false;
        """)

        if auth_type == "auto":
            self._auto_auth()
        elif auth_type == "sts":
            self._sts_auth()
        elif auth_type == "custom_endpoint":
            if not isinstance(endpoint_url, str):
                endpoint_url = str(endpoint_url)

            self._custom_endpoint_auth(endpoint_url, use_ssl)

    def _auto_auth(self) -> None:
        """Automatically authenticates using environment variables"""

        self._connection.execute("""
            INSTALL aws;
            LOAD aws;
            CREATE SECRET (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN
            );
        """)

    def _sts_auth(self) -> None:
        """Authenicates using assumed roles on AWS"""

        self._connection.execute("""
                INSTALL aws;
                LOAD aws;
                CREATE SECRET (
                    TYPE S3,
                    PROVIDER CREDENTIAL_CHAIN,
                    CHAIN 'sts'
                );
            """)

    def _custom_endpoint_auth(self, endpoint_url: str, use_ssl: bool = True) -> None:
        """Authenticates to a custom endpoint

        Args:
            endpoint_url: Endpoint to the s3 provider.
            use_ssl: Flag for using ssl (https connections).
        """

        self._connection.execute(f"""
            CREATE SECRET (
                TYPE S3,
                ENDPOINT '{remove_protocol_from_url(endpoint_url)}',
                URL_STYLE 'path',
                USE_SSL '{str(use_ssl).lower()}'
            );
        """)


class DuckDBFileReader(DuckDBReader):
    """DuckDB implementation for reading files"""
