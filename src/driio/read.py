from abc import ABC, abstractmethod
from typing import Any, List, Optional

import duckdb
from duckdb import DuckDBPyConnection

from driio.utils import remove_protocol_from_url


class ReaderInterface(ABC):
    """Abstract implementation for a IO reader"""

    @abstractmethod
    def read(self, *args, **kwargs) -> Any:
        """Reads data from a source"""


class DuckDBReader(ReaderInterface):
    """Abstract implementation of a DuckDB Reader"""

    _connection: DuckDBPyConnection
    """A connection to DuckDB"""

    def __enter__(self):
        """Creates a connection when used in a context block"""
        self._connection = duckdb.connect()

    def __exit__(self, *args) -> None:
        """Closes the connection when exiting the context"""
        self._connection.close()

    def __del__(self):
        """Closes the connection when deleted"""
        self._connection.close()

    def __init__(self, *args, **kwargs) -> None:
        self._connection = duckdb.connect()


class DuckDBS3Reader(DuckDBReader):
    """Concrete Implementation of a DuckDB reader for reading
    data from an S3 endpoint"""

    def __init__(
        self,
        auth_type: str,
        endpoint_url: Optional[str] = None,
    ) -> None:
        """Initializes

        Args:
            endpoint_url: Custom s3 endpoint
        """

        self._connection = duckdb.connect()

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
            self._custom_endpoint_auth(endpoint_url)

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
                CREATE SECRET (
                    TYPE S3,
                    PROVIDER CREDENTIAL_CHAIN,
                    CHAIN 'sts'
                );
            """)

    def _custom_endpoint_auth(self, endpoint_url: str, use_ssl: Optional[bool] = False) -> None:
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
                USE_SSL 'false'
            );
        """)

    def read(self, query: str, params: Optional[List] = None) -> Any:
        """Requests to read a file

        Args:
            query: The query to send.
            params: The parameters to supplement the query.
        """

        return self._connection.execute(query, params)


if __name__ == "__main__":
    # reader = DuckDBS3Reader("sts")
    # query = "SELECT * FROM read_parquet('s3://ukceh-fdri-staging-timeseries-level-0/cosmos/PRECIP_1MIN_2024_LOOPED/2024-02/2024-02-14.parquet');"
    # print(reader.read(query).pl())

    endpoint = "http://localhost:4566"
    file = "s3://ukceh-fdri-timeseries-level-0/cosmos/PRECIP_1MIN_2024_LOOPED/2024-01/2024-01-01.parquet"
    query = f"SELECT * FROM read_parquet('{file}');"
    print(query)
    reader = DuckDBS3Reader("custom_endpoint", endpoint_url=endpoint)

    print(reader.read(query).pl())
