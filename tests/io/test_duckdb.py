import unittest
from unittest.mock import MagicMock, Mock, patch

import duckdb
from duckdb import DuckDBPyConnection
from parameterized import parameterized

from driutils.io.duckdb import DuckDBFileReader, DuckDBS3Reader


class TestDuckDBFileReader(unittest.TestCase):
    def test_initialization(self) -> None:
        """Test that the class can be initialized"""
        reader = DuckDBFileReader()

        self.assertIsInstance(reader._connection, DuckDBPyConnection)

    @patch("driutils.io.duckdb.DuckDBFileReader.close")
    def test_context_manager_is_functional(self, mock: Mock) -> None:
        """Should be able to use context manager to auto-close file connection"""

        with DuckDBFileReader() as con:
            self.assertIsInstance(con._connection, DuckDBPyConnection)

        mock.assert_called_once()

    @patch("driutils.io.duckdb.DuckDBFileReader.close")
    def test_connection_closed_on_delete(self, mock: Mock) -> None:
        """Tests that duckdb connection is closed when object is deleted"""

        reader = DuckDBFileReader()
        del reader
        mock.assert_called_once()

    def test_close_method_closes_connection(self) -> None:
        """Tests that the .close() method closes the connection"""

        reader = DuckDBFileReader()
        reader._connection = MagicMock()

        reader.close()

        reader._connection.close.assert_called()

    def test_read_executes_query(self) -> None:
        """Tests that the .read() method executes a query"""

        reader = DuckDBFileReader()

        reader._connection = MagicMock()

        query = "read this plz"
        params = ["param1", "param2"]

        reader.read(query, params)

        reader._connection.execute.assert_called_once_with(query, params)

    def test_read_missing_file_raises_error(self) -> None:
        """Test that a missing file raises an IOException"""
        reader = DuckDBFileReader()
        query = "SELECT * FROM read_parquet('notafile.parquet')"

        with self.assertRaises(duckdb.IOException):
            reader.read(query)


class TestDuckDBS3Reader(unittest.TestCase):
    @parameterized.expand(["a", 1, "cutom_endpoint"])
    def test_value_error_if_invalid_auth_option(self, value: int | str) -> None:
        """Test that a ValueError is raised if a bad auth option is selected"""

        with self.assertRaises(ValueError):
            DuckDBS3Reader(value)

    @parameterized.expand(["auto", "AUTO", "aUtO"])
    @patch("driutils.io.duckdb.DuckDBS3Reader._authenticate")
    def test_upper_or_lowercase_option_accepted(self, value: str, mock: Mock) -> None:
        """Tests that the auth options can be provided in any case"""
        DuckDBS3Reader(value)

        mock.assert_called_once()

    @patch.object(DuckDBS3Reader, "_auto_auth", side_effect=DuckDBS3Reader._auto_auth, autospec=True)
    def test_init_auto_authentication(self, mock: Mock) -> None:
        """Tests that the reader can use the 'auto' auth option"""

        DuckDBS3Reader("auto")
        mock.assert_called_once()

    @patch.object(DuckDBS3Reader, "_sts_auth", side_effect=DuckDBS3Reader._sts_auth, autospec=True)
    def test_init_sts_authentication(self, mock: Mock) -> None:
        """Tests that the reader can use the 'sts' auth option"""
        DuckDBS3Reader("sts")
        mock.assert_called_once()

    @parameterized.expand([["https://s3-a-real-endpoint", True], ["http://localhost:8080", False]])
    @patch.object(DuckDBS3Reader, "_custom_endpoint_auth", wraps=DuckDBS3Reader._custom_endpoint_auth, autospec=True)
    def test_init_custom_endpoint_authentication_https(self, url: str, ssl: bool, mock: Mock) -> None:
        """Tests that the reader can authenticate to a custom endpoint
        with https protocol"""
        reader = DuckDBS3Reader("custom_endpoint", url, ssl)
        mock.assert_called_once_with(reader, url, ssl)

    def test_error_if_custom_endpoint_not_provided(self) -> None:
        """Test that an error is raised if custom_endpoint authentication used but
        endpoint_url_not_given"""

        with self.assertRaises(ValueError):
            DuckDBS3Reader("custom_endpoint")

    def test_read_parquet_by_query_with_invalid_key_error(self) -> None:
        """Test that an invalid key raises error"""
        reader = DuckDBS3Reader("auto")
        bucket = "fake-bucket"
        key = "non_existent_key.parquet"
        query = f"SELECT * FROM read_parquet('s3://{bucket}/{key}')"

        with self.assertRaises(duckdb.HTTPException):
            reader.read(query)

    def test_read_parquet_retry(self) -> None:
        """Test that the retry decorator works as expected"""
        reader = DuckDBS3Reader("auto")
        query = "SELECT * FROM read_parquet('README.md')"

        with self.assertRaises(duckdb.InvalidInputException):
            reader.read(query)

        stats = reader.read.statistics
        self.assertEqual(stats["attempt_number"], 3)  # Should have tried 3 times
        self.assertEqual(stats["idle_for"], 4)  # Should have waited 2 seconds between each try
