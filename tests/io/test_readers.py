import unittest
from unittest.mock import patch, MagicMock
from driutils.io.read import DuckDBFileReader, DuckDBS3Reader
from duckdb import DuckDBPyConnection
from parameterized import parameterized

class TestDuckDBFileReader(unittest.TestCase):

    def test_initialization(self):
        """Test that the class can be initialized"""
        reader = DuckDBFileReader()

        self.assertIsInstance(reader._connection, DuckDBPyConnection)
    
    @patch("driutils.io.read.DuckDBFileReader.close")
    def test_context_manager_is_functional(self, mock):
        """Should be able to use context manager to auto-close file connection"""

        with DuckDBFileReader() as con:
            self.assertIsInstance(con._connection, DuckDBPyConnection)

        mock.assert_called_once()

    @patch("driutils.io.read.DuckDBFileReader.close")
    def test_connection_closed_on_delete(self, mock):
        """Tests that duckdb connection is closed when object is deleted"""

        reader = DuckDBFileReader()
        del reader
        mock.assert_called_once()

    def test_close_method_closes_connection(self):
        """Tests that the .close() method closes the connection"""
        
        reader = DuckDBFileReader()
        reader._connection = MagicMock()

        reader.close()

        reader._connection.close.assert_called()

    def test_read_executes_query(self):
        """Tests that the .read() method executes a query"""
        
        reader = DuckDBFileReader()

        reader._connection = MagicMock()

        query = "read this plz"
        params = ["param1", "param2"]

        reader.read(query, params)

        reader._connection.execute.assert_called_once_with(query, params)

class TestDuckDBS3Reader(unittest.TestCase):
    
    @parameterized.expand(["a", 1, "cutom_endpoint"])
    def test_value_error_if_invalid_auth_option(self, value):
        """Test that a ValueError is raised if a bad auth option is selected"""
        
        with self.assertRaises(ValueError):
            DuckDBS3Reader(value)

    @parameterized.expand(["auto", "AUTO", "aUtO"])
    @patch("driutils.io.read.DuckDBS3Reader._authenticate")
    def test_upper_or_lowercase_option_accepted(self, value, mock):
        """Tests that the auth options can be provided in any case"""
        DuckDBS3Reader(value)

        mock.assert_called_once()

    @patch.object(DuckDBS3Reader, "_auto_auth", side_effect=DuckDBS3Reader._auto_auth, autospec=True)
    def test_init_auto_authentication(self, mock):
        """Tests that the reader can use the 'auto' auth option"""
        
        DuckDBS3Reader("auto")
        mock.assert_called_once()
        
    @patch.object(DuckDBS3Reader, "_sts_auth", side_effect=DuckDBS3Reader._sts_auth, autospec=True)
    def test_init_sts_authentication(self, mock):
        """Tests that the reader can use the 'sts' auth option"""
        DuckDBS3Reader("sts")
        mock.assert_called_once()

    @parameterized.expand([
            ["https://s3-a-real-endpoint", True],
            ["http://localhost:8080", False]
    ])
    @patch.object(DuckDBS3Reader, "_custom_endpoint_auth", wraps=DuckDBS3Reader._custom_endpoint_auth, autospec=True)
    def test_init_custom_endpoint_authentication_https(self, url, ssl, mock):
        """Tests that the reader can authenticate to a custom endpoint
        with https protocol"""
        reader = DuckDBS3Reader("custom_endpoint", url, ssl)
        mock.assert_called_once_with(reader, url, ssl)

    def test_error_if_custom_endpoint_not_provided(self):
        """Test that an error is raised if custom_endpoint authentication used but
        endpoint_url_not_given"""

        with self.assertRaises(ValueError):
            DuckDBS3Reader("custom_endpoint")