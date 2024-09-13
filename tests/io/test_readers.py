import unittest
from unittest.mock import patch, MagicMock
from driutils.io.read import DuckDBFileReader
from duckdb import DuckDBPyConnection

class TestDuckDBFileReader(unittest.TestCase):

    def test_initialization(self):
        """Test that the class can be initialized"""
        reader = DuckDBFileReader()

        self.assertIsInstance(reader._connection, DuckDBPyConnection)
    
    def test_context_manager_is_function(self):
        """Should be able to use context manager to auto-close file connection"""

        mock = MagicMock()

        with DuckDBFileReader() as con:
            self.assertIsInstance(con._connection, DuckDBPyConnection)

            con._connection = mock

        self.assertTrue(mock.close.called)

    def test_connection_closed_on_delete(self):
        """Tests that duckdb connection is closed when object is deleted"""
        assert False

    def test_close_method_closes_connection(self):
        """Tests that the .close() method closes the connection"""
        assert False
    def test_read_executes_query(self):
        """Tests that the .read() method executes a query"""
        assert False

class TestDuckDBS3Reader(unittest.TestCase):
    
    def test_value_error_if_invalid_auth_option(self):
        """Test that a ValueError is raised if a bad auth option is selected"""
        assert False

    def test_init_auto_authentication(self):
        """Tests that the reader can use the 'auto' auth option"""
        assert False
        
    def test_init_sts_authentication(self):
        """Tests that the reader can use the 'sts' auth option"""
        assert False
        
    def test_init_custom_endpoint_authentication_https(self):
        """Tests that the reader can authenticate to a custom endpoint
        with https protocol"""
        assert False
        
    def test_init_custom_endpoint_authentication_http(self):
        """Tests that the reader can authenticate to a custom endpoint
        with http protocol"""
        assert False
        