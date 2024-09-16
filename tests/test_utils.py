import unittest

from driutils.utils import remove_protocol_from_url, ensure_list

class TestRemoveProtocolFromUrl(unittest.TestCase):
    def test_https_url(self):
        """Test removing protocol from an HTTPS URL."""
        url = "https://www.example.com"
        expected = "www.example.com"
        result = remove_protocol_from_url(url)
        self.assertEqual(result, expected)

    def test_http_url(self):
        """Test removing protocol from an HTTP URL."""
        url = "http://www.example.com"
        expected = "www.example.com"
        result = remove_protocol_from_url(url)
        self.assertEqual(result, expected)

    def test_url_with_path(self):
        """Test removing protocol from a URL with a path."""
        url = "https://www.example.com/path/to/resource"
        expected = "www.example.com/path/to/resource"
        result = remove_protocol_from_url(url)
        self.assertEqual(result, expected)

    def test_url_with_port(self):
        """Test removing protocol from a URL with a port."""
        url = "https://www.example.com:8080"
        expected = "www.example.com:8080"
        result = remove_protocol_from_url(url)
        self.assertEqual(result, expected)

    def test_url_without_protocol(self):
        """Test a URL that already has no protocol."""
        url = "www.example.com"
        expected = "www.example.com"
        result = remove_protocol_from_url(url)
        self.assertEqual(result, expected)

class TestSteralizeSiteIds(unittest.TestCase):
    def test_none_input(self):
        """Test with None as input, should return an empty list.
        """
        result = ensure_list(None)
        self.assertEqual(result, [])

    def test_no_input(self):
        """Test with no input, should return an empty list.
        """
        result = ensure_list()
        self.assertEqual(result, [])

    def test_empty_string_input(self):
        """Test with an empty string as input, should return an empty list.
        """
        result = ensure_list('')
        self.assertEqual(result, [])

    def test_single_string_input(self):
        """Test with a single site ID as a string.
        """
        result = ensure_list('site1')
        self.assertEqual(result, ['site1'])

    def test_list_of_strings_input(self):
        """Test with a list of site IDs.
        """
        result = ensure_list(['site1', 'site2', 'site3'])
        self.assertEqual(result, ['site1', 'site2', 'site3'])

    def test_empty_list_input(self):
        """Test with an empty list, should return an empty list.
        """
        result = ensure_list([])
        self.assertEqual(result, [])
