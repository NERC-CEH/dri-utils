import re
from typing import Any

import pytest

from driutils.metadata_api.utils import (
    SITE_ID_EXTRACT_REGEX,
    URI_ID_EXTRACT_REGEX,
    check_single_list_item,
    get_property,
)


class TestUriIdExtractRegex:
    @pytest.mark.parametrize(
        "string, expected",
        [
            ("http://example.com/parameter/simple", "simple"),
            ("http://example.com/parameter/single-hyphen", "single-hyphen"),
            ("http://example.com/parameter/multiple-hyphen-test", "multiple-hyphen-test"),
            ("http://example.com/parameter/under_score", "under_score"),
            ("http://example.com/parameter/multiple_under_score", "multiple_under_score"),
            ("http://example.com/parameter/mix-hyphen_underscore", "mix-hyphen_underscore"),
            ("http://example.com/parameter/g1", "g1"),
        ],
        ids=[
            "simple",
            "with_hyphen",
            "with_multiple_hyphen",
            "with_underscore",
            "with_multiple_underscore",
            "hypen_underscore",
            "with_numbers",
        ],
    )
    def test_parsing_success(self, string: str, expected: str) -> None:
        """Test that validation passes when string format meets the regex."""
        result = re.match(URI_ID_EXTRACT_REGEX, string).group(1)
        assert result == expected

    @pytest.mark.parametrize(
        "string",
        ["invalid-format-not-url", "", "http://example.com/parameter/", "http://example.com/parameter/test&value"],
        ids=[
            "not_a_url",
            "empty_string",
            "no_name_after_slash",
            "special_character",
        ],
    )
    def test_parsing_none(self, string: str) -> None:
        """Test that no result matched when string format that fails the regex."""
        result = re.match(URI_ID_EXTRACT_REGEX, string)
        assert result is None


class TestSiteIdExtractRegex:
    @pytest.mark.parametrize(
        "string, expected",
        [
            ("http://fdri.ceh.ac.uk/id/site/cosmos-chimn", "chimn"),
            ("http://fdri.ceh.ac.uk/id/site/cosmos-alic1", "alic1"),
        ],
        ids=[
            "all_string",
            "with_numbers",
        ],
    )
    def test_parsing_success(self, string: str, expected: str) -> None:
        """Test that validation passes when string format meets the regex."""
        result = re.match(SITE_ID_EXTRACT_REGEX, string).group(1)
        assert result == expected

    @pytest.mark.parametrize(
        "string",
        [
            "invalid-format-not-url",
            "",
            "http://example.com/id/site/",
            "http://example.com/id/site/cosmos-test&site",
            "http://example.com/id/site/cosmos-test-site-id",
            "http://example.com/id/site/12345",
        ],
        ids=[
            "not_a_url",
            "empty_string",
            "no_name_after_slash",
            "special_character",
            "multiple_hyphens",
            "only_numbers",
        ],
    )
    def test_parsing_none(self, string: str) -> None:
        """Test that no result matched when string format that fails the regex."""
        result = re.match(SITE_ID_EXTRACT_REGEX, string)
        assert result is None


class TestGetProperty:
    """Test the get_property function."""

    @pytest.mark.parametrize(
        "value, expected",
        [(123, 123), (123.4, 123.4), ("123", "123"), (["123"], "123"), (None, None), ("", ""), (0, 0)],
        ids=["int", "float", "string", "string list", "None", "empty string", "zero"],
    )
    def test_get_property(self, value: Any, expected: Any) -> None:
        """Test extracting a property"""
        key = "abc"

        result = get_property(key, {key: value})
        assert result == expected

    def test_get_property_none(self) -> None:
        """Test trying to extract a None dictionary"""
        key = "abc"
        expected = None
        result = get_property(key, None)
        assert result == expected

class TestCheckSingleListItem:
    """Test the check_single_list_item method"""

    def test_success(self) -> None:
        """Test method returns the only item from a list"""
        input_data = ["test"]
        expected = "test"

        result = check_single_list_item(input_data)

        assert result == expected

    @pytest.mark.parametrize(
        "input_data,error",
        [(["test1", "test2"], ValueError), ({"test"}, TypeError)],
        ids=[
            "value_error",
            "type_error",
        ],
    )
    def test_error(self, input_data: list | dict, error: Exception) -> None:
        """Test correct errors are raised."""
        with pytest.raises(error):
            check_single_list_item(input_data)
