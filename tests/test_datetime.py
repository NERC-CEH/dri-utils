import unittest
from unittest.mock import patch
from datetime import date, datetime

from driutils.datetime import steralize_dates, validate_iso8601_duration

class TestValidateISO8601Duration(unittest.TestCase):
    @patch("builtins.__import__")
    def test_error_if_isodate_not_installed(self, mock):
        """Tests that isodate is installed"""

        mock.side_effect = ModuleNotFoundError
        with self.assertRaises(ModuleNotFoundError):
            duration = "P1Y2M3DT4H5M6S"
            validate_iso8601_duration(duration)

    def test_valid_duration_full_format(self):
        """Test a valid ISO 8601 duration in full format."""
        duration = "P1Y2M3DT4H5M6S"
        self.assertTrue(validate_iso8601_duration(duration))

    def test_valid_duration_days_only(self):
        """Test a valid ISO 8601 duration with days only."""
        duration = "P3D"
        self.assertTrue(validate_iso8601_duration(duration))

    def test_valid_duration_hours_only(self):
        """Test a valid ISO 8601 duration with hours only."""
        duration = "PT4H"
        self.assertTrue(validate_iso8601_duration(duration))

    def test_valid_duration_combination(self):
        """Test a valid ISO 8601 duration with a combination of elements."""
        duration = "P2W"
        self.assertTrue(validate_iso8601_duration(duration))

    def test_invalid_duration_missing_p(self):
        """Test an invalid ISO 8601 duration missing the 'P' character."""
        duration = "1Y2M3DT4H5M6S"
        self.assertFalse(validate_iso8601_duration(duration))

    def test_invalid_duration_wrong_format(self):
        """Test an invalid ISO 8601 duration with a wrong format."""
        duration = "P1Y2M3D4H5M6S"
        self.assertFalse(validate_iso8601_duration(duration))

    def test_invalid_duration_non_iso_string(self):
        """Test an invalid ISO 8601 duration with a non-ISO string."""
        duration = "This is not a duration"
        self.assertFalse(validate_iso8601_duration(duration))

    def test_empty_string(self):
        """Test an invalid ISO 8601 duration with an empty string."""
        duration = ""
        self.assertFalse(validate_iso8601_duration(duration))


class TestSteralizeDates(unittest.TestCase):
    def test_start_date_only(self):
        """Test with only start_date provided as date that datetimes of start and end of that date are returned
        """
        start = date(2023, 8, 1)
        expected_start = datetime.combine(start, datetime.min.time())
        expected_end = datetime.combine(start, datetime.max.time())
        result = steralize_dates(start)
        self.assertEqual(result, (expected_start, expected_end))

    def test_start_date_and_end_date_as_dates(self):
        """Test with both start_date and end_date provided as dates that datetimes are returned
        """
        start = date(2023, 8, 1)
        end = date(2023, 8, 10)
        expected_start = datetime.combine(start, datetime.min.time())
        expected_end = datetime.combine(end, datetime.max.time())
        result = steralize_dates(start, end)
        self.assertEqual(result, (expected_start, expected_end))

    def test_start_date_after_end_date_error(self):
        """Test with start_date after end_date, should raise UserWarning.
        """
        start = date(2023, 8, 10)
        end = date(2023, 8, 1)
        with self.assertRaises(UserWarning):
            steralize_dates(start, end)

    def test_start_date_equals_end_date(self):
        """Test with start_date equal to end_date that datetimes of start and end of that date are returned.
        """
        start = date(2023, 8, 1)
        end = date(2023, 8, 1)
        expected_start = datetime.combine(start, datetime.min.time())
        expected_end = datetime.combine(end, datetime.max.time())
        result = steralize_dates(start, end)
        self.assertEqual(result, (expected_start, expected_end))

    def test_datetime_input(self):
        """Test with datetime inputs for both start_date and end_date."""
        start = datetime(2023, 8, 1, 12, 0)
        end = datetime(2023, 8, 10, 18, 0)
        result = steralize_dates(start, end)
        self.assertEqual(result, (start, end))

    def test_mixed_date_and_datetime(self):
        """Test with start_date as date and end_date as datetime."""
        start = date(2023, 8, 1)
        end = datetime(2023, 8, 10, 18, 0)
        expected_start = datetime.combine(start, datetime.min.time())
        result = steralize_dates(expected_start, end)
        self.assertEqual(result, (expected_start, end))