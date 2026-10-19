from datetime import date, datetime

import pytest

from driutils.metadata_api.models.batch import BatchDataset


class TestBatchDataset:
    """Test the BatchDataset model"""

    base = {
        "dataset": "water-daily-flow-mean",
        "site": "57001",
        "variable": "flow",
        "aggregation": "mean",
        "units": "m3s",
        "resolution": "p1d",
        "status": "ingested",
        "s3_key": "test_key",
        "s3_bucket": "test_bucket",
        "s3_column": "value",
        "filename": "test.csv",
        "access_url": "s3://test_bucket/test_key/",
    }

    @pytest.mark.parametrize(
        "value,expected",
        [
            (datetime(2025, 10, 1, 12, 30, 45), datetime(2025, 10, 1, 12, 30, 45)),
            (date(2025, 10, 1), datetime(2025, 10, 1, 0, 0, 0)),
            (None, None),
        ],
    )
    def test_ensure_datetime(self, value: date | datetime | None, expected: datetime | None) -> None:
        """Datetimes keep their time, dates are converted to midnight and None is left alone"""
        dataset = BatchDataset(**self.base, last_updated=value, start_date=value, end_date=value)

        assert dataset.last_updated == expected
        assert dataset.start_date == expected
        assert dataset.end_date == expected
