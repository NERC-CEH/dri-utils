import os
from datetime import datetime

import pytest
import requests
from driutils.metadata_api.models.batch import Batch, BatchDataset
from driutils.metadata_api.updater import MetadataAPIUpdater

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"
METADATA_INGESTER_URL = "http://localhost:8000"


class TestAPIManager:
    """Test the APIManager class."""

    def test_class_instantiated(self) -> None:
        """Test the class is instantiated."""
        updater = MetadataAPIUpdater("fake/url")

        assert isinstance(updater.host, str)

    @pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Ingester API cannot be reached from github action")
    def test_update_metadata_post_and_delete_no_existing_batch(self) -> None:
        """Test the payload is posted successfully."""

        updater = MetadataAPIUpdater(METADATA_INGESTER_URL)
        payload = (
            b'{"batch_id":"test_batch","datasets":[{"dataset":"test_dataset","site":"test_site",'
            b'"variable":"test_variable","aggregation":"mean","units":"m3s","resolution":"p1d",'
            b'"status":"ingested","s3_key":"test_key","s3_bucket":"test_bucket","s3_column":"value",'
            b'"filename":"test.csv","last_updated":"2025-01-03T00:00:00","start_date":"2025-01-01T00:00:00",'
            b'"end_date":"2025-01-02T00:00:00"}]}\n'
        )

        component = "nrfa-batch"
        mode = "UPDATE"

        result = updater.update_metadata(payload, component, mode)

        assert result["detail"] == f"Successfully published data to '{component}' component"

        # Delete entry if successful
        payload = b'{"batch_id":"test_batch","datasets":[]}'
        mode = "DELETE"
        updater.update_metadata(payload, component, mode)

    @pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Ingester API cannot be reached from github action")
    def test_http_error_raised(self) -> None:
        """Test correct error raised if incorrect request made"""

        updater = MetadataAPIUpdater(METADATA_INGESTER_URL)
        payload = "non-bytes-payload"
        component = "nrfa-batch"
        mode = "UPDATE"

        with pytest.raises(requests.exceptions.HTTPError):
            updater.update_metadata(payload, component, mode)

    def test_json_to_jsonlines(self) -> None:
        """Test json correctly converted to jsonlines."""

        updater = MetadataAPIUpdater(METADATA_INGESTER_URL)

        test_batch_dataset = BatchDataset(
            dataset="test_dataset",
            site="test_site",
            variable="test_variable",
            aggregation="mean",
            units="m3s",
            resolution="p1d",
            status="ingested",
            s3_key="test_key",
            s3_bucket="test_bucket",
            s3_column="value",
            filename="test.csv",
            start_date=datetime(2025, 1, 1, 0, 0, 0),
            end_date=datetime(2025, 1, 2, 0, 0, 0),
            last_updated=datetime(2025, 1, 3, 0, 0, 0),
        )
        test_batch = Batch(batch_id="test_batch", datasets=[test_batch_dataset])
        test_batch_json = test_batch.model_dump_json()

        expected_output = (
            b'{"batch_id":"test_batch","datasets":[{"dataset":"test_dataset","site":"test_site",'
            b'"variable":"test_variable","aggregation":"mean","units":"m3s","resolution":"p1d",'
            b'"status":"ingested","s3_key":"test_key","s3_bucket":"test_bucket","s3_column":"value",'
            b'"filename":"test.csv","last_updated":"2025-01-03T00:00:00","start_date":"2025-01-01T00:00:00",'
            b'"end_date":"2025-01-02T00:00:00"}]}\n'
        )

        result = updater.convert_json_to_jsonlines(test_batch_json)

        assert result == expected_output
