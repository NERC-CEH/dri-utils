import pytest
from datetime import datetime

from driutils.metadata_api.models.batch import Batch, BatchDataset
from driutils.metadata_api.transformers.batches import transform_batches
from driutils.testing_utils.fixture_helpers import discover_json_test_cases, load_json_file


class TestBatchTransformer:
    """Test the batch transformer"""

    @pytest.mark.parametrize("filename", discover_json_test_cases("transformers/batch"))
    def test_transform_batch(self, filename) -> None:
        """Test transforming batches"""

        expected_response = [
            Batch(
                batch_id = "test_1",
                datasets = [
                    BatchDataset(
                        **{
                            "batch_dataset_id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57001",
                            "filename": "test_file.csv",
                            "site": "57001",
                            "variable": "flow",
                            "aggregation": "mean",
                            "units": "m3s",
                            "status": "ingested",
                            "resolution": "p1d",
                            "measure": "flow-p1d-mean-m3s",
                            "dataset": "water-daily-flow-mean",
                            "start_date": datetime(2020, 1, 1, 0, 0, 0),
                            "end_date": datetime(2025, 9, 30, 0, 0, 0),
                            "s3_key": "nrfa/batch=test_1/dataset=water-daily-flow-mean/",
                            "s3_bucket": "ukceh-fdri-staging-timeseries-level-0",
                            "s3_column": "value",
                        }
                    ),
                    BatchDataset(
                        **{
                            "batch_dataset_id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57002",
                            "filename": "test_file.csv",
                            "site": "57002",
                            "variable": "flow",
                            "aggregation": "mean",
                            "units": "m3s",
                            "status": "ingested",
                            "resolution": "p1d",
                            "measure": "flow-p1d-mean-m3s",
                            "dataset": "water-daily-flow-mean",
                            "start_date": datetime(2020, 1, 1, 0, 0, 0),
                            "end_date": datetime(2025, 9, 30, 0, 0, 0),
                            "s3_key": "nrfa/batch=test_1/dataset=water-daily-flow-mean/",
                            "s3_bucket": "ukceh-fdri-staging-timeseries-level-0",
                            "s3_column": "value",
                        }
                    ),
                ],
            )
        ]

        result = transform_batches(load_json_file(filename))
        assert result == expected_response
