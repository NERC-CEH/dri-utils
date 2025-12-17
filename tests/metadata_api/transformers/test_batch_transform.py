from datetime import datetime

from driutils.metadata_api.models.batch import BatchDataset
from driutils.metadata_api.transformers.batches import transform_batches


class TestBatchTransformer:
    """Test the batch transformer"""

    def test_transform_batch(self) -> None:
        """Test transforming batches"""

        raw_data = {
            "items": [
                {
                    "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-test_1",
                    "title": ["NRFA Batch test_1"],
                    "identifier": ["test_1"],
                    "@type": [{"@id": "http://fdri.ceh.ac.uk/vocab/metadata/ObservationDatasetSeries"}],
                    "originatingProgramme": [
                        {"@id": "http://fdri.ceh.ac.uk/id/programme/nrfa", "label": ["NRFA Programme"]}
                    ],
                    "hasPart": [
                        {
                            "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57001",
                            "title": ["water-daily-flow-mean for 57001 from batch test_1"],
                            "@type": [{"@id": "http://fdri.ceh.ac.uk/vocab/metadata/ObservationDataset"}],
                            "temporalResolution": "P1D",
                            "temporal": {
                                "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-test_1-water-daily-flow-mean-57001#temporal",
                                "startDate": "2020-01-01T00:00:00",
                                "endDate": "2025-09-30T00:00:00",
                            },
                            "hasAnnotation": [
                                {
                                    "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57001#dataset",
                                    "hasValue": {
                                        "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57001#dataset.value",
                                        "value": "water-daily-flow-mean",
                                    },
                                },
                                {
                                    "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57001#filename",
                                    "hasValue": {
                                        "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57001#filename.value",
                                        "value": "test_file.csv",
                                    },
                                },
                            ],
                            "processingLevel": {"@id": "http://fdri.ceh.ac.uk/ref/common/processing-level/ingested"},
                            "measure": [{"@id": "http://fdri.ceh.ac.uk/ref/common/measure/flow-p1d-mean-m3s"}],
                            "originatingSite": [
                                {
                                    "@id": "http://fdri.ceh.ac.uk/id/site/nrfa-57001",
                                    "label": ["Taf Fechan at Taf Fechan Reservoir"],
                                }
                            ],
                            "sourceColumnName": "value",
                            "sourceBucket": "ukceh-fdri-staging-timeseries-level-0",
                            "sourceDataset": "nrfa/batch=test_1/dataset=water-daily-flow-mean/",
                        },
                        {
                            "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57002",
                            "title": ["water-daily-flow-mean for 57002 from batch test_1"],
                            "@type": [{"@id": "http://fdri.ceh.ac.uk/vocab/metadata/ObservationDataset"}],
                            "temporalResolution": "P1D",
                            "temporal": {
                                "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57002#temporal",
                                "startDate": "2020-01-01T00:00:00",
                                "endDate": "2025-09-30T00:00:00",
                            },
                            "hasAnnotation": [
                                {
                                    "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57002#dataset",
                                    "hasValue": {
                                        "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57002#dataset.value",
                                        "value": "water-daily-flow-mean",
                                    },
                                },
                                {
                                    "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57002#filename",
                                    "hasValue": {
                                        "@id": "http://fdri.ceh.ac.uk/id/dataset/nrfa-batch-dataset-test_1-water-daily-flow-mean-57002#filename.value",
                                        "value": "test_file.csv",
                                    },
                                },
                            ],
                            "processingLevel": {"@id": "http://fdri.ceh.ac.uk/ref/common/processing-level/ingested"},
                            "measure": [{"@id": "http://fdri.ceh.ac.uk/ref/common/measure/flow-p1d-mean-m3s"}],
                            "originatingSite": [
                                {
                                    "@id": "http://fdri.ceh.ac.uk/id/site/nrfa-57002",
                                    "label": ["Taf Fawr at Llwynon Reservoir"],
                                }
                            ],
                            "sourceColumnName": "value",
                            "sourceBucket": "ukceh-fdri-staging-timeseries-level-0",
                            "sourceDataset": "nrfa/batch=test_1/dataset=water-daily-flow-mean/",
                        },
                    ],
                }
            ]
        }

        expected_response = [
            {
                "batch_id": "test_1",
                "datasets": [
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
            }
        ]

        result = transform_batches(raw_data)
        assert result == expected_response
