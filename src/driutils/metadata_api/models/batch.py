from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict


@dataclass
class BatchDataset:
    """Batch dataset model

    Attributes:
        batch_dataset_id: The ID of the dataset
        filename: Which uploaded filename the data comes from
        site: The NRFA site ID
        status: The processing status of the dataset
        resolution: The resolution
        measure: The dataset measure
            composed of variable-resolution-aggregation-units if its an aggregate measure
            composed of variable-resolution-units if instantaneous measure
        dataset: the type of dataset e.g. water-daily-flow-mean
        start_date: the series start date
        end_date: the series end date
        s3_key: the s3 key for the data
        s3_bucket: the s3 bucket where the data is stored
        s3_column: the column contaiing the values
    """

    batch_dataset_id: str
    filename: str
    site: str
    variable: str
    aggregation: str
    units: str
    status: str
    resolution: str
    measure: str
    dataset: str
    start_date: datetime | None
    end_date: datetime | None
    s3_key: str
    s3_bucket: str
    s3_column: str


@dataclass
class Batch(TypedDict):
    """Batch Model

    Attributes:
        batch_id: The batch id
        datasets: The datasets that comprise the batch
    """

    batch_id: str
    datasets: list[BatchDataset]
