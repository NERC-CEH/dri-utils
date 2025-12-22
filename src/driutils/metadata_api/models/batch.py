from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class BatchDataset(BaseModel):
    """The BatchDataset model.

    Other properties exist but are not required in the batch metadata template
    Leaving for now as some of the values are yet to be extracted
    date_uploaded, measuring_authority, operator_id, area, uploaded_by
    """

    dataset: str
    site: str
    variable: str
    aggregation: str
    units: str
    resolution: str
    status: str
    s3_key: str
    s3_bucket: str
    s3_column: str
    filename: str
    last_updated: datetime
    start_date: Optional[date | datetime] = Field(default=None)
    end_date: Optional[date | datetime] = Field(default=None)

    @field_validator("start_date", "end_date", "last_updated", mode="after")
    def ensure_datetime(cls, value: date | datetime) -> datetime:
        if isinstance(value, date):
            value = datetime.combine(value, datetime.min.time())

        return value


class Batch(BaseModel):
    """The Batch model."""

    batch_id: str
    datasets: list[BatchDataset]
