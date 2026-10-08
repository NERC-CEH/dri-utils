from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class BatchDataset(BaseModel):
    """The BatchDataset model.

    Some properties are hardcoded until the data can be passed from the UI
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
    access_url: str
    # TODO: These two values should be updated to values captured by the batch uploader on the UI
    measuring_authority: str = "unknown"
    uploaded_by: str = "dri-ui"
    last_updated: Optional[date | datetime] = Field(default=None)
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
