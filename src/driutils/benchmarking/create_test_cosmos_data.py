"""Script to generate test cosmos data.

Requires user inputs to be set.

Currently used for benchmarking duckdb queries.

Data created per interval for user defined sites and date range.

Can be exported into three different s3 bucket structures, all stored under the cosmos-test prefix:

(original format)
'date': cosmos-test/structure/dataset_type/YYYY-MM/YYYY-MM-DD.parquet
(current format)
'partitioned_date': cosmos-test/structure/dataset=dataset_type/date=YYYY-MM-DD/data.parquet
(proposed format)
'partitioned_date_site': cosmos-test/structure/dataset=dataset_type/site=site/date=YYYY-MM-DD/data.parquet

As discussed, use case for loading from multiple dataset types
(precip, soilmet) unlikely due to different resolutions.
So assuming we will just be querying one dataset at a time.

Notes:

You need to have an aws-vault session running to connect to s3
You (might) need extended permissions to write the test data to s3.
"""

import io
import random
from datetime import date, timedelta

import boto3
import polars as pl
from botocore.exceptions import ClientError
from dateutil.rrule import MONTHLY

from driutils.datetime import chunk_date_range, steralize_date_range

# User defined inputs
DATASET = "PRECIP_1MIN_2024_LOOPED"
INPUT_BUCKET = "ukceh-fdri-staging-timeseries-level-0"
INPUT_KEY = f"cosmos/dataset={DATASET}/date=2024-01-01/2024-01-01.parquet"
OUTPUT_BUCKET = "ukceh-fdri"
START_DATE = date(2021, 7, 1)
END_DATE = date(2024, 12, 31)

# How to structure the test data.
# 'date': cosmos-test/structure/dataset_type/YYYY-MM/YYYY-MM-DD.parquet
# 'partitioned_date': cosmos-test/structure/dataset=dataset_type/date=YYYY-MM-DD/data.parquet
# 'partitioned_date_site': cosmos-test/structure/dataset=dataset_type/site=site/date=YYYY-MM-DD/data.parquet
STRUCTURES = ["partitioned_date"]

# Set up s3 client
S3_CLIENT = boto3.client("s3")


def write_parquet_s3(bucket: str, key: str, data: pl.DataFrame) -> None:
    # Write parquet to s3
    buffer = io.BytesIO()
    data.write_parquet(buffer)

    try:
        S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    except (RuntimeError, ClientError) as e:
        print(f"Failed to put {key} in {bucket}")
        raise e


def build_test_cosmos_data(
    start_date: date, end_date: date, interval: timedelta, sites: list, schema: pl.Schema
) -> pl.DataFrame:
    """
    Builds test cosmos data.

    For each site, and for each datetime object at the specified interval between
    the start and end date, random data is generated. The dataframe is initialised with
    the supplied schema, which is taken from the dataset for which you want to create
    test data.

    Args:
        start_date: The start date.
        end_date: The end date.
        interval: Interval to seperate datetime objects between the start and end date
        sites: cosmos sites
        schema: required schema

    Returns:
        A dataframe of random test data.
    """
    # Create empty dataframe with the required schema
    test_data = pl.DataFrame(schema=schema)

    # Build datetime range series
    datetime_range = pl.datetime_range(start_date, end_date, interval, eager=True).alias("time")

    # Attach each datetime to each site
    array = {"time": [], "SITE_ID": []}

    for site in sites:
        array["time"].append(datetime_range)
        array["SITE_ID"].append(site)

    date_site_data = pl.DataFrame(array).explode("time")

    test_data = pl.concat([test_data, date_site_data], how="diagonal")

    # Number of required rows
    required_rows = test_data.select(pl.len()).item()

    # Update rest of the columns with random values
    # Remove cols already generated
    schema.pop("time", None)
    schema.pop("SITE_ID", None)

    for column, dtype in schema.items():
        if isinstance(dtype, pl.Float64):
            col_values = pl.Series(column, [random.uniform(1, 50) for i in range(required_rows)])
            col_values = col_values.round(3)

        if isinstance(dtype, pl.Int64):
            col_values = pl.Series(column, [random.randrange(1, 255, 1) for i in range(required_rows)])

        test_data.replace_column(test_data.get_column_index(column), col_values)

    return test_data


def export_test_data(bucket: str, dataset: str, data: pl.DataFrame, structure: str = "partitioned_date") -> None:
    """Export the test data.

    Data can be exported to various s3 structures:

    (original format)
    'date': cosmos-test/structure/dataset_type/YYYY-MM/YYYY-MM-DD.parquet
    (current format)
    'partitioned_date': cosmos-test/structure/dataset=dataset_type/date=YYYY-MM-DD/data.parquet
    (proposed format)
    'partitioned_date_site': cosmos-test/structure/dataset=dataset_type/site=site/date=YYYY-MM-DD/data.parquet

    Args:
        bucket: Name of the s3 bucket
        dataset: dataset type which has been processed (precip, soilmet etc)
        data: Test data to be exported
        structure: s3 structure. Defaults to date_partitioned (current structure)

    Raises:
        ValueError if invalid structure string is provided.
    """
    # Save out in required structure
    # Validate user input
    valid_structures = ["date", "partitioned_date", "partitioned_date_site"]
    if structure not in valid_structures:
        raise ValueError(f"Incorrect structure arguement entered; should be one of {valid_structures}")

    groups = [(group[0][0], group[1]) for group in data.group_by(pl.col("time").dt.date())]

    for date_obj, df in groups:
        if structure == "date":
            day = date_obj.strftime("%Y-%m-%d")
            month = date_obj.strftime("%Y-%m")
            key = f"cosmos-test/{structure}/{dataset}/{month}/{day}.parquet"

            write_parquet_s3(bucket, key, df)

        if structure == "partitioned_date":
            day = date_obj.strftime("%Y-%m-%d")
            key = f"cosmos-test/{structure}/dataset={dataset}/date={day}/data.parquet"

            write_parquet_s3(bucket, key, df)

        if structure == "partitioned_date_site":
            groups = [(group[0][0], group[1]) for group in df.group_by(pl.col("SITE_ID"))]

            for site, site_df in groups:
                day = date_obj.strftime("%Y-%m-%d")
                key = f"cosmos-test/{structure}/dataset={dataset}/site={site}/date={day}/data.parquet"

                write_parquet_s3(bucket, key, site_df)


if __name__ == "__main__":
    # Get sample object for required dataset to extract the required schema and sites
    try:
        data = S3_CLIENT.get_object(Bucket=INPUT_BUCKET, Key=INPUT_KEY)
        df = pl.read_parquet(data["Body"].read())
    except (RuntimeError, ClientError) as e:
        print(f"Failed to get {INPUT_KEY} from {INPUT_BUCKET}")
        raise e

    sites = set(df.get_column("SITE_ID"))
    schema = df.schema

    # Format dates
    start_date, end_date = steralize_date_range(START_DATE, END_DATE)

    # Build and export test data
    # Chunked into years for processing
    year_chunks = chunk_date_range(start_date, end_date, chunk=MONTHLY)

    for years in year_chunks:
        print(f"Building test data for {DATASET} between {years[0]} and {years[1]}")
        test_data = build_test_cosmos_data(years[0], years[1], timedelta(minutes=1), sites, schema)

        # Export test data based on required s3 structure
        for structure in STRUCTURES:
            print(
                f"Exporting test data to {OUTPUT_BUCKET} between {years[0]} and {years[1]} with structure '{structure}'"
            )
            export_test_data(OUTPUT_BUCKET, DATASET, test_data, structure)
