"""Script to generate test cosmos data.

Currently used for benchmarking duckdb queries.

Data created per minute for user defined sites and date range.

Can be exported into three different s3 bucket structures:

1) Original format (no partitioning): /YYYY-MM/YYYY-MM-DD.parquet
2) Current format (partitioned by date): /date=YYYY-MM-DD/data.parquet
3) Proposed format (partitioned by date and site): /sire=site/date=YYYY-MM-DD/data.parquet

As discussed, use case for loading from multiple dataset types
(precip, soilmet) unlikely due to different resolutions.
So assuming we will just be querying one dataset at a time.

Notes:

You need to have an aws-vault session running to connect to s3
You (might) need extended permissions to write the test data to s3.
"""

import random
from datetime import date, timedelta

import duckdb
import polars as pl
import s3fs

from driutils.datetime import steralize_date_range


def write_parquet_s3(bucket: str, key: str, data: pl.DataFrame) -> None:
    # Write parquet to s3
    fs = s3fs.S3FileSystem()  # noqa
    destination = f"s3://{bucket}/{key}"  # noqa
    # with fs.open(destination, mode="wb") as f:
    #   data.write_parquet(f)


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

    # Format dates
    start_date, end_date = steralize_date_range(start_date, end_date)

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
    schema.pop("time")
    schema.pop("SITE_ID")

    for column, dtype in schema.items():
        if isinstance(dtype, pl.Float64):
            col_values = pl.Series(column, [random.uniform(1, 50) for i in range(required_rows)])

        if isinstance(dtype, pl.Int64):
            col_values = pl.Series(column, [random.randrange(1, 255, 1) for i in range(required_rows)])
            col_values.round(3)

        test_data.replace_column(test_data.get_column_index(column), col_values)

    return test_data


def export_test_data(bucket: str, data: pl.DataFrame, structure: str = "partitioned_date") -> None:
    """Export the test data.

    Data can be exported to various s3 structures:

    'date': cosmos/dataset_type/YYYY-MM/YYYY-MM-DD.parquet (original format)
    'date_partitioned': cosmos/dataset_type/date=YYYY-MM-DD/data.parquet (current format)
    'date_site_partitioned': cosmos/dataset_type/site=site/date=YYYY-MM-DD/data.parquet (proposed format)

    Args:
        bucket: Name of the s3 bucket
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
            key = f"cosmos/dataset=PRECIP_1MIN_2024_LOOPED/{month}/{day}.parquet"

            print(df)

            write_parquet_s3(bucket, key, df)

        if structure == "partitioned_date":
            day = date_obj.strftime("%Y-%m-%d")
            key = f"cosmos/dataset=PRECIP_1MIN_2024_LOOPED/date={day}/data.parquet"

            print(df)

            write_parquet_s3(bucket, key, df)

        if structure == "partitioned_date_site":
            groups = [(group[0][0], group[1]) for group in df.group_by(pl.col("SITE_ID"))]

            for site, site_df in groups:
                day = date_obj.strftime("%Y-%m-%d")
                key = f"cosmos/dataset=PRECIP_1MIN_2024_LOOPED/site={site}/date={day}/data.parquet"

                print(site_df)

                write_parquet_s3(bucket, key, site_df)


if __name__ == "__main__":
    # Setup basic duckdb connection
    conn = duckdb.connect()

    conn.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET force_download = true;
        SET enable_profiling = query_tree;
    """)

    # Add s3 connection details
    conn.execute("""
        CREATE SECRET aws_secret (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            CHAIN 'sts'
        );
    """)

    # Load single file to get list of unique sites, and the dataset schema
    bucket = "ukceh-fdri-staging-timeseries-level-0"
    key = "cosmos/dataset=PRECIP_1MIN_2024_LOOPED/date=2024-01-01/*.parquet"

    query = f"""SELECT * FROM read_parquet('s3://{bucket}/{key}', hive_partitioning=false)"""
    df = conn.execute(query).pl()

    sites = set(df.get_column("SITE_ID"))
    schema = df.schema

    # Build test data
    test_data = build_test_cosmos_data(date(2024, 3, 28), date(2024, 3, 29), timedelta(minutes=1), sites, schema)

    # Export test data based on required s3 structure
    export_test_data(bucket, test_data, "partitioned_date_site")
