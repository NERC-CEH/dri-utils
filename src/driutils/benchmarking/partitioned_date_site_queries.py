"""Script to benchmark parquet file ingress from s3 using duckdb queries.

Two different bucket structures have been created for testing.

(current format)
'partitioned_date': cosmos-test/structure/dataset=dataset_type/date=YYYY-MM-DD/data.parquet
(proposed format)
'partitioned_date_site': cosmos-test/structure/dataset=dataset_type/site=site/date=YYYY-MM-DD/data.parquet

User can select which strcuture to query.

Each query profile is saved to a json.
"""

import json
import os

import duckdb
import polars as pl

# User defined inputs
BUCKET = "ukceh-fdri"
PREFIX = "cosmos-test"
DATASET = "PRECIP_1MIN_2024_LOOPED"
PROFILE_OUTPUT = "profile.json"

BASE_BUCKET_PATH = f"s3://{BUCKET}/{PREFIX}/partitioned_date_site"


def extract_metrics(profile: str | os.PathLike) -> pl.DataFrame:
    """Extract the relevant metrics into a polars datframe.

    Args:
        profile: the saved query profile json.

    Returns:
        polars dataframes with required proifle metrics.
    """

    with open(profile) as f:
        p = json.load(f)

    metrics = {}
    metrics["query"] = p["query_name"]
    metrics["total_elapsed_query_time_(s)"] = p["latency"]
    metrics["rows_returned"] = p["rows_returned"]
    metrics["result_set_size_(Mb)"] = p["result_set_size"] / 1048576
    metrics["rows_scanned"] = p["cumulative_rows_scanned"]
    metrics["cpu_time_(s)"] = p["cpu_time"]
    metrics["read_parquet_operator_time_(s)"] = p["children"][0]["children"][0]["operator_timing"]

    return pl.DataFrame(metrics)


def query_one_site_one_date(base_path, dataset):
    # Test a very small return with partition filter
    return f"""SELECT * FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_partitioning=true)
            WHERE site='BUNNY' AND date='2019-01-27'"""


def query_multi_dates_using_conditionals(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    return f"""
        SELECT *
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_partitioning=true)
        WHERE date >= '2015-01-01' AND date <= '2015-01-31'
    """


def query_multi_sites_and_multi_dates_using_conditionals(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    # Non partitioned column used
    return f"""
        SELECT *
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_partitioning=true)
        WHERE date >= '2015-01-01' AND date <= '2015-12-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


def query_multi_dates_using_hive_types(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    return f"""
        SELECT *
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_partitioning=true, hive_types = {{date: DATE}})
        WHERE date BETWEEN '2015-01-01' AND '2015-01-31'
    """


def query_multi_sites_and_multi_dates_using_hive_types(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    # Non partitioned column used
    return f"""
        EXPLAIN ANALYSE SELECT *
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_partitioning=true, hive_types = {{date: DATE}})
        WHERE date BETWEEN '2015-01-01' AND '2015-01-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


if __name__ == "__main__":
    # Setup basic duckdb connection
    conn = duckdb.connect()

    conn.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET force_download = true;
        SET enable_profiling = json;
        SET profiling_output = 'profile.json';
    """)

    # Add s3 connection details
    conn.execute("""
        CREATE SECRET aws_secret (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            CHAIN 'sts'
        );
    """)

    queries = [
        query_one_site_one_date(BASE_BUCKET_PATH, DATASET),
        # query_multi_dates_using_conditionals(BASE_BUCKET_PATH, DATASET),
        # query_multi_sites_and_multi_dates_using_conditionals(BASE_BUCKET_PATH, DATASET),
        query_multi_dates_using_hive_types(BASE_BUCKET_PATH, DATASET),
        query_multi_sites_and_multi_dates_using_hive_types(BASE_BUCKET_PATH, DATASET),
    ]

    # Create empty dataframe to sotre the results
    data = pl.DataFrame()

    for query in queries:
        # Query profile is saved to ./profile.json
        print(f"Running \n{query}\n")
        conn.execute(query).pl()

        # Extract whats need from the profiler
        print(f"Extracting results from {PROFILE_OUTPUT}")
        df = extract_metrics(profile=PROFILE_OUTPUT)

        data = pl.concat([data, df], how="diagonal")

    print(data.glimpse())
    print(data)
