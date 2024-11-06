"""Script to benchmark parquet file ingress from s3 using duckdb queries.

Two different bucket structures have been created for testing.

(current format)
'partitioned_date': cosmos-test/structure/dataset=dataset_type/date=YYYY-MM-DD/data.parquet
(proposed format)
'partitioned_date_site': cosmos-test/structure/dataset=dataset_type/site=site/date=YYYY-MM-DD/data.parquet

User can select which structure to query.

Each query profile is saved to ./profile.json. Final metrics are written to csv.
"""

import json
import os

import duckdb
import polars as pl

# User defined inputs
BUCKET = "ukceh-fdri"
PREFIX = "cosmos-test"
DATASET = "PRECIP_1MIN_2024_LOOPED"
OUTPUT_PROFILE = "profile.json"
OUTPUT_CSV = "metrics.csv"
# Select columns to filter. List to select some, empty to select all.
COLUMNS = ["SITE_ID", "time", "P_INTENSITY_RT"]

# Derived constants
BASE_BUCKET_PATH = f"s3://{BUCKET}/{PREFIX}/partitioned_date_site"
COLUMNS_SQL = ", ".join(COLUMNS) if isinstance(COLUMNS, list) else "*"


def extract_metrics(profile: str | os.PathLike) -> pl.DataFrame:
    """Extract the relevant metrics into a polars datframe.

    Args:
        profile: the saved query profile json.

    Returns:
        polars dataframes with required profile metrics.
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

    return pl.DataFrame(metrics)


def query_one_site_one_date(base_path, dataset):  # noqa: ANN001, ANN201
    # Test a very small return with partition filter
    return f"""SELECT {COLUMNS_SQL} FROM read_parquet('{base_path}/dataset={dataset}/
            site=BUNNY/date=2017-09-27/data.parquet')"""


def query_multi_dates_using_conditionals_month(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=BUNNY/*/data.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-01-31'
    """


def query_multi_dates_using_conditionals_year(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=BUNNY/*/data.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-12-31'
    """


def query_multi_sites_and_multi_dates_using_conditionals_month(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=*/date=*/data.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-01-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


def query_multi_sites_and_multi_dates_using_conditionals_year(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=*/date=*/data.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-12-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


def query_multi_dates_using_hive_types_month(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    # Fields of type DATE automatically picked up by duckdb so no need to specify as a hive type
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=BUNNY/date=*/data.parquet')
        WHERE date BETWEEN '2019-01-01' AND '2019-01-31'
    """


def query_multi_dates_using_hive_types_year(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    # Fields of type DATE automatically picked up by duckdb so no need to specify as a hive type
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=BUNNY/date=*/data.parquet')
        WHERE date BETWEEN '2019-01-01' AND '2019-12-31'
    """


def query_multi_sites_and_multi_dates_using_hive_types_month(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    # Fields of type DATE automatically picked up by duckdb so no need to specify as a hive type
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=*/date=*/data.parquet')
        WHERE date BETWEEN '2019-01-01' AND '2019-01-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


def query_multi_sites_and_multi_dates_using_hive_types_year(base_path, dataset):  # noqa: ANN001, ANN201
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    # Fields of type DATE automatically picked up by duckdb so no need to specify as a hive type
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/site=*/date=*/data.parquet')
        WHERE date BETWEEN '2015-01-01' AND '2015-12-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


if __name__ == "__main__":
    # Setup basic duckdb connection
    conn = duckdb.connect(config={"threads": 64})

    conn.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET force_download = false;
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
        query_multi_dates_using_conditionals_month(BASE_BUCKET_PATH, DATASET),
        query_multi_dates_using_conditionals_year(BASE_BUCKET_PATH, DATASET),
        query_multi_sites_and_multi_dates_using_conditionals_month(BASE_BUCKET_PATH, DATASET),
        query_multi_sites_and_multi_dates_using_conditionals_year(BASE_BUCKET_PATH, DATASET),
        query_multi_dates_using_hive_types_month(BASE_BUCKET_PATH, DATASET),
        query_multi_dates_using_hive_types_year(BASE_BUCKET_PATH, DATASET),
        query_multi_sites_and_multi_dates_using_hive_types_month(BASE_BUCKET_PATH, DATASET),
        query_multi_sites_and_multi_dates_using_hive_types_year(BASE_BUCKET_PATH, DATASET),
    ]

    # Create empty dataframe to store the results
    data = pl.DataFrame()

    for query in queries:
        print(f"Running {query}\n")

        # Query profile is saved to ./profile.json
        new_df = conn.sql(query).pl()

        # Write out to csv to test all data returned
        new_df.write_csv("./test.csv")

        # Extract whats need from the profiler
        df = extract_metrics(profile=OUTPUT_PROFILE)
        print(df.glimpse())

        data = pl.concat([data, df], how="diagonal")

    conn.close()
    data.write_csv(OUTPUT_CSV)
