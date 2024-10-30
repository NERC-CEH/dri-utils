"""Script to benchmark parquet file ingress from s3 using duckdb queries.

Two different bucket structures have been created for testing.

(current format)
'partitioned_date': cosmos-test/structure/dataset=dataset_type/date=YYYY-MM-DD/data.parquet
(proposed format)
'partitioned_date_site': cosmos-test/structure/dataset=dataset_type/site=site/date=YYYY-MM-DD/data.parquet

User can select which strcuture to query.

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
    # metrics["read_parquet_operator_time_(s)"] = p["children"][0]["children"][0]["operator_timing"]

    return pl.DataFrame(metrics)


def query_one_site_one_date(base_path, dataset):
    # Test a very small return with partition filter
    return f"""SELECT {COLUMNS_SQL} FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet')
            WHERE date='2017-09-27' AND site='BUNNY'"""


def query_one_site(base_path, dataset):
    # Test a very small return without partition filter
    return f"""SELECT {COLUMNS_SQL} FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet')
            WHERE site='BUNNY'"""


def query_multi_dates_using_conditionals_month(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-01-31'
    """


def query_multi_dates_using_conditionals_year(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-12-31'
    """


def query_multi_sites_and_multi_dates_using_conditionals_month(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    # Non partitioned column used
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-01-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


def query_multi_sites_and_multi_dates_using_conditionals_year(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    # Non partitioned column used
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet')
        WHERE date >= '2019-01-01' AND date <= '2019-12-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


def query_multi_dates_using_hive_types_month(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_types = {{date: DATE}})
        WHERE date BETWEEN '2019-01-01' AND '2019-01-31'
    """


def query_multi_dates_using_hive_types_year(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_types = {{date: DATE}})
        WHERE date BETWEEN '2019-01-01' AND '2019-12-31'
    """


def query_multi_sites_and_multi_dates_using_hive_types_month(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    # Non partitioned column used
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_types = {{date: DATE}})
        WHERE date BETWEEN '2019-01-01' AND '2019-01-31'
        AND site IN ('BUNNY', 'ALIC1')
    """


def query_multi_sites_and_multi_dates_using_hive_types_year(base_path, dataset):
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    # Non partitioned column used
    return f"""
        SELECT {COLUMNS_SQL}
        FROM read_parquet('{base_path}/dataset={dataset}/*/*/*.parquet', hive_types = {{date: DATE}})
        WHERE date BETWEEN '2019-01-01' AND '2019-12-31'
        AND site='BUNNY'
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
        # query_one_site(BASE_BUCKET_PATH, DATASET),
        # query_one_site_one_date(BASE_BUCKET_PATH, DATASET),
        # query_multi_dates_using_conditionals_month(BASE_BUCKET_PATH, DATASET),
        # query_multi_dates_using_conditionals_year(BASE_BUCKET_PATH, DATASET),
        # query_multi_sites_and_multi_dates_using_conditionals_month(BASE_BUCKET_PATH, DATASET),
        # query_multi_sites_and_multi_dates_using_conditionals_year(BASE_BUCKET_PATH, DATASET),
        # query_multi_dates_using_hive_types_month(BASE_BUCKET_PATH, DATASET),
        # query_multi_dates_using_hive_types_year(BASE_BUCKET_PATH, DATASET),
        # query_multi_sites_and_multi_dates_using_hive_types_month(BASE_BUCKET_PATH, DATASET),
        query_multi_sites_and_multi_dates_using_hive_types_year(BASE_BUCKET_PATH, DATASET)
    ]

    # Create empty dataframe to store the results
    data = pl.DataFrame()

    for query in queries:
        print(f"Running {query}\n")

        # Query profile is saved to ./profile.json
        conn.execute(query).pl()

        # Extract whats need from the profiler
        df = extract_metrics(profile=OUTPUT_PROFILE)
        print(df.glimpse())

        data = pl.concat([data, df], how="diagonal")

    data.write_csv(OUTPUT_CSV)
