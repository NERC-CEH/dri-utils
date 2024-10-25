import duckdb


BUCKET = "ukceh-fdri"
PREFIX = 'cosmos-test'
DATASET = "PRECIP_1MIN_2024_LOOPED"
STRUCTURE = 'partitioned_date'

def query_everything(bucket, prefix, dataset):
    # Testing a very large return and multi-level globbing
    return f"EXPLAIN ANALYSE SELECT * FROM read_parquet('s3://{bucket}/{prefix}/{dataset}/**/*.parquet')"


def query_one_site_one_date(bucket, prefix, dataset, structure):
    # Test a very small return
    if structure == 'date':
        return f"""EXPLAIN ANALYSE SELECT * read_parquet('s3://{bucket}/{prefix}/{dataset}/*/*.parquet')
                WHERE date='2023-09-27' AND SITE_ID='BUNNY'"""
    
    if structure == 'partitioned_date':
        return f"""EXPLAIN ANALYSE SELECT * FROM read_parquet('s3://{bucket}/{prefix}/dataset={dataset}/*/*.parquet') 
                WHERE date='2023-09-27' AND SITE_ID='BUNNY'"""
    
    if structure == 'partitioned_date_site':
        return f"""EXPLAIN ANALYSE SELECT * FROM read_parquet('s3://{bucket}/{prefix}/dataset={dataset}/*/*/*.parquet') 
                WHERE date='2023-09-27' AND site='BUNNY'"""


def query_multi_sites_and_multi_dates_using_conditionals(bucket, prefix, dataset, structure):
    # Test larger and more complex query parameters
    # Dates are filtered using conditionals
    if structure == 'date':
        return f"""
            EXPLAIN ANALYSE SELECT * 
            FROM read_parquet('s3://{bucket}/{prefix}/{dataset}/*/*.parquet') 
            WHERE date >= datetime(2019,1,1,0,0,0) AND date <= datetime(2023,9,27,0,0,0) 
            AND SITE_ID IN ('BUNNY', 'ALIC1')
        """

    if structure == 'partitioned_date':
        return f"""
            EXPLAIN ANALYSE SELECT * 
            FROM read_parquet('s3://{bucket}/{prefix}/dataset={dataset}/*/*.parquet') 
            WHERE date >= datetime(2019,1,1,0,0,0) AND date <= datetime(2023,9,27,0,0,0) 
            AND SITE_ID IN ('BUNNY', 'ALIC1')
        """

    if structure == 'partitioned_date_site':
        return f"""
            EXPLAIN ANALYSE SELECT * 
            FROM read_parquet('s3://{bucket}{prefix}/dataset={dataset}/*/*/*.parquet') 
            WHERE date >= datetime(2019,1,1,0,0,0) AND date <= datetime(2023,9,27,0,0,0) 
            AND site IN ('BUNNY', 'ALIC1')
        """


def query_multi_sites_and_multi_dates_using_hive_types(bucket, prefix, dataset, structure):
    # Test larger and more complex query parameters
    # Dates are hive types and filtered using BETWEEN
    if structure == 'partitioned_date':
        return f"""
            EXPLAIN ANALYSE SELECT * 
            FROM read_parquet('s3://{bucket}/{prefix}/dataset={dataset}/*/*.parquet', hive_types = {{date: DATE}}) 
            WHERE date BETWEEN datetime(2019,1,1,0,0,0) AND datetime(2023,9,27,0,0,0) 
            AND SITE_ID IN ('BUNNY', 'ALIC1')
        """

    if structure == 'partitioned_date_site':
        return f"""
            EXPLAIN ANALYSE SELECT * 
            FROM read_parquet('s3://{bucket}{prefix}/dataset={dataset}/*/*/*.parquet', hive_types = {{date: DATE}}) 
            WHERE date BETWEEN datetime(2019,1,1,0,0,0) AND datetime(2023,9,27,0,0,0) 
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
        SET profiling_mode = detailed;
    """)

    # Add s3 connection details
    conn.execute("""
        CREATE SECRET aws_secret (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            CHAIN 'sts'
        );
    """)

    query = query_everything(BUCKET, PREFIX, STRUCTURE)
    df = conn.execute(query).pl()