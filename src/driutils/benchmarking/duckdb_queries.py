# setup duck db with profiling
# pretty print stats into table??
bucket =
key = # three different structures

# queries

def query_everything(bucket):
    # Testing a very large return and multi-level globbing
    return f"SELECT * FROM read_parquet('s3://{bucket}/**/*.parquet')"

def query_one_site_one_date(bucket, structure):
    # Test a very small return
    if structure in ['date', 'partitioned_date']:
        return f"SELECT * FROM PARQUET('s3://{bucket}/*/*.parquet) WHERE date='2023-09-27' AND SITE_ID='BUNNY'"
    
    if structure == 'partitioned_date_site':
        return f"SELECT * FROM PARQUET('s3://{bucket}/*/*/*.parquet) WHERE SITE_ID='BUNNY' AND date='2023-09-27'"

def query_multi_sites_and_multi_dates_using_conditionals():

def query_multi_sites_and_multi_dates_using_hive_types():


