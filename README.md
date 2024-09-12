# DRI IO

[![tests badge](https://github.com/NERC-CEH/dri-io/actions/workflows/pipeline.yml/badge.svg)](https://github.com/NERC-CEH/dri-io/actions)
[![docs badge](https://github.com/NERC-CEH/dri-io/actions/workflows/deploy-docs.yml/badge.svg)](https://nerc-ceh.github.io/dri-io/)

This is a Python package that serves to hold commonly implemented Input/Output actions, typically reading and writing file

## Getting Started

### Using the Githook

From the root directory of the repo, run:

```
git config --local core.hooksPath .githooks/
```

This will set this repo up to use the git hooks in the `.githooks/` directory. The hook runs `ruff format --check` and `ruff check` to prevent commits that are not formatted correctly or have errors. The hook intentionally does not alter the files, but informs the user which command to run.

### Installing the package

This package is configured to use optional dependencies based on what you are doing with the code.

As a user, you would install the code with only the dependencies needed to run it:

```
pip install .
```

To work on the docs:

```
pip install -e .[docs]
```

To work on tests:

```
pip install -e .[tests]
```

To run the linter and githook:

```
pip install -e .[lint]
```

The docs, tests, and linter packages can be installed together with:

```
pip install -e .[dev]
```

## Readers

### DuckDB Reader
The DuckDB classes use the duckdb python interface to read files from local documents or S3 object storage - this comes with the capacity to use custom s3 endpoints.

To read a local file:
```python

from driio.read import DuckDBFileReader

reader = DuckDBFileReader()
query = "SELECT * FROM READ_PARQUET('myfile.parquet');"
result = reader.read(query)

# Result will be a <DuckDBPyConnection object>
# Get your desired format such as polars like:
df = result.pl()

# Or pandas
df = result.df()

# Close the connection
reader.close()
```

Alternatively, use a context manager to automatically close the connection:
```python
...

with DuckDBFileReader() as reader:
    df = reader.read(query, params).df()
```

To read from an S3 storage location there is a more configuration available and there is 3 use cases supported:

* Automatic credential loading from current environment variables
* Automatic credential loading from an assumed role
* Authentication to a custom s3 endpoint, i.e. localstack. This currently assumes that credentials aren't needed (they aren't for now)

The reader is instantiated like this:
```python
from driio.read import import DuckDBS3Reader

# Automatic authentication from your environment
auto_auth_reader = DuckDBS3Reader("auto")

# Automatic authentication from your assumed role
sts_auth_reader = DuckDBS3Reader("sts")

# Custom url for localstack
endpoint = "http://localhost:<port>"
custom_url_reader = DuckDBS3Reader(
    "custom_endpoint",
    endpoint_url=endpoint,
    use_ssl=False
    )

# Custom url using https protocol
endpoint = "https://a-real.s3.endpoint"
custom_url_reader = DuckDBS3Reader(
    "custom_endpoint",
    endpoint_url=endpoint,
    use_ssl=True
    )
```

The `reader.read()` in the background forwards a DuckDB SQL query and parameters to fill arguments in the query with.