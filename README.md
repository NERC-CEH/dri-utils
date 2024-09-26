# DRI Utils

[![tests badge](https://github.com/NERC-CEH/dri-utils/actions/workflows/pipeline.yml/badge.svg)](https://github.com/NERC-CEH/dri-utils/actions)

This is a Python package that serves to hold commonly re-implemented utilities such ach IO operations.

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

#### Other Optional Packages

Some utilities need additional packages that aren't relevant to all projects. To install everything, run:

```
pip install -e .[all]
```

or to include datetime utilities:

```
pip install -e .[datetime]
```

#### A Note on Remote Installs

You are likely including this on another project, in this case you should include the git url when installing. For manual installs:
```
pip install "dri-utils[all] @ git+https://github.com/NERC-CEH/dri-utils.git"

```

or if including it in your dependencies
```
dependencies = [
    "another-package",
    ...
    "dri-utils[all] @ git+https://github.com/NERC-CEH/dri-utils.git"
    ]
```

## I/O Modules

### AWS S3

#### Reading
The `S3Reader` class reads files from an S3 object store using the `boto3` client. It simply requires a client to work with to initialize. Provisioning of credentials is left to the user and their s3_client

```python
from driutils.io.aws import S3Reader
import boto3

client = boto3.client("s3")
reader = S3Reader(client)

# Request a file from AWS S3

object_bytes = reader.read(bucket="my-bucket", key="Path/to/file")
```

#### Writing
The `S3Writer` operates in the same way as the reader but supplies a `write` method instead of `read`

```python
from driutils.io.aws import S3Writer
import boto3

client = boto3.client("s3")
body = b"I'm a byte encoded document"
writer = S3Writer(client)

# Submit a file from AWS S3

object_bytes = reader.read(bucket="my-bucket", key="Path/to/file", body=body)
```
### DuckDB Reader
The DuckDB classes use the duckdb python interface to read files from local documents or S3 object storage - this comes with the capacity to use custom s3 endpoints.

To read a local file:
```python

from driutils.read import DuckDBFileReader

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
from driutils.read import import DuckDBS3Reader

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

## Writers

### S3 Object Writer

The `S3Writer` uploads files to S3 using a pre-existing `S3Client` which is left to the user to resource, but is commonly implemented as:
```python

import boto3
from driutils.write import S3Writer

s3_client = boto3.client('s3', endpoint_url="an_optional_url")
content = "Just a lil string"

writer = S3Writer(s3_client)
writer.write(
    bucket_name="target-bucket",
    key="path/to/upload/destination",
    body=content
)
```

## Logging

There is a logging module here that defines the base logging format used for all projects, to use it add:

```python

from driutils import logger

logger.setup_logging()
```

## Datetime Utilities

The module `driutils.datetime` contains common utilities for working with dates and times in Python. The methods within are currently simple validation methods. Some of the methods require additional packages that are not needed for all projects, so ensure that the package is installed as `pip install .[datetime]` or `pip install .[all]`

## General Utilities

The module `driutils.utils` contains utility methods that didn't fit anywhere else and includes things such as ensuring that a list is always returned and removing protocols from URLs.