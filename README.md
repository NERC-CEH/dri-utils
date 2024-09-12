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