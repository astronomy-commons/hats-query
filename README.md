
# HATS Query

Server for querying HATS catalogs through VO and HTTP.

[![Template](https://img.shields.io/badge/Template-LINCC%20Frameworks%20Python%20Project%20Template-brightgreen)](https://lincc-ppt.readthedocs.io/en/latest/)

[![PyPI](https://img.shields.io/pypi/v/hats-query?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/hats-query/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/astronomy-commons/hats-query/smoke-test.yml)](https://github.com/astronomy-commons/hats-query/actions/workflows/smoke-test.yml)
[![Codecov](https://codecov.io/gh/astronomy-commons/hats-query/branch/main/graph/badge.svg)](https://codecov.io/gh/astronomy-commons/hats-query)
[![Read The Docs](https://img.shields.io/readthedocs/hats-query)](https://hats-query.readthedocs.io/)

This project was automatically generated using the LINCC-Frameworks 
[python-project-template](https://github.com/lincc-frameworks/python-project-template).

A repository badge was added to show that this project uses the python-project-template, however it's up to
you whether or not you'd like to display it!

For more information about the project template see the 
[documentation](https://lincc-ppt.readthedocs.io/en/latest/).

## Running the server

```
>> hats-query --config hats-query.toml
```

See [`hats-query.example.toml`](hats-query.example.toml) for the config file and `hats-query --help` for all options.

Every setting can be given on the command line, as an environment variable, or in the config file, in this order of priority:

```
>> hats-query --server.address 127.0.0.1 --server.port 8000
>> HATS_QUERY_SERVER__ADDRESS=127.0.0.1 HATS_QUERY_SERVER__PORT=8000 hats-query
```

The config file path can also be given as `HATS_QUERY_CONFIG`.
Relative mount `source` paths in the config file are resolved against the file's directory.

`--server.developer_mode true` restarts the server on code changes and returns tracebacks in error responses.

## Dev Guide - Getting Started

Before installing any dependencies or writing code, it's a great idea to create a
virtual environment. LINCC-Frameworks engineers primarily use `conda` to manage virtual
environments. If you have conda installed locally, you can run the following to
create and activate a new environment.

```
>> conda create -n <env_name> python=3.12
>> conda activate <env_name>
```

ADQL tests use the IVOA [lyonetia](https://github.com/ivoa/lyonetia) query collection
(its `src/adql/ivoa/` directory), which is a git submodule at `tests/data/lyonetia`.
Either clone with `git clone --recurse-submodules`, or let `./.setup_dev.sh` fetch it
for you (it runs `git submodule update --init`).

Once you have created a new environment, you can install this project for local
development using the following commands:

```
>> curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
>> ./.setup_dev.sh
>> conda install pandoc
```

Notes:
1. The package includes a Rust extension, so a Rust toolchain is required to build it.
   The first command installs it with [rustup](https://rustup.rs).
2. `./.setup_dev.sh` will initialize pre-commit for this local repository, so
   that a set of tests will be run prior to completing a local commit. For more
   information, see the Python Project Template documentation on 
   [pre-commit](https://lincc-ppt.readthedocs.io/en/latest/practices/precommit.html)
3. Install `pandoc` allows you to verify that automatic rendering of Jupyter notebooks
   into documentation for ReadTheDocs works as expected. For more information, see
   the Python Project Template documentation on
   [Sphinx and Python Notebooks](https://lincc-ppt.readthedocs.io/en/latest/practices/sphinx.html#python-notebooks)
