# hats-query Development Guide

This document provides a guide for contributing to the development of this project, and can include information such as design principles, core abstractions, API conventions, development workflow, and best-practices.

## Design Goals and North Stars
**CRITICAL: Always keep these design principles in mind when making changes to the project.**

**Correctness is the Top Priority.** The code should include thorough tests. Any approximations should be clearly documented and communicated to the user.

**Code should be modular and use a few consistent APIs.**

**Make Easy Things Easy, Hard Things Possible.** Common use cases should require minimal configuration or set up, but users should also be able to perform complex work.

**Code should follow a standard format** with automatic formatting supported by `ruff`.

**Code should be documented.** Docstrings should provide sufficient API information, such as parameter explanations, for users to understand the code.



## Common Commands

```bash
# Create .venv with all Python packages and dev dependencies, building the Rust extension in debug mode.
# `uv run` re-syncs it and rebuilds the extension if its Rust code has changed.
uv sync --all-packages --all-groups
# Rebuild the extension showing the full Cargo output
(cd hats-query-python && uv run maturin develop --uv)
# Update the locked Python dependencies
uv lock --upgrade

# Run the server placeholder
cargo run -p hats-query

# Rust tests, lints and formatting for all workspace crates
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt

# All Python tests: unit tests of the bindings and integration tests of the server, add `-m ""` to run STILTS tests
uv run pytest
# Only some of them
uv run pytest hats-query-tests

# Lint and format (let the linter fix style — do not hand-tune)
uv run ruff check hats-query-python/ hats-query-tests/
uv run ruff format hats-query-python/ hats-query-tests/
# Pre-commit (runs ruff lint/format, workflow/pyproject schema checks, pytest, etc.)
uv run pre-commit run --all-files

# Build docs
uv run sphinx-build -M html ./docs ./_readthedocs

# Run the server with autoreload
uv run python -m hats_query --config hats-query.toml --server.address 127.0.0.1 --server.port 8000 --server.developer_mode true
```

## Repository Structure

```
Cargo.toml                  Cargo workspace: shared version, dependencies and lints
pyproject.toml              uv workspace of the Python projects, dev dependencies, pytest settings for all Python tests
uv.lock                     Locked Python dependencies
hats-query/                 Rust server binary, a placeholder for now
hats-query-core/            Rust library with the code common to the server and the Python bindings
hats-query-python/          Python package `hats-query`, built with maturin
  pyproject.toml
  Cargo.toml                Rust extension module `hats_query._extension`
  rust/                     Rust sources of the extension module
  python/hats_query/        Python sources
    __main__.py             `python -m hats_query` entry point
    server/                 FastAPI app
      config.py             Configuration, see hats-query.example.toml
      routes/               Endpoints, one APIRouter per module
      templates/            Jinja2 templates
  tests/                    Python unit tests
hats-query-tests/           Integration tests of the server, in Python, assume the server is installed independently
  pyproject.toml            Test dependencies, not a package
  tests/
test-data/                  Test data shared by Rust and Python tests
  ivoa-schemas/             IVOA XML schemas to validate VOSI responses
  git-submodules/           Git submodules, excluded from pytest collection and ruff
    hats/                   HATS library (https://github.com/astronomy-commons/hats), see tests/data/ for example catalogs
    lyonetia/               IVOA ADQL test queries (https://github.com/ivoa/lyonetia), see src/adql/ivoa/
hats-query.example.toml     Example config, used by tests
docs/                       Sphinx documentation sources
```


## Testing Conventions

- **File naming:** `hats-query-python/tests/<SUBDIR>/test_<name>.py`, mirroring the `python/hats_query/` layout.
  Server tests over its HTTP interface go to `hats-query-tests/tests/`.
- **Fixtures:** defined in `tests/conftest.py` of each project. Use existing fixtures, do not duplicate test data.
- Default test run: `uv run pytest`, which runs both Python test suites with the settings from the root `pyproject.toml`
- **Third-party test data:** git submodules in `test-data/git-submodules/`, fetch them with `git submodule update --init`.
  Do not modify them; they are excluded from pytest collection and ruff. To bump one, run
  `git submodule update --remote test-data/git-submodules/<name>` and commit the new pointer.
  - **Example HATS catalogs:** come from `tests/data/` of the `hats` submodule.
  - **ADQL test queries:** come from `src/adql/ivoa/` of the `lyonetia` submodule.
