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
# All tests
python -m pytest

# Parallel tests
python -m pytest -n auto

# Lint and format (let the linter fix style — do not hand-tune)
ruff check src/ tests/
ruff format src/ tests/
# Pre-commit (runs ruff lint/format, workflow/pyproject schema checks, pytest, etc.)
pre-commit run --all-files

# Build docs
sphinx-build -M html ./docs ./_readthedocs
```

## Repository Structure

```
src/hats_query/             Main package
tests/hats_query/           Test suite
tests/data/lyonetia/        Git submodule with IVOA ADQL test queries (https://github.com/ivoa/lyonetia), see src/adql/ivoa/
docs/                             Sphinx documentation sources
```


## Testing Conventions

- **File naming:** `tests/hats_query/<SUBDIR>/test_<name>.py`, mirroring the `src/hats_query/` layout.
- **Fixtures:** defined in `tests/conftest.py`. Use existing fixtures, do not duplicate test data.
- Default test run: `python -m pytest`
- **ADQL test queries:** come from the `tests/data/lyonetia` git submodule, fetch it with `git submodule update --init`.
  Do not modify it; it is excluded from pytest collection and ruff. To bump it, run
  `git submodule update --remote tests/data/lyonetia` and commit the new pointer.
