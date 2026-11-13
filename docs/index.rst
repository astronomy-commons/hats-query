HATS Query
========================================================================================

Server for querying HATS catalogs through VO and HTTP.

Dev Guide - Getting Started
---------------------------

The Python projects of this repository form a `uv <https://docs.astral.sh/uv/>`_ workspace:
uv creates the virtual environment in ``.venv``, installs the locked dependencies from ``uv.lock``,
and rebuilds the Rust extension when its code changes.
You can install this project for local development using the following commands:

.. code-block:: console

   >> curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   >> curl -LsSf https://astral.sh/uv/install.sh | sh
   >> git submodule update --init
   >> uv sync --all-packages --all-groups
   >> uv run pre-commit install
   >> uv run pytest

Then install ``pandoc`` with your system package manager.

Notes:

1) The package includes a Rust extension, so a Rust toolchain is required to build it.
   The first command installs it with `rustup <https://rustup.rs>`_, the second one installs uv.
2) Tests use third-party data from git submodules in ``test-data/git-submodules/``:
   example HATS catalogs from `hats <https://github.com/astronomy-commons/hats>`_ (its ``tests/data/`` directory)
   and the IVOA `lyonetia <https://github.com/ivoa/lyonetia>`_ ADQL query collection (its ``src/adql/ivoa/`` directory).
   You can skip ``git submodule update --init`` if you cloned with ``git clone --recurse-submodules``.
3) ``pre-commit install`` will initialize pre-commit for this local repository, so
   that a set of tests will be run prior to completing a local commit. For more
   information, see the Python Project Template documentation on
   `pre-commit <https://lincc-ppt.readthedocs.io/en/latest/practices/precommit.html>`_.
4) Installing ``pandoc`` allows you to verify that automatic rendering of Jupyter notebooks
   into documentation for ReadTheDocs works as expected. For more information, see
   the Python Project Template documentation on
   `Sphinx and Python Notebooks <https://lincc-ppt.readthedocs.io/en/latest/practices/sphinx.html#python-notebooks>`_.


.. toctree::
   :hidden:

   Home page <self>
   API Reference <autoapi/index>
   Notebooks <notebooks>
