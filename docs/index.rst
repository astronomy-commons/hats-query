HATS Query
========================================================================================

Server for querying HATS catalogs through VO and HTTP.

Dev Guide - Getting Started
---------------------------

Before installing any dependencies or writing code, it's a great idea to create a
virtual environment. LINCC-Frameworks engineers primarily use `conda` to manage virtual
environments. If you have conda installed locally, you can run the following to
create and activate a new environment.

.. code-block:: console

   >> conda create env -n <env_name> python=3.13
   >> conda activate <env_name>


Once you have created a new environment, you can install this project for local
development using the following commands:

.. code-block:: console

   >> curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   >> git submodule update --init
   >> pip install -e .'[dev]'
   >> pre-commit install
   >> conda install pandoc


Notes:

1) The package includes a Rust extension, so a Rust toolchain is required to build it.
   The command above installs it with `rustup <https://rustup.rs>`_.
2) ADQL tests use the IVOA `lyonetia <https://github.com/ivoa/lyonetia>`_ query collection
   (its ``src/adql/ivoa/`` directory), which is a git submodule at ``tests/data/git-submodules/lyonetia``.
   You can skip ``git submodule update --init`` if you cloned with ``git clone --recurse-submodules``.
3) The single quotes around ``'[dev]'`` may not be required for your operating system.
4) ``pre-commit install`` will initialize pre-commit for this local repository, so
   that a set of tests will be run prior to completing a local commit. For more
   information, see the Python Project Template documentation on
   `pre-commit <https://lincc-ppt.readthedocs.io/en/latest/practices/precommit.html>`_.
5) Installing ``pandoc`` allows you to verify that automatic rendering of Jupyter notebooks
   into documentation for ReadTheDocs works as expected. For more information, see
   the Python Project Template documentation on
   `Sphinx and Python Notebooks <https://lincc-ppt.readthedocs.io/en/latest/practices/sphinx.html#python-notebooks>`_.


.. toctree::
   :hidden:

   Home page <self>
   API Reference <autoapi/index>
   Notebooks <notebooks>
