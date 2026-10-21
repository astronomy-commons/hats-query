Third-party test data included as git submodules, fetch them with `git submodule update --init`.
They are excluded from pytest collection and ruff, do not modify them.

| Directory | Source | License | Used by |
|-----------|--------|---------|---------|
| `lyonetia` | [ivoa/lyonetia](https://github.com/ivoa/lyonetia) | [GPL-3.0](https://github.com/ivoa/lyonetia/blob/master/LICENSE) | ADQL parser tests over `src/adql/ivoa/`, see `rust/adql/parser.rs` |

To bump a submodule, run `git submodule update --remote tests/data/git-submodules/<directory>`
and commit the new pointer.