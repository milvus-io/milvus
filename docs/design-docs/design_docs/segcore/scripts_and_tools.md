# Scripts and Tools

The following scripts and commands may be used during segcore development.

## code format

- under milvus/internal/core directory
  - run `./run_clang_format.sh .` to format cpp code
    - to call clang-format-12, need to install `apt install clang-format-12` in advance
    - call `build-support/add_${lang}_license.sh` to add license info for cmake and cpp files
- under milvus/ directory
  - use `make cppcheck` to check format, including
    - if clang-format is executed
    - if license info is added
    - if `cpplint.py` standard meets , might need to be fixed by hand
  - `make verifier` also includes functions in `make cppcheck`

- under milvus/ directory
  - use `make static-check` to check golang code format
