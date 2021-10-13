# Scripts and Tools
The following scripts and commands may be used during segcore development

## code format 
- under milvus/internal/core directory
    - run `./run_clang_format .` to format cpp code
        - to call clang-format-10,  need to install `apt install clang-format-10` in advance
        - call `build-support/add_${lang}_license.sh` to add license info for cmake and cpp file
- under milvus/ directory
    - use `make cppcheck` to check format, including
        - if clang-format is executed
        - if license info is added
        - if `cpplint.py` standard meets , might need to be fixed by hand
    - `make verifier` also include functions in `make cppcheck`