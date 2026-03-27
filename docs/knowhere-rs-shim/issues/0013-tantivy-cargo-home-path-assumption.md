## Summary

- Remote `core_build.sh` no longer fails on Conan/protobuf/OpenBLAS prerequisites and reaches the `tantivy` thirdparty target.
- The next build-layer failure is `ls_cargo_target`, which assumes `${HOME}/.cargo/bin` exists.
- In the stage-1 authority environment, `HOME` is intentionally redirected to `/data/work/milvus-rs-integ/home` to avoid the full root filesystem, but that private cargo home is not initialized yet.

## Impact

- Blocks `internal/core` from progressing through the `tantivy` build graph even though `cargo` itself is already available in `PATH`.
- This is a remote environment/bootstrap assumption, not a `knowhere-rs` or Milvus core logic issue.

## Evidence

- Remote build fails with:
  - `ls: cannot access '/data/work/milvus-rs-integ/home/.cargo/bin/': No such file or directory`
  - `make[2]: *** [thirdparty/tantivy/CMakeFiles/ls_cargo_target.dir/build.make:73: thirdparty/tantivy/ls_cargo] Error 2`
- `internal/core/thirdparty/tantivy/CMakeLists.txt` contains:
  - `set(ENV{PATH} ${HOME_VAR}/.cargo/bin:${PATH_VAR})`
  - `COMMAND ls ${HOME_VAR}/.cargo/bin/`
- The remote environment already reports `cargo exists`, which means the failure is the missing home-path directory, not missing Rust toolchain binaries.

## Resolution Direction

- Keep the fix in remote build/bootstrap scripts, not Milvus core code.
- Ensure `${HOME}/.cargo/bin` exists under `/data/work/milvus-rs-integ/home`.
- Prefer also linking or mirroring `cargo` and `rustc` into that private cargo home so the `tantivy` target sees a coherent Rust toolchain under the redirected `HOME`.
