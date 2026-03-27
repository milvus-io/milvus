# 0009 RocksDB GCC 13 Build Blocker

## Summary

- Remote `scripts/3rdparty_build.sh -o OFF -t Release` fails before `cmake_build/conan/conanbuildinfo.cmake` is generated
- The failing package is `rocksdb/6.29.5@milvus/dev`
- On the HannsDB reference host (`gcc 13.3.0`, `libstdc++11`), `table/block_based/data_block_hash_index.h` is compiled without a visible fixed-width integer header
- Root filesystem pressure is a separate remote risk because compiler temporaries fall back to `/tmp` unless `TMPDIR` is redirected under `/data/work`

## Impact

- Blocks Milvus `internal/core` dependency resolution
- Blocks `scripts/core_build.sh`
- Blocks stage-1 remote verification for the `knowhere-rs` shim path even though the shim itself passes local smoke

## Evidence

- Direct compile with the Conan-generated `rocksdb` flags fails on missing `uint8_t`, `uint16_t`, and `uint32_t` in `data_block_hash_index.h`
- The same compile succeeds when:
  - `TMPDIR=/data/work/milvus-rs-integ/tmp`
  - `-include cstdint` is injected into the C++ compile flags
- Remote root filesystem remains full, so relying on default `/tmp` is not acceptable for stage-1 builds

## Stage 1 Policy

- Keep the workaround in the build layer
- Do not modify `knowhere-rs`
- Do not modify Milvus `query`, `segcore`, or `indexbuilder` core logic
- Prefer a shim-specific remote build wrapper over broad changes to the default Milvus development path
