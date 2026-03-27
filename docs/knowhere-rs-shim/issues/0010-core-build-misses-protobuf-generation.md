## Summary

- Remote stage-1 integration initially invoked `scripts/core_build.sh` directly after thirdparty setup.
- Milvus's normal build graph requires `generated-proto` before `build-cpp`.
- Without protobuf generation, `internal/core/src/pb` contains only `CMakeLists.txt`, so CMake fails with:
  - `No SOURCES given to target: milvus_pb`

## Impact

- Blocks remote `internal/core` compilation before any knowhere-rs shim objects are compiled.
- Produces a misleading failure that looks like a CMake/source-layout issue, but the actual root cause is a skipped build prerequisite in the integration wrapper.

## Evidence

- `Makefile` defines:
  - `build-cpp: generated-proto plan-parser-so`
  - `generated-proto: download-milvus-proto build-3rdparty get-proto-deps`
- `scripts/core_build.sh` does not generate protobuf artifacts.
- `internal/core/src/pb/CMakeLists.txt` expects generated `.pb.cc` files via recursive source discovery.

## Resolution Direction

- Keep the fix in the remote build layer.
- Update the integration helper so the `core` path runs:
  - `download_milvus_proto.sh`
  - `make get-proto-deps`
  - `generate_proto.sh`
  - `core_build.sh`
- Do not patch generated files into the repository and do not modify `internal/core/src/pb/CMakeLists.txt`.
