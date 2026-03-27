## Summary

- Remote `core_build.sh` now reaches real C++ compilation after protobuf generation succeeds.
- The next blocker is `milvus-common` failing to compile `src/knowhere/thread_pool.cc` because `cblas.h` is missing.
- The HannsDB authority host has neither `libopenblas-dev` nor a discoverable `cblas.h`.

## Impact

- Blocks `internal/core` compilation before the stage-1 shim can be validated end-to-end inside Milvus.
- Confirms the current failure is still in remote prerequisites, not in `knowhere-rs` or the compatibility shim.

## Evidence

- Remote `scripts/core_build.sh -t Release` fails with:
  - `fatal error: cblas.h: No such file or directory`
- Repository references consistently treat OpenBLAS as a prerequisite:
  - `scripts/install_deps.sh`
  - `scripts/README.md`
  - builder Dockerfiles for Ubuntu
- Remote probe found:
  - no `cblas.h` in standard include locations
  - no installed OpenBLAS package
  - no `gfortran`
- `apt-cache search openblas` on the host fails with:
  - `No space left on device`
  - `IO Error saving source cache`

## Resolution Direction

- Do not patch Milvus source to avoid the BLAS dependency.
- Keep the fix in remote provisioning under `/data/work/milvus-rs-integ`.
- Preferred stage-1 direction:
  - provision a private BLAS install under `/data/work`
  - export include/library paths through `remote_env.sh`
  - avoid system-level `apt install` because `/` is already full
- If package extraction is impractical under the rootfs constraint, test a source-built OpenBLAS path that does not require the system package manager.
