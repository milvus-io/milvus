## Summary

- After remote protobuf and OpenBLAS blockers were removed, `internal/core` compilation reached the shim include surface.
- The next compile failures come from missing knowhere public headers that Milvus includes broadly in common/config/index code:
  - `knowhere/operands.h`
  - `knowhere/utils.h`
  - `knowhere/comp/knowhere_config.h`

## Impact

- Blocks `internal/core` compilation before the stage-1 HNSW path can be exercised inside Milvus.
- Confirms the current integration gap is now in the compatibility header surface, not in remote prerequisites.

## Evidence

- Remote `core_build.sh` proceeds past protobuf generation and past the OpenBLAS `cblas.h` blocker.
- The next errors include:
  - `fatal error: knowhere/comp/knowhere_config.h: No such file or directory`
  - `fatal error: knowhere/utils.h: No such file or directory`
  - `fatal error: knowhere/operands.h: No such file or directory`

## Resolution Direction

- Extend the shim with the minimum compile-time API needed by Milvus common/config/index code.
- Lock the gap first with a local compile-surface smoke before adding production headers.
- Keep runtime behavior minimal:
  - simple aliases or POD types for operand types
  - no-op/default implementations for knowhere config helpers where stage-1 does not depend on full behavior
