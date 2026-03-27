# 0005 Resource and Mmap Stubs

## Stage 1 Defaults

- Resource estimation can be conservative
- `mmap` / `UseDiskLoad` should default to unsupported in stage 1

## Guardrail

- Favor explicit `not supported` or conservative stub behavior over pretending to implement disk or mmap semantics
- If Milvus startup or load flow requires stronger behavior, record the exact expectation before expanding the shim
