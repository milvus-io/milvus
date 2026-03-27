# 0001 Stage 1 Scope

## Hard Scope

- Remote x86 only
- Milvus standalone only
- float vector only
- HNSW only
- `create_index` / `load` / `search` only

## Guardrails

- Do not modify `knowhere-rs` source code
- Do not modify Milvus `query` / `segcore` / `indexbuilder` core behavior
- Keep changes inside Milvus `thirdparty` / build / adapter layers unless a new issue explicitly expands scope
