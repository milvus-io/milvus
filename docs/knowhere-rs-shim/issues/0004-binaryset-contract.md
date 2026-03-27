# 0004 BinarySet Contract

## Stage 1 Assumption

- `BinarySet` may start as a single-blob contract for HNSW only

## Questions to Resolve

- What exact blob naming and metadata shape does Milvus expect on the `Serialize` / `Deserialize` path?
- Can the first-stage shim preserve the opaque bytes produced by `knowhere-rs` without reformatting?

## Guardrail

- Do not invent a broader persistence contract until the minimal HNSW round-trip is proven
