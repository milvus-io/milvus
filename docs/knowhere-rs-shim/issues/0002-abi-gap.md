# 0002 ABI Gap

## Known Surface Mismatch

- Milvus expects Knowhere C++ headers and object model
- `knowhere-rs` currently exposes a Rust-owned C ABI
- The shim must cover the minimum C++ compatibility surface for:
  - `Version`
  - `DataSet`
  - `IndexNode`
  - `IndexFactory`
  - `BinarySet`
  - config helpers / constants

## Stage 1 Policy

- Record every newly discovered ABI mismatch here before widening the adapter surface
- Prefer a thin translation layer over changes to `knowhere-rs`
