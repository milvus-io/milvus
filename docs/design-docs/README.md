# Milvus design documents

Design Docs are durable records of significant Milvus feature and architecture
decisions. They explain the problem, alternatives, selected design,
compatibility impact, and verification plan before or alongside implementation.

## Directory layout

- [`design_docs/`](design_docs/) contains Design Docs. Historical filenames and
  topic subdirectories are retained for compatibility.
- [`assets/`](assets/) contains images shared by Design Docs.

New public user documentation belongs in
[`milvus-io/milvus-docs`](https://github.com/milvus-io/milvus-docs). Existing
topic directories under `design_docs/`, including their supporting material,
remain in place for compatibility. Put current subsystem documentation under
`docs/agent_guides/`, and keep executable experiments with the implementation,
tests, or benchmark suite they validate.
