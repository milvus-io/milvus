# Milvus design documents

Design Docs are durable records of significant Milvus feature and architecture
decisions. They explain the problem, alternatives, selected design,
compatibility impact, and verification plan before or alongside implementation.

## Directory layout

- [`design_docs/`](design_docs/) contains Design Docs. Historical filenames and
  topic subdirectories are retained for compatibility.
- [`assets/`](assets/) contains images shared by Design Docs.

User guides, current subsystem references, and executable experiments do not
belong under `design_docs/`. Maintain user documentation in
[`milvus-io/milvus-docs`](https://github.com/milvus-io/milvus-docs), current
subsystem documentation under `docs/agent_guides/`, and executable experiments
with the implementation, tests, or benchmark suite they validate.
