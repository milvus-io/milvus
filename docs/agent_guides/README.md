# Current subsystem guides

These guides describe current, code-backed Milvus subsystem behavior. They are
part of the implementation interface: changes to the documented architecture,
invariants, or debugging procedure must update the corresponding guide in the
same pull request.

## Subsystems

- [Observability](observability/README.md): logging, metrics, tracing, and
  observability debugging workflows.
- [Streaming system](streaming-system/streaming-system.md): write path, WAL,
  messages, coordination, replication, and CDC foundations.

Each subsystem's top-level guide is an index. Follow its links to the detailed
documents before changing or explaining that subsystem.
