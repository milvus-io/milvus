# Schema Change Design Index

This directory contains implementation-oriented documents for online schema
evolution. The documents are derived from, and must remain consistent with, the
canonical design document:

- [Online Schema Evolution](../20260715-online-schema-evolution.md)

## Scope and terminology

The canonical design uses **protocol phases** for the schema lifecycle:

1. Write Only / install invisible field
2. Data Build
3. Atomic Switch / publish visible schema

Its implementation plan also uses **delivery phases**:

0. Milvus 3.0 safety gate
1. Metadata Foundation
2. Read/Write View Adoption
3. RootCoord state machine
4. StreamingNode segment schema boundary
5. Readiness gates
6. Drop cleanup

The first detailed document in this directory covers **delivery Phase 0: the
schema install gate**. Delivery Phase 1 has not been refined yet and is not
documented here.

## Documents

| Document | Scope | Status |
| --- | --- | --- |
| [phase-0-schema-install-gate.md](./phase-0-schema-install-gate.md) | Quiesce and fence query-side topology mutations while the target schema is installed; requires all registered nodes to be released version 3.0.1 or later | Baseline implemented; validation pending |

## Working rules

- The canonical design document is the source of truth for protocol semantics.
- Implementation documents should identify current code, invariants, proposed
  interfaces, migration constraints, and verification requirements.
- A document must not claim that a capability is implemented until the relevant
  component tests and cross-component failure paths have been verified.
- Changes to protobuf-generated files must follow the repository generation
  workflow; generated `.pb.go` files must not be edited by hand.
