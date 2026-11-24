# Ordinary external Struct projection

- **Feature DRI:** @weiliu1031
- **Primary Approver:** Pending
- **Independent Approver:** Pending
- **Design Review:** Pending

This draft extends Struct reads from Milvus snapshots to Parquet, Vortex,
Lance and Iceberg external sources. It does not change the internal Struct
representation or the milvus-table identity contract.

## Mapping contract

Every external collection Struct parent (`StructArrayFieldSchema`) must
explicitly set `external_field` to the top-level source column name (parent
renaming is supported; `IsExternalCollection` checks `FieldSchema.external_field`
and `StructArrayFieldSchema.external_field` directly with no spec-based
fallback). Target children select same-named source children under that parent;
selecting a subset is allowed. Child `external_field` values are canonical
`<parent_external_field>[<child>]` paths. Child renaming is unsupported.
Top-level aliases of a selected Struct parent's `external_field` column are
rejected, while sibling children share that physical column.

| Arrow source | Milvus StructArray row |
| --- | --- |
| Non-null Struct | One-element array |
| Null Struct | Null row |
| Non-null List<Struct> | Array with the same element order |
| Empty List<Struct> | Empty non-null array |
| Null List<Struct> | Null row |

A null Struct object inside a non-null list is invalid. A null child value is
allowed only by that child's element-nullability contract. Nested scalar Arrays
and vector children retain their existing Milvus type and validity rules.
Hidden Arrow payload below a null parent must not become visible elements.
Missing selected children and incompatible source types are errors; omitted
target children do not require a source read projection of their own.

## Read path

Go and C++ physical-column resolvers map each ordinary Struct child to its
parent column and deduplicate reader projections. Milvus-table continues to
address source leaf columns using numeric FieldIDs.

Ordinary Struct expansion uses an explicit projection descriptor registered
from StructArrayFieldSchema. Each selected source child maps to exactly one
target child. A bracket-shaped field name or repeated external_field alone
does not establish Struct identity. The descriptor is filtered to the current
load selection; an empty selection produces no children. Ordinary scalar and
JSON readers retain their existing single-field path. This feature does not
require generic physical-column alias expansion.

The shared Arrow normalization boundary extracts selected children into the
existing per-child List representation. Scans, index builds, load/cache
materialization, take and refresh sampling must use the same conversion. The
physical source column name distinguishes a required ordinary parent from an
already-flat Milvus snapshot child. Existing Struct offsets are built from the
projected child columns, whose row offsets and validity must agree.

Sampling reads the physical parent but reports each logical child's decoded
size separately. Size aggregation must not overwrite sibling estimates or use
one physical-parent key for multiple different logical sizes. Existing forced
Struct offsets loading and conservative load resource accounting remain in
force, including when warmup is disabled. Translator reservations retain the
existing conservative multiplier for the number of materialized Struct
children, including total-size and sampled fallbacks and overflow checks.
This is a reservation bound, not a claim that every child copies the complete
parent payload.

## Refresh and compatibility

Refresh preserves existing external-source publication and segment replacement
semantics. Replacement data must satisfy the selected child schema. Virtual PKs
are stable within a retained segment and across reload; replacing segments can
change them. Ordinary inner collections and numeric milvus-table mappings must
retain their existing behavior. `StructArrayFieldSchema.external_field` is added
in `milvus-proto` and plumbed through Go SDK and RESTful v2 APIs.

## CDC boundary

Creation carries the normalized schema in the existing CreateCollection WAL
message. This extension adds no secondary-local field identity allocation.
External refresh remains unreplicable (`_ur`) under the existing contract;
this feature does not add automatic refresh or query visibility replication.
CDC runtime validation is not covered by the ordinary-source E2E suite.

## Verification plan

- Admission: implicit mapping, child subset, nullable propagation, mapping
  conflicts, atomic validation failure and existing milvus-table behavior.
- C++: Struct singleton and List<Struct> projection, sliced arrays, null parent
  payload, empty arrays, child validity, malformed input and physical mapping.
- DataNode: logical child size aggregation and failed source validation.
- Real-file Go SDK E2E: all four formats and both source shapes; exact Query and
  Search outputs, element-filter offsets, refresh and reload, virtual PK, all
  warmup disabled. Rich cases cover nested Arrays and nullable child vectors;
  invalid-source cases require specific refresh failures.
- Repeat applicable verification independently on the 3.0 backport.

This document describes the intended contract. Runtime support is established
by the recorded validation results, not by this plan alone.
