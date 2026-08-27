# FieldValidator Package

The `fieldvalidator` package validates Milvus `FieldData` payloads (insert /
upsert rows) and fills null/default values. It is a pure leaf: it imports only
`pkg/v3` and protobuf types and has **no dependency on the proxy root package or
any sibling proxy sub-package**.

## Overview

Before DML rows are packed and dispatched, the proxy must guarantee the payload
is well-formed:

- **Aligned**: every field has the same row count (`CheckAligned`).
- **Type-correct**: each field's data matches its schema type — vector dims,
  varchar/array length and capacity, integer overflow, NaN in float vectors,
  JSON/timestamptz constraints
  (`ValidateUtil.Validate` with `With*Check` options).
- **Complete**: nullable / defaultable fields are expanded so downstream
  consumers always see dense payloads (`FillWithNullValue`,
  `FillWithDefaultValue`).

This package is the "VALIDATE" component of the proxy extraction plan (issue
#44761); it was extracted verbatim from the proxy root package
(`validate_util.go`).

## Responsibilities

1. **`ValidateUtil`** — a configurable validator built with functional options
   (`NewValidateUtil(WithNANCheck(), WithMaxLenCheck(), WithOverflowCheck(),
   WithMaxCapCheck())`). Its `Validate` method checks a batch of `FieldData`
   against a `*typeutil.SchemaHelper`.
2. **`CheckAligned`** — cheap row-count alignment guard run before the full
   validation to avoid index-out-of-range panics.
3. **`FillWithNullValue` / `FillWithDefaultValue`** — expand compact `ValidData`
   payloads into dense field data, honoring nullable/defaultable schemas and
   nested (ArrayOfVector / struct) fields.
4. **`ValidateGeometryFieldSearchResult`** — geometry result sanity check used
   by the search/query reduce path.
5. **`ValidateAutoIndexMmapConfig`** — AutoIndex mmap config compatibility check.

## Architecture

```
┌──────────────────────────────────────────────┐
│               fieldvalidator                 │
│                                              │
│   ValidateUtil ── options ──► Validate()     │
│        │                                     │
│        └──► CheckAligned()                   │
│                                              │
│   FillWithNullValue / FillWithDefaultValue   │
│   ValidateGeometryFieldSearchResult          │
│   ValidateAutoIndexMmapConfig                │
└──────────────────────────────────────────────┘
```

### Key types

```go
type ValidateUtil struct{ ... }

type ValidateOption func(*ValidateUtil)

func NewValidateUtil(opts ...ValidateOption) *ValidateUtil

func (v *ValidateUtil) Validate(data []*schemapb.FieldData,
    helper *typeutil.SchemaHelper, numRows uint64) error

func (v *ValidateUtil) CheckAligned(data []*schemapb.FieldData,
    schema *typeutil.SchemaHelper, numRows uint64) error
```

## Usage

- **Insert / upsert tasks** call
  `fieldvalidator.NewValidateUtil(fieldvalidator.WithNANCheck(), ...)` then
  `Validate(...)` on the request's `FieldData`; upsert additionally uses
  `CheckAligned` and `FillWith*` for nullable payloads.
- **Search / query** call `fieldvalidator.ValidateGeometryFieldSearchResult` on
  result field data in the reduce path.
- **Index** calls `fieldvalidator.ValidateAutoIndexMmapConfig`.

The package holds **no state** and reads config only through
`paramtable.Get()` (never the proxy `Params` global), so every entry point is
pure and unit-testable.

## Testing

`validate_util_test.go` is a package-local white-box suite (moved verbatim with
the code) exercising every `check*` method, alignment, and fill path with
`schemapb` data only — no coordinators, no mocks.

## Related Components

- **TASKS** (`internal/proxy/task_*.go`): the only consumers. Edges are one-way
  (tasks → fieldvalidator).
- **Proxy root** (`internal/proxy/`): `util.go` also calls
  `ValidateAutoIndexMmapConfig`.
