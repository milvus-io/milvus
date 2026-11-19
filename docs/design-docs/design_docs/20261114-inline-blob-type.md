# MEP: Inline BLOB field type and binary encoding

- **Created:** 2026-11-14
- **Author(s):** @aldeberts
- **Status:** Draft
- **Component:** Proxy
- **Related Issues:** #53858 (B01), #52062 (BLOB roadmap)

## Summary

Add a `Blob` scalar data type for bounded binary values stored inline in the
row. This document fixes the wire representation, NULL versus empty bytes, the
size limit, and the schema rules. The type is behind an experimental config
that is off by default.

This is the Stanford CS146S catalog project B01. It only covers the type
definition and proxy validation. Persistence and reload (B02, #53859),
projection and retrieval (B03, #53860), import (B04, #53861) and values hosted
in external object storage are out of scope. Until they land, BLOB is not a
usable feature, and nothing in this change presents it as one.

## Motivation

Today a binary value has to go into a VarChar field, which assumes text and is
checked for valid UTF-8, or into a JSON field, or outside Milvus with a URI
string in the row. None of these lets a client store a small binary value
(a thumbnail, a serialized feature, a hash) next to its vectors and get the
exact bytes back.

BLOB needs a type that holds any bytes, including zero bytes and sequences
that are not UTF-8, and keeps an empty value distinct from NULL.

## Public Interfaces

- **Proto:** a new `DataType` value `Blob` in `schema.proto` in milvus-proto.
  Its ID is `32`, the next free scalar ID after `UUID = 31`. No new message
  is needed: values use the existing
  `ScalarField.bytes_data` (`BytesArray`) and `FieldData.valid_data`.
- **Config:** two experimental proxy parameters, not exported to
  `milvus.yaml`, like `proxy.enableLocalCollectionVisibility`:
  - `proxy.enableBlobType` (default `false`) enables the type. With it off,
    any schema with a BLOB field is rejected.
  - `proxy.maxBlobLength` (default `65535` bytes) caps the `max_length` of a
    BLOB field.
- **Behavior:** CreateCollection, AddCollectionField and AlterCollectionSchema
  accept a BLOB field only when `proxy.enableBlobType` is on. Insert, upsert
  and import into a collection with a BLOB field are rejected until storage
  supports BLOB (see Insert path past validation).
- No SDK, metric or REST change is part of this document.

## Design Details

### Wire representation

A BLOB column is sent as `FieldData{type: Blob, scalars: {bytes_data: ...}}`.
Each entry of `bytes_data.data` is the exact value of one row. The proxy
does not decode, normalize or check the bytes for UTF-8. All 256 byte values,
including `0x00`, are valid anywhere in a value.

### NULL versus empty bytes

The encoding follows the existing rule for nullable scalar fields in
`fieldvalidator`. `valid_data` has one entry per row, and `bytes_data` holds
only the valid rows (compact form):

| Row value | `valid_data[i]` | entry in `bytes_data` |
|---|---|---|
| NULL | `false` | none |
| empty | `true` | `[]byte{}` |
| non-empty | `true` | the bytes |

After `FillWithNullValue` expands the column to one entry per row, a NULL row
holds a nil slice. Protobuf encodes a nil slice and an empty slice the same
way, so `valid_data` is the only thing that separates NULL from empty. Code
downstream must read `valid_data` and must not use the length of a value to
detect NULL.

A NULL value is only allowed in a field declared `nullable`. For a field that
is not nullable, `valid_data` must be empty (the existing
`nullutil.CheckValidData` rule).

### Size limit

Every value has a maximum length in bytes, set per field and capped by a
global config. This is the model VarChar uses with `max_length` and
`proxy.maxVarCharLength`. A BLOB field must set the `max_length` type param in
bytes, in `(0, proxy.maxBlobLength]`, and every value must be at most
`max_length` bytes: a value of exactly the limit is accepted, and one byte
more is rejected. This reuses the VarChar checks (`validateMaxLengthPerRow`,
`parameterutil.GetMaxLength`, `verifyLengthPerRow`).

### Schema rules

A BLOB field:

- requires the experimental parameter to be on;
- cannot be the primary key, partition key or clustering key;
- cannot have a default value;
- cannot have an index;
- cannot be an Array element or a struct sub-field;
- must set `max_length`;
- may be nullable.

`validateBlobField` in `internal/proxy/ddl/util_ddl.go` checks the gate, the
default value and `max_length`. It is called from `ValidateField`, which runs
for CreateCollection, AddCollectionField and AlterCollectionSchema. The other
rules are already enforced by allow-lists in the existing code, so they need
tests, not new checks:

| Rule | Existing check |
|---|---|
| primary key | `validatePrimaryKey`: Int64 or VarChar only |
| partition key | `validatePartitionKey`: Int64 or VarChar only |
| clustering key | `typeutil.IsClusteringKeyType` |
| index | AUTOINDEX and every scalar index checker in `indexparamcheck` use allow-lists |
| Array element | `typeutil.ValidateArrayElementType` |
| struct sub-field | `validateFieldType` (CreateCollection) and `ValidateFieldsInStruct` (add paths): Array or ArrayOfVector only |

Rootcoord's `checkFieldSchema` would also reject a default value, because a
`bytes_data` default is only allowed for JSON. The proxy check gives a clear
message and runs before the request leaves the proxy.

External collections reject BLOB too: `isExternalFieldTypeSupported` does not
list it.

`IsStringType` must not return true for BLOB. Several of these allow-lists
accept string types, so adding BLOB there would open them up.

The gate is needed in the same change as the proto bump. After the new enum
value exists, rootcoord's `validateFieldDataType` accepts it (it allows any
named enum value), and the proxy's `validateFieldType` has no default case
that rejects unknown types.

### Insert validation

`ValidateUtil.Validate` (`internal/proxy/fieldvalidator`) gets a BLOB case.
It rejects:

- a missing `bytes_data` in a field that is not nullable;
- a NULL row in a field that is not nullable;
- a row count different from `num_rows`;
- a value longer than the size limit.

Two shared helpers need a BLOB case for this to work:
`FillWithNullValue` (it returns "undefined data type" for `bytes_data` today)
and `funcutil.GetNumRowOfFieldDataWithSchema` (row counts).

All of these failures are caused by the request content, so they use the
InputError factories (`WrapErrParameterInvalid*`). BLOB fields are not part of
the UTF-8 check (`checkInputUtf8Compatiable` only selects VarChar and TEXT).

### Insert path past validation

Code after the proxy validator does not know BLOB yet. For example,
`typeutil.EstimateEntitySize`, which the proxy calls when it splits an insert
into messages, panics on an unknown data type, and `internal/storage` has no
BLOB payload type.

So this change rejects insert, upsert and import into a
collection with a BLOB field in the proxy, even with the parameter on
(`typeutil.RejectBlobWrite`). The check runs in the PreExecute of InsertTask,
UpsertTask and ImportTask, right after the schema is resolved, next to the
TEXT StorageV3 check. It returns `ErrParameterInvalid`: retrying cannot
succeed, and only a change to the request (another collection) can. B02
removes the check once storage can write BLOB columns. Until then the insert
validation above runs only in unit tests.

Because the check is per collection, adding a BLOB field to an existing
collection with the parameter on also stops inserts into that collection.
Segcore has no BLOB type, so loading a collection with a BLOB field, or
adding one to a collection that is already loaded, is not supported. What a
query node does in those cases has not been traced. These are the reasons
the parameter must stay off outside development and testing.

### Replication (CDC)

The BLOB field is part of the collection schema, and the schema reaches a
secondary inside the CreateCollection or AddCollectionField WAL message. The
secondary applies the message without running proxy validation, so the
experimental parameter only guards requests at the cluster that receives
them. This is the intended behavior: the parameter is a guard at the request
entry point, not cluster state. No message needs to be marked unreplicable.

### C++

`internal/core/src/common/Types.h` has no BLOB entry, and this change does
not add one. Segcore support belongs with persistence and load in B02.

## Compatibility, Deprecation, and Migration Plan

- The parameter is off by default, so existing deployments see no change.
- No stored format changes. No data written before this change is affected.
- Rollback: turning the parameter off blocks new BLOB fields. A collection
  already created with a BLOB field keeps its schema. Segcore and storage have
  no BLOB support until B02, so loading such a collection is not supported,
  and the parameter must stay off outside development and testing.
- A client that does not know the new enum value sees it as an unknown
  `DataType` number. That is the same situation as every earlier type added
  to the enum.

## Test Plan

Unit tests in `pkg/util/typeutil`, `internal/proxy/fieldvalidator`,
`internal/proxy/ddl` and `internal/proxy/dml`:

- **Encoding:** one value with all 256 byte values; values with embedded
  `0x00`; invalid UTF-8; the value is unchanged after validation.
- **NULL:** empty bytes and NULL in the same column stay distinct after
  validation; NULL in a field that is not nullable is rejected.
- **Size:** a value exactly at the limit is accepted; one byte over is
  rejected.
- **Shape:** a row-count mismatch and a missing `bytes_data` are rejected.
- **Schema:** with the parameter off, a BLOB field is rejected in
  `ValidateField` (shared by CreateCollection, AddCollectionField and
  AlterCollectionSchema) and in AddCollectionField's PreExecute. With it on, a
  BLOB field as primary key, partition key, clustering key or Array element is
  rejected, as are a default value, a missing or out-of-range `max_length`,
  AUTOINDEX, every scalar index type and use as a struct sub-field.
- **Writes:** InsertTask, UpsertTask and ImportTask PreExecute reject a
  collection with a BLOB field.
- **Helpers:** `IsBlobType`, and that `IsStringType` and
  `isExternalFieldTypeSupported` stay false for BLOB.

No integration or E2E test is included. A BLOB collection cannot be loaded
or queried until B02 and B03.

## Rejected Alternatives

- **Reuse VarChar or a VarChar type param.** VarChar values go through UTF-8
  checks and text code paths, and the issue asks for binary values that are
  not mistaken for text or URI strings.
- **A new `BlobArray` message in `ScalarField`.** `BytesArray` already holds
  one `bytes` value per row, and `valid_data` already handles NULL. A new
  message would add a proto change and SDK work with no difference on the
  wire.
- **Base64 or hex in a string field.** Larger on the wire, and the client and
  server would both have to agree on an encoding that the type should own.
- **Only a per-field limit, or only a global limit.** A per-field limit alone
  lets one schema set any size, and a global limit alone gives every BLOB
  field the same bound. A per-field value capped by a global config gives
  both, and matches VarChar.
- **Expose the type without a gate.** The rest of the read and write path
  does not support BLOB yet. Without a gate, a user could create a collection
  that later steps (load, flush, query) do not support.

## References

- Issue: #53858 (B01), roadmap: #52062
- Follow-ups: #53859 (B02), #53860 (B03), #53861 (B04)
- Precedent for adding a `DataType`: milvus-io/milvus-proto#636 (`UUID = 31`)
