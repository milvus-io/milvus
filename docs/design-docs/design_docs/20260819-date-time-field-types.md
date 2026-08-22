# MEP: Native DATE and TIME field types

- **Created:** 2026-08-19
- **Author(s):** @daviddallakyan2005
- **Status:** Under Review
- **Component:** Proxy | Storage | Index | SDK
- **Related Issues:** [#27577](https://github.com/milvus-io/milvus/issues/27577)
- **Released:** TBD

## Summary

Add PostgreSQL-aligned `DATE` and `TIME` scalar field types. `DATE` stores calendar days since the Unix epoch as `int32` (Arrow Date32 shape). `TIME` without time zone stores microseconds since midnight as `int64` (Arrow Time64 microsecond shape). Users insert and filter with ISO-8601 literals. Time with time zone is out of scope.

The proto slots already exist. [milvus-proto#621](https://github.com/milvus-io/milvus-proto/pull/621) added `DataType.Date = 28`, `DataType.Time = 29`, `DateArray` (`repeated int32`), and `TimeArray` (`repeated int64`). This work wires those types through schema validation, insert, expression planning, segcore, scalar index, and the in-tree Go client.

## Motivation

[#27577](https://github.com/milvus-io/milvus/issues/27577) asked for native date and time types so users do not pack calendar values into `INT64` themselves. Maintainer `xiaofan-luan` asked to start from date and time, aligned with PostgreSQL, and asked for a design document. `TIMESTAMPTZ` already ships end to end. `DATE` and `TIME` reuse that path with integer encodings and no timezone conversion.

## Public Interfaces

### Proto (already merged)

- `DataType.Date = 28`, `DataType.Time = 29`
- Insert and query payload: `ScalarField.date_data` (`DateArray` of `int32` days) and `ScalarField.time_data` (`TimeArray` of `int64` microseconds)
- Insert input at the proxy: ISO-8601 strings, rewritten in place to the packed integer oneofs (same pattern as `TIMESTAMPTZ`)
- Query and search output: ISO-8601 strings (`YYYY-MM-DD` and `HH:MM:SS[.ffffff]`)

### Literals

- `DATE`: `YYYY-MM-DD` only. Example: `'2024-06-22'`. A time or offset suffix is rejected.
- `TIME`: `HH:MM:SS` with optional fractional microseconds. Example: `'13:45:30.123456'`. `24:00:00` is accepted (PostgreSQL). A timezone suffix is rejected.
- Filter expressions use ordinary string literals. The `ISO` keyword and `INTERVAL` arithmetic stay `TIMESTAMPTZ`-only.

### Schema rules

- Nullable is allowed.
- Not a primary key, partition key, clustering key, or entity TTL field.
- `default_value` is not supported in this milestone (matches the proto comment on `FieldSchema.default_value`).
- Autoindex uses `STL_SORT`.

### Out of scope

- Time with time zone
- Timestamp without time zone
- Interval storage or `INTERVAL` arithmetic on `DATE`/`TIME`
- `EXTRACT`
- pymilvus (separate repository)

## Design Details

### Encodings

| Type | User literal | Storage | Proto array | Arrow |
| --- | --- | --- | --- | --- |
| `DATE` | `YYYY-MM-DD` | `int32` days since 1970-01-01 UTC | `DateArray` | `int32` (Date32 shape, not Arrow `date32()`) |
| `TIME` | `HH:MM:SS[.ffffff]` | `int64` microseconds since midnight | `TimeArray` | `int64` (Time64 us shape, not Arrow `time64()`) |

`DATE` day 0 is 1970-01-01. Negative days are dates before the epoch. Values that do not fit in `int32` are rejected.

`TIME` ranges from `0` through `86_400_000_000` inclusive (`24:00:00.000000`). Values outside that range are rejected.

Storage stays integer, not string. Segcore treats `DATE` like `INT32` and `TIME` like `INT64`/`TIMESTAMPTZ` for columns, term/unary compare, range, and null checks.

### Null semantics

Nullability uses the existing `valid_data` bitmap. A null `DATE` or `TIME` is not equal to any literal, including the packed zero value. Comparisons against null follow the same three-valued logic as other scalars.

### Index

`STL_SORT` is the supported scalar index, matching `TIMESTAMPTZ`. Bitmap and inverted indexes are not enabled in this milestone.

### Expression execution

Literal compares, `IN`, range, and `IS NULL` are supported. Field-to-field `DATE` vs `DATE` and `TIME` vs `TIME` are supported in `CompareExpr` from the start. `DATE` vs `TIME` vs `TIMESTAMPTZ` mixing is rejected at plan time.

`TIMESTAMPTZ` field-to-field compare currently misses both `CompareExpr` switches. This work does not patch `TIMESTAMPTZ` there.

### Insert path

The client may send ISO strings on `StringData`. Proxy validation parses them and replaces the oneof with `DateArray` or `TimeArray` before the message reaches datanode. Packed integer payloads are also accepted.

### Result path

Query and search rewrite packed integers back to ISO strings before returning to the client, without applying a collection timezone.

## Compatibility, Deprecation, and Migration Plan

New enum values. Existing collections are unchanged. Rolling upgrade is safe: old nodes that do not know `Date`/`Time` reject create and insert for those fields. No rewrite of existing binlogs. Rollback of a collection that already stored `DATE`/`TIME` is not supported (same class of change as `TIMESTAMPTZ`).

## Test Plan

- Unit tests for ISO parse and format, including rejection of timezone suffixes and out-of-range `TIME`
- `pkg/util/typeutil` size, append, merge, and primitive-type helpers
- `internal/parser/planparserv2` literal compare, `IN`, null, and mixed-type rejection
- Proxy insert validation: string to packed int, nullable rows
- C++ segcore storage, `TermExpr`, `UnaryExpr`, both `CompareExpr` switches, and `STL_SORT` dispatch
- Go client create, insert, and filter against a running standalone

## Rejected Alternatives

- Store as `VARCHAR`. Breaks range indexes and numeric compare.
- Arrow `date32()` / `time64()` physical types. `TIMESTAMPTZ` already stores as Arrow `int64`. Matching that keeps serde and mmap code on one path.
- Time with time zone. PostgreSQL warns that DST cannot be resolved without a date. Explicitly skipped.
- Reuse the `TIMESTAMPTZ` `ISO` keyword and `INTERVAL` grammar. Those are timezone and calendar arithmetic. `DATE`/`TIME` literals are ordinary strings.

## References

- [#27577](https://github.com/milvus-io/milvus/issues/27577)
- [milvus-proto#621](https://github.com/milvus-io/milvus-proto/pull/621)
- [PostgreSQL date/time types](https://www.postgresql.org/docs/current/datatype-datetime.html)
