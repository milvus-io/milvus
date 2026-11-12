# MEP: Inline BLOB values

- **Created:** 2026-11-11
- **Author(s):** @heychaeyin
- **Status:** Draft
- **Component:** Proxy, Segcore
- **Related Issues:** #53858, #52062
- **Released:** Pending

## Summary

B01 proposes a byte encoding for small binary values. A value may contain any byte, including zero. An empty value and a null value must remain distinguishable after serialization.

## Motivation

Applications that store small binary payloads currently have to encode them as text or manage an external object. The inline representation would let callers supply the original bytes.

## Public Interfaces

The proposed scalar needs a type ID in `milvus-proto` and a matching entry in the C++ core. The example leaves `FieldData.type` unset so reviewers can evaluate the payload before reserving an ID. The proposed field parameter is `max_length`, measured in bytes per value. The type ID, upper bound on `max_length`, and experimental gate need maintainer review.

## Design Details

Use the existing `ScalarField.bytes_data` for a compact batch. Write one validity bit per logical row to both `FieldData.valid_data` and `ScalarField.valid_data`, as the Go client does for nullable scalar fields. Append one byte string for each valid row, including an empty string for a valid zero-length value. Check each value's byte length against the field limit before accepting the batch.

For example, three rows containing `NULL`, empty bytes, and `00 FF` produce the validity mask `[false, true, true]`. The bytes array contains two entries with lengths zero and two. An all-null batch still selects the protobuf `bytes_data` variant. This compact layout is a proposal for BLOB. The pinned schema documents a row-aligned layout for Decimal, which also uses `BytesArray`.

## Compatibility, Deprecation, and Migration Plan

Keep the experimental encoding local until the protocol and core agree on a type ID. Once that ID exists, check both create-collection and add-field validation with the gate disabled. Persistence and reload belong to B02.

## Test Plan

The runnable example serializes every byte value, reconstructs the logical rows from the compact payload, and checks both validity masks. It also covers all-null and empty batches, values at and above the per-value limit, and invalid limits.

From the Milvus root, use the pinned protocol checkout from `scripts/download_milvus_proto.sh`, a protobuf compiler, and Python 3.10 or later with the protobuf runtime.

```sh
mkdir -p cmake_build/b01
protoc -I cmake_build/thirdparty/milvus-proto/proto --python_out=cmake_build/b01 common.proto schema.proto
PYTHONPATH=cmake_build/b01 python3 tests/scripts/check_inline_blob_wire.py -v
```

Once the type ID and gate are available, run the same cases through the experimental Milvus component. Verify that ordinary schema requests reject BLOB while the gate is disabled.

## Rejected Alternatives

Base64 would add an encoding step and increase the payload size. An object URI would require a separate storage lifecycle. The existing protobuf byte array carries the inline payload directly.

## References

- [Stanford B01](https://github.com/milvus-io/milvus/issues/53858)
- [BLOB and composite types](https://github.com/milvus-io/milvus/issues/52062)
- [Pinned wire schema](https://github.com/milvus-io/milvus-proto/blob/3c2afcbf9ce6/proto/schema.proto)
