# MEP: CMEK for growing-source flush

- **Created:** 2026-09-17
- **Author(s):** @XuanYang-cn
- **Status:** Under Review
- **Component:** QueryNode, Storage
- **Related Issues:** #40013

## Summary

This change supplies the flush task's encryption context to the native writer for non-TEXT Parquet column groups. It preserves the existing manifest, field projection, offset range, statistics and commit behavior. It builds on the Parquet acceptance and binary key transport work in [#53450](https://github.com/milvus-io/milvus/pull/53450).

## Motivation

Storage V3 growing-source flush extracts data from a pinned QueryNode growing segment and writes it through the C++ `SegmentWriter`. Unlike canonical DataNode Parquet flush, this path does not currently supply CMEK writer properties. An encrypted collection can therefore produce plaintext Parquet objects when growing-source flush is enabled.

## Public Interfaces

The existing growing-source flush configuration selects this path; no new SDK, RPC, configuration flag or metric is introduced. The internal `CFlushConfig` receives generic writer properties prepared by the Go caller. Encrypted flush configurations with invalid identities fail before writing; encrypted TEXT/LOB and Vortex configurations return an unsupported-operation error.

## Design Details

### Context and ownership

`GrowingSourceSyncTask.buildFlushConfig` already carries the collection ID and schema snapshot selected for the flush task. The delegator passes those values to `LocalSegment.FlushData`; the task snapshot must remain authoritative even if the growing segment's runtime schema changes.

At this boundary, an absent `cipher.ezID` means plaintext. A present but invalid EZ ID must fail before entering the storage writer. QueryNode already registers the collection key through `Collection.Ref → PutOrRefPluginContext → ICipherPlugin.Update`; growing flush reuses that context. It passes the task's EZ ID and collection ID, without obtaining or copying the EZK again.

Before entering native growing flush, the Go caller supplies those IDs to the packed writer's shared encryption-property helper. It calls `GetEncParams` with a null key to obtain a fresh DEK and metadata from the registered context. `GetEncParams` skips `Update` when no key is supplied, while canonical DataNode writers retain their existing explicit registration behavior. `CFlushConfig` carries only generic writer properties; segcore forwards them to `SegmentWriterConfig` without interpreting encryption identities. The DEK remains Base64-encoded across the string property boundary; the Parquet writer performs the decode. Returned allocations are released after copying the properties, including failure paths. Missing plugins or cached keys fail before writer creation.

Each native flush call holds a collection reference until the write returns, following QueryNode's existing `Ref`/`Unref` convention. Source resolution also runs on insert-buffer probes, so it does not refresh the cipher context. Release handoff can detach and retain a segment between flush attempts past channel release, so each retained entry additionally holds one collection reference. Repeated retention updates reuse that reference. Commit releases it after the detached segment drains; rollback and provider close release it alongside the retained pin. A missing collection prevents a write or retention.

### Native writer

Before creating `SegmentWriter`, set `writer.enc.enable`, `writer.enc.key`, `writer.enc.meta` and `writer.enc.algorithm` using the existing encryption interface. A plaintext flush leaves encryption disabled. Plugin failures preserve their existing C status and reach the sync task without a plaintext retry.

This support is restricted to Parquet groups without TEXT columns. Encrypted TEXT/LOB or Vortex configurations must be rejected before creating any data object. Inline TEXT and spilled LOB storage require separate encryption support; the presence of a Parquet main writer is insufficient to guarantee encrypted LOBs.

Manifest transaction retries, object paths and statistics are unchanged. Statistics and other auxiliary artifacts are not added to the encryption acceptance scope by this change.

## Compatibility, Deprecation, and Migration Plan

The writer uses the existing Parquet encryption properties and key-metadata format from #53450. It introduces no new persistent format or migration and does not rewrite existing plaintext objects. Go and native libraries must be rebuilt together because the internal `CFlushConfig` layout changes. Canonical DataNode writers continue to register their supplied keys, and plaintext growing flush continues without encryption properties.

## Test Plan

The growing-source CMEK suite loads the collection before inserting data and enables growing-source flush. Compaction is disabled before the MiniCluster starts. The suite reuses the existing scalar, vector and StructArray campaigns to enumerate every manifest-referenced Parquet object, inspect encryption metadata, release/reload the collection and verify actual values. The scalar campaign also exercises correct, missing and wrong keys against one real object using an independent reader.

Verification must confirm that the flush actually used a growing source, so canonical DataNode fallback cannot hide the missing context. Negative coverage checks invalid encryption metadata and unsupported formats. Collection lifetime tests cover commit, partial commit, rollback, close and an already released collection. Existing plaintext flush coverage remains applicable.

## Rejected Alternatives

Passing EZ and collection IDs into segcore would make the segment C API responsible for resolving cipher context. Preparing properties in the shared packed writer helper keeps that responsibility with writer configuration. Importing the EZ key again for every growing flush would duplicate QueryNode's existing collection-context registration and lifetime management.

## Follow-up

TEXT/LOB encryption, Vortex encryption and encryption of auxiliary artifacts remain separate work. This change does not establish complete Storage V3 CMEK support or backport availability.

## References

- [Customer Managed Encryption Key feature #40013](https://github.com/milvus-io/milvus/issues/40013)
- [Storage V3 CMEK Parquet acceptance #53450](https://github.com/milvus-io/milvus/pull/53450)
