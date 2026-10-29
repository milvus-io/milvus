# Backfill manifest commits on Milvus 3.0

[Upstream design and implementation](https://github.com/milvus-io/milvus/pull/53491).

## Result and publication

Spark writes a result JSON object. Each DataCoord reads it from the configured
object store and commits it against that cluster's catalog. The primary and
standby require separate submissions and validate their own segment identities,
collection schema, partition and flushed state. Roll out a compatible receiver
on each cluster before enabling delta output on the producer.

- V2 carries column groups. DataCoord uses its existing column-group upsert
  and preserves its binlog key and row-count conventions.
- Legacy V3 carries a pre-built manifest version. A positive `sourceVersion`
  pins the revision used to build it. Old results without that field can
  still advance the pointer on the same base. Both forms require an advance.
- Delta V3 carries `ops`. Each operation describes an add or replace by
  field-ID strings, file paths, format and row ranges. Files must belong to
  the target segment's existing `_data/` directory. The handler checks fields
  against the collection schema and checks the schema version before dispatch.

A matching group with the same columns and file paths/ranges is an idempotent
replay. An add conflicts with an existing group with different files. A
replace with different files is a new write. Producers must order distinct
replacements for the same column. A partial replay pins the revision on which
the handler omitted operations. A moved pointer requires reclassification.

## Native storage

Milvus 3.0 pins milvus-storage at
`03e239603a9f39559ef2aba4f616f9cc05cbd1b3`. The synchronous
`packed.CommitManifestUpdates(base, version, ...)` opens a loon transaction
against that revision. A replace stages `loon_transaction_drop_column` and
`loon_transaction_add_column_group` in one transaction. Native apply removes
the target columns, empty groups and their manifest indexes before validating
the new groups. The overwrite resolver applies updates to the specified read
revision. A larger returned version alone cannot prove that a concurrent
catalog update is included.

## DataCoord on 3.0

Manifest preparation reads the current catalog pointer and writes the new
revision outside the segment metadata lock. A delta can rebase on updates
already published when preparation starts. At publication, DataCoord takes
`SegMu`, checks the segment state and base pointer again, and applies the
new pointer to a clone of the latest segment record. A concurrent manifest
change during I/O fails publication. Retrying the result reads the new base.
An unreferenced prepared revision can remain after a conflict or catalog error.

Add and legacy results are prepared with bounded concurrency. Each V3 batch
fits both the RPC batch cap and the catalog's transaction operation limit.
The catalog uses `CommitWithoutFallback` for manifest publication. If the
limit changes while a batch is preparing, an oversized transaction fails
before the first metadata write. Callers receive per-segment outcomes.

A replace uses its own catalog transaction. The index catalog serializes
segment index admissions/removals for that segment and locks matching builds
in build-ID order. It reclaims live build records for the replaced fields.
The same transaction publishes the segment pointer, clears the segment's
text/JSON statistics for those fields, and deletes the affected build keys.
In-memory changes follow catalog success. A subsequent build reads the new
manifest. The index definitions remain available to schedule the rebuild.

The public `BatchUpdateManifest` RPC keeps its existing broadcast path.
`CommitBackfillResult` publishes synchronously. Collection DDL can still
advance the schema between the final schema check and publication. The native
column drop also leaves named manifest statistics and materialized outputs
derived from replaced fields in place. Rebuilding those derived artifacts
requires a separate lifecycle.
