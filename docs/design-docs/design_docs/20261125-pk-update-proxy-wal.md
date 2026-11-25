# Primary-key Update admission through Proxy and WAL

A primary-key Update assigns the same field values to each selected row. For example, one request can set `age = 42` for user IDs 7 and 9. Partial Upsert reads existing rows, merges the changes and writes replacement rows. A literal Update can send just the keys and assignments to the WAL. This design covers that write path. [Tracking issue](https://github.com/milvus-io/milvus/issues/55738) · [Full feature design](https://github.com/milvus-io/milvus/pull/55186)

## Request and validation

The Proxy DML queue runs an `UpdateTask` against the collection's write schema. The request carries a nonempty list of INT64 or VARCHAR primary keys, an optional partition name and one assignment per target field. Omitting the partition selects all partitions. Proxy checks the keys and literals, resolves field names to IDs, and rejects restricted fields such as primary keys, partition keys and text-match-enabled VARCHARs. For `set_null = true`, Proxy writes the field default when present. Otherwise it writes NULL for a nullable field and rejects the assignment for a required field.

## WAL and concurrency

Proxy groups keys by VChannel. Each UPDATE header carries the collection ID, schema version and partition scope. Each body carries that fragment's keys and the shared assignments. Proxy prepares every fragment before the first append. When Proxy splitting is enabled it checks each encoded message against the applicable WAL size limit. The configured `maxInsertSize` budget counts encoded bytes across all channels, including assignments repeated in each fragment.

StreamingNode checks the collection, VChannel, schema version and partition scope from the header. A schema mismatch rejects that append and causes Proxy to invalidate its cached write schema. With partial-upsert CAS enabled, Update participates in the existing per-PK write coordination so a concurrent partial Upsert can detect a stale read.

A single fragment appends directly. Multiple fragments on one VChannel commit in one WAL transaction. VChannels commit independently, so a different channel may have committed when one fails. On success the internal handler reports the number of submitted keys and the maximum acknowledged TimeTick. Matching keys to rows happens during apply. CDC uses the existing WAL replication path and preserves the assignments and transaction boundaries.

## Rollout and verification

The public RPC returns `ErrServiceUnimplemented` for either value of `proxy.pkUpdate.enabled`. The setting is hidden, defaults to false and gates the internal `Proxy.Update` handler. Once consumers can persist, replay and serve updates, the public RPC can delegate to that handler. Operators can then enable writes separately.

Verify the public stub with both flag values, and test the internal path separately. Cover invalid assignments, a request whose fragments exceed the combined budget, stale schema and invalid partition headers, and a split transaction that fails before commit. Before enabling public writes, verify that a committed Update survives replay and is visible to queries on both the primary and a CDC replica.
