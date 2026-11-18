# Per-request usage measurements

A distribution can account for successful client reads and writes in bytes by registering a `hook.Extension` with `extension.SetRequestUsageReporter` from its own hook configuration. Apply that configuration in both DataCoord and Proxy. A standalone DataCoord can register the reporter without initializing the Proxy RPC hook. A stock binary has no registered usage reporter.

DataCoord checks the registration when preparing a new DataVersion. Proxy captures the reporter when a client RPC starts and keeps it through retries and both parts of a filtered Delete. Passing nil turns off measurement for subsequent operations. When enabling measurement on an existing deployment, apply the hook configuration to the coordinators first, then to the proxies once the intended collections are serving views with a size. Published versions keep their original measurement.

## Extension contract

The registered reporter uses the existing `Report(any) int` entry point. Ordinary compiled-in hooks and `.so` plug-ins continue to receive their usual reports when the distribution leaves the usage reporter unset. A measurement includes `op_type`, `database` and `username`, and one of the following sets of inputs. Numeric fields are Go `int64`.

| Operation | Measurement supplied to Report |
| --- | --- |
| Insert | `write_bytes` contains the client-supplied field values. |
| Upsert | `write_bytes` contains the client-supplied field values plus one additional copy of the supplied primary key. A 108-byte payload including an eight-byte key contributes 116 bytes. |
| Delete by key | `write_bytes` contains the primary keys in the successful tombstone write. Each Int64 key uses eight bytes. A string key uses its UTF-8 length. |
| Search | `collection_storage_bytes` is the whole logical size of the serving QueryView. `read_count` is the number of query items. Two embedding lists containing three and five vectors contribute two items. Search by ID includes requested rows with null vectors. |
| HybridSearch | The same whole-collection size and the sum of validated query counts across all subrequests. Three subrequests with two items each contribute six items. |
| Query | The same whole-collection size and a `read_count` of one. This includes Get and count queries. Each client RPC from an iterator is measured independently. |
| Filtered Delete | One `delete` report contains the bytes of the successfully written keys. A separate `query` report contains the lookup's whole-collection size and one read. A successful zero-match lookup contributes zero write bytes and one read. |

For byte-based metering, the extension returns `write_bytes` for a write and `collection_storage_bytes × read_count` for a read. A search over a 5 GB collection with two query items contributes 10 GB of logical read volume. This quantity measures the whole collection addressed by each item. The existing scanned-byte metrics measure I/O separately. A read against an empty collection contributes zero bytes.

Proxy adds each increment to `milvus_proxy_report_value{node_id,msg_type,db_name,username}`. A filtered Delete contributes to both the `delete` and `query` series. If a read product exceeds the extension's Go `int`, Proxy obtains the size of one item and applies the count when adding to the floating-point Prometheus counter. As with other Prometheus counters, exact representation of every integral byte is limited to 53 bits. Prices and customer billing responses are outside this contract.

Proxy retains measurements from the final successful attempt and reports at external RPC completion. Rate limits, failed tasks and cancellation observed at completion contribute no usage. Recall evaluation, Search-by-ID lookup and the per-shard key lookup of a filtered Delete belong to the enclosing request. With request usage disabled, the existing extension receives its usual inputs at the original attempt boundaries.

Insert and Upsert measure client fields after resolving the schema and before precision conversion, function execution, ID allocation or merging old rows during a partial update. The byte count includes dynamic JSON, array elements and struct children. Fixed-width values use their schema widths. Protobuf framing, metadata and validity bitmaps are excluded. Client-supplied old primary keys in AutoID Upserts contribute bytes. REST and gRPC requests carrying equivalent field representations have the same measurement.

## Collection size

While a request usage reporter is registered, DataCoord persists one whole-collection measurement in each new DataVersion. QueryCoord copies the scalar from the exact DataView reference it pins when preparing a shard. All shards and replicas using that version receive the same value, including shards prepared after compaction or coordinator recovery. An empty collection starts at an explicit zero. Previously published versions retain their original field presence.

For sealed data, the measurement uses the segment IDs in every shard and partition of the candidate DataView. During compaction the old version keeps its parents' bytes and the new version counts the replacement. A selected parent can already be marked Dropped in SegmentMeta. DataCoord also samples visible L0 delete logs and recorded unflushed data in live partitions, including collection-wide L0 logs. It counts each segment once. It adds insert, stats and delta bytes, text and JSON-key index bytes, and the serialized sizes of finished builds of live field indexes. Flush takes the final segment bytes from the same transaction that commits the cumulative statistics and DataView. A missing selected segment prevents publication of an incomplete measurement.

The StreamingNode includes the sample of its serving view in its existing `GetQueryPlan` reply before partition or segment pruning. The Proxy uses one whole-collection sample across all successful shard plans. During a handoff it picks the newest measured DataVersion, with the vchannel name breaking ties. A smaller sample after compaction takes effect when that view serves a request. Changes to growing data, delete logs or indexes are sampled on the next DataVersion. A placement-only change keeps its pinned version’s value. No per-read metadata RPC or time-based Proxy cache is needed.

Search by IDs uses the vector lookup plan if all requested vectors are null. Filtered Delete obtains the sample from the same internal query that finds matching keys, including a zero-match result, and validates it before writing tombstones. When automatic loading is enabled and that query finds a released collection, Proxy loads its bound collection ID and repeats the key lookup once. The write begins after the successful lookup.
