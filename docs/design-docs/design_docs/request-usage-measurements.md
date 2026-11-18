# Per-request usage measurements

A Proxy extension can measure the fields supplied to a mutation and the whole collection addressed by a read. The interface serves deployments that need one final report for a successful client RPC.

## Extension contract

The existing `hook.Extension.Report(any) int` interface keeps its existing attempt boundaries and map fields. An interested extension implements the additional method below. Returning a callback opts the current RPC into measurement. Returning nil selects the existing reporting path.

```go
NewRequestReport() func(map[string]any) (int, error)
```

Proxy obtains the callback once at RPC entry. Each search retry replaces the previous attempt's measurements. A ground-truth search for recall evaluation, a Search-by-ID vector lookup and a shard query run by Delete belong to the enclosing request. Only the final successful request invokes the callback. Validation errors, rate limits, failed tasks and a cancellation observed at completion produce no report. Different RPCs issued by an iterator are independent.

The callback returns an integer increment for the existing `milvus_proxy_report_value` counter. The labels are node, operation, database and username. It may also set `extra_read_report_value` to an integer to add to the query counter. Proxy checks that both increments are nonnegative before publishing either metric. A reporting error propagates on a read. On a mutation that has already committed, Proxy logs the reporting error and preserves the mutation result.

## Measurements

| Input | Operations | Definition |
| --- | --- | --- |
| `client_request_bytes` | Insert, Upsert | The supplied field values using schema widths for fixed-width scalars and the supplied representation for vectors. |
| `primary_key_bytes` | Upsert | The supplied primary-key field, already included in the request byte count. |
| `deleted_pk_bytes` | Delete | Int64 keys use eight bytes each. Strings use the number of UTF-8 bytes submitted to the successful tombstone write. |
| `is_filter_delete`, `delete_expr_bytes` | Delete | Classification of the original parsed expression. The byte count includes UTF-8 expression text and supplied typed template values. |
| `collection_storage_bytes` | Search, HybridSearch, Query, filtered Delete | The whole-collection logical size recorded by the serving QueryView used for this request. |
| `nq`, `vector_element_cnt` | Search | Query items and underlying input vectors. For embedding lists of three and five vectors these values are two and eight. Search by ID counts the IDs supplied, including rows with null vectors. |
| `sub_req_cnt`, `sub_total_nq` | HybridSearch | Number of subrequests and sum of their validated query counts. |

Proxy measures Insert and Upsert after resolving the write schema and before conversions, function execution, allocation of IDs and merging old rows for partial updates. The measurement includes dynamic JSON, array elements and struct children. Metadata, protobuf framing and null-validity bitmaps are excluded. A client-supplied old primary key in an AutoID upsert contributes bytes. REST handlers that produce the same field values and representation as gRPC produce the same measurements.

Proxy classifies a PK equality or membership predicate from the user plan. A filtered Delete reports one outer operation across shards. It deduplicates the retrieved keys across batches and records their bytes after the tombstone write succeeds.

## Collection size

With `queryView.publishCollectionSize` enabled, DataCoord persists one whole-collection measurement in each new DataVersion. The default is false. QueryCoord copies the scalar from the exact DataView reference it pins when preparing a shard. All shards and replicas using that version receive the same value, including shards prepared after compaction or coordinator recovery. An empty collection starts at an explicit zero. Previously published versions retain their original field presence.

For sealed data, the measurement uses the segment IDs in every shard and partition of the candidate DataView. During compaction the old version keeps its parents' bytes and the new version counts the replacement. A selected parent can already be marked Dropped in SegmentMeta. DataCoord also samples visible L0 delete logs and recorded unflushed data in live partitions, including collection-wide L0 logs. It counts each segment once. It adds insert, stats and delta bytes, text and JSON-key index bytes, and the serialized sizes of finished builds of live field indexes. Flush takes the final segment bytes from the same transaction that commits the cumulative statistics and DataView. A missing selected segment prevents publication of an incomplete measurement.

The StreamingNode includes the sample of its serving view in its existing `GetQueryPlan` reply before partition or segment pruning. The Proxy uses one whole-collection sample across all successful shard plans. During a handoff it picks the newest measured DataVersion, with the vchannel name breaking ties. A smaller sample after compaction takes effect when that view serves a request. Changes to growing data, delete logs or indexes are sampled on the next DataVersion. A placement-only change keeps its pinned version’s value. No per-read metadata RPC or time-based Proxy cache is needed.

Search by IDs uses the vector lookup plan if all requested vectors are null. Filtered Delete obtains the sample from the same internal query that finds matching keys, including a zero-match result, and validates it before writing tombstones. When automatic loading is enabled and that query finds a released collection, Proxy loads its bound collection ID and repeats the key lookup once. The write begins after the successful lookup. All metered hosts need the coordinator publication option and QueryView-aware components.
