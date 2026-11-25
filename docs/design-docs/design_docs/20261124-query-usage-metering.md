# Query usage metering

AgentBase uses a distribution-installed reporter to record one `bytes` value at Proxy for each successful Query RPC, including a QueryIterator page. The external Query response keeps its existing format.

| Contribution | Measurement |
| --- | --- |
| Predicate | For every segment selected by the plan, count the entire logical column for each planned predicate reference. A short-circuit or an empty result leaves this amount unchanged. Two separate references may each incur the column size. |
| Grep | Count the whole logical input column in each selected segment. A predicate on the same column is a separate read. Grep output is included with the other returned values. |
| Grouping and aggregation | Count the whole input columns in each selected segment. Keys and aggregate arguments that share one projection count once in that projection. `COUNT(*)` has no input column. |
| Sorting | Count the whole column for each planned sort key in each selected segment, including keys also used for grouping. |
| Output | Count the logical values in the final result at Proxy. For example, 100 returned INT64 values contribute 800 bytes. Projection alone contributes no predicate bytes. |

Filtering, MVCC and the number of groups do not reduce the planned column sizes.

Sealed workers read the field totals bound to the selected manifest revision and verify their row coverage. Sealed logical sizes include the stored validity bits. Growing workers track field payloads as they insert or recover batches. Missing required data fails the request. A known zero remains valid. For example, a recovered TEXT reference that carries no original length cannot supply a planned column size.

Proxy adds successful shard contributions and the returned values. It reports once after the request succeeds. A failed or canceled request emits no usage. Empty shard results retain their usage.

The first change uses the unary Query path for metered requests so it can receive complete usage from all selected workers. Each successful QueryIterator page is measured independently.

## Search

Search counts the original vector column and all planned predicate, grouping, aggregation, and sort columns in each selected segment. For each ANN or filter-only stage, workers multiply the planned column sizes by that stage's effective NQ. Filter-only stages include their predicates. Index sizes and hit counts do not enter the calculation.

Proxy adds successful read stages, including internal primary-key lookups and requery, to the same Search usage. A requery counts its own planned columns once per execution. Proxy adds the logical values in the final client response once and reports a single `bytes` value for the successful request. Metered Search receives worker usage through unary reduction.

## HybridSearch

Each sub-search follows the Search column rules using its own effective NQ. Workers sum these contributions. A sub-search skipped during planning contributes zero. Proxy adds successful shared requery reads and the logical values in the final fused response, then reports one `bytes` value for the successful HybridSearch RPC. The legacy advanced Search entry uses the same accounting and retains its Search operation name.

## Delete by filter

The internal lookup counts the planned predicate columns for every selected segment, including when no rows match. A primary-key delete that can build its mutation directly has no query read. A successful Delete reports the lookup bytes together with its existing write inputs under `msg_type=delete` on `milvus_proxy_report_value`. Internal lookup rows do not go to the client and contribute no returned-value bytes.

## Conditional Upsert

A `WriteCondition` lookup counts the whole primary-key column used to locate old rows and the whole source column used in the version comparison, in each selected segment. Proxy accumulates successful lookup stages across retries. On a successful Upsert, it reports these read bytes alongside the existing write inputs under `msg_type=upsert` on the same `milvus_proxy_report_value` metric.
