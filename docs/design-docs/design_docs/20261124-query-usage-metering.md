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

Sealed workers read the field totals bound to the selected manifest revision and verify their row coverage. Sealed logical sizes include the stored validity bits. Growing workers track field payloads as they insert or recover batches. Growing recovery uses the persisted payload totals when the accounting version, manifest revision, and row count match the loaded data. The worker adds the subsequent WAL inserts to those totals. Fields without a verified total are measured from the loaded values. A recovered remote TEXT reference needs a verified payload total because the reference lacks the original length. Missing required data fails the request. A known zero remains valid.

Proxy adds successful shard contributions and the returned values. It reports once after the request succeeds. A failed or canceled request emits no usage. Empty shard results retain their usage.

The first change uses the unary Query path for metered requests so it can receive complete usage from all selected workers. Each successful QueryIterator page is measured independently.

## Search

Search counts the original vector column and all planned predicate, grouping, aggregation, and sort columns in each selected segment. For each ANN or filter-only stage, workers multiply the planned column sizes by that stage's effective NQ. Filter-only stages include their predicates. Index sizes and hit counts do not enter the calculation.

Proxy adds successful read stages, including internal primary-key lookups and requery, to the same Search usage. A requery counts its own planned columns once per execution. Proxy adds the logical values in the final client response once and reports a single `bytes` value for the successful request. Metered Search receives worker usage through unary reduction.

## HybridSearch

Each sub-search follows the Search column rules using its own effective NQ. Workers sum these contributions. A sub-search skipped during planning contributes zero. Proxy adds successful shared requery reads and the logical values in the final fused response, then reports one `bytes` value for the successful HybridSearch RPC. The legacy advanced Search entry uses the same accounting and retains its Search operation name.

## Delete by filter

The internal lookup counts the planned predicate columns for every selected segment, including when no rows match. A primary-key delete that can build its mutation directly has no query read. Internal lookup rows do not go to the client and contribute no returned-value bytes.

A successful Delete reports the lookup `bytes` and the logical `write_bytes` of its delete keys under `msg_type=delete` on `milvus_proxy_report_value`. A direct delete uses the supplied keys. A filter delete uses the keys from the completed lookup sent for deletion. No matches yield zero write bytes. INT64 keys count as eight bytes each, and VARCHAR keys count their UTF-8 bytes.

## Partial Upsert

The lookup counts the entire primary-key column in each selected segment. Proxy adds the logical bytes of all old fields returned by the internal Query, including any fields the request replaces. Zero matches contribute zero returned bytes. A `WriteCondition` also adds the whole source column used in the version comparison.

Proxy accumulates successful lookup stages across retries. On a successful Upsert, it reports one read `bytes` value under `msg_type=upsert` on `milvus_proxy_report_value`. The same report includes `write_bytes`, the sum of the original request field values and the primary keys committed as deletes. Fields inherited from the lookup and generated values add no write bytes. A partial Upsert counts the original request payload once, and a channel retry adds delete bytes only when it commits.

## Full AutoID Upsert

The existence lookup counts the entire primary-key column in every selected segment, including lookups with zero matches. Proxy adds the lookup bytes to the successful Upsert's report. The delete part of `write_bytes` covers the keys selected for deletion by that lookup.

## Insert

Proxy measures the logical bytes of the client-supplied field values, including vectors and dynamic fields, before materializing the write. Generated IDs, field defaults, and function results add no write bytes. A successful Insert submits `write_bytes` to the same usage reporter and metric as queries. The legacy reporter still receives its existing request-size and row-count inputs.
