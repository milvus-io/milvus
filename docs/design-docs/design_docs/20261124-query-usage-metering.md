# Query usage metering

AgentBase uses a distribution-installed reporter to record one `bytes` value at Proxy for each successful ordinary Query. The external Query response keeps its existing format.

| Contribution | Measurement |
| --- | --- |
| Predicate | For every segment selected by the plan, count the entire logical column for each planned predicate reference. A short-circuit or an empty result leaves this amount unchanged. Two separate references may each incur the column size. |
| Grouping and aggregation | Count the values in the group and aggregate input columns after filtering and MVCC. A projected column shared by multiple aggregates is counted once at that input. `COUNT(*)` has no input column. |
| Sorting | Count the key values passed to the segment's sort operator. For grouped queries, count each sort key passed in for each group. The current Query API accepts grouping columns as sort keys. |
| Output | Count the logical values in the final result at Proxy. For example, 100 returned INT64 values contribute 800 bytes. Projection alone contributes no predicate bytes. |

Sealed workers read the field totals bound to the selected manifest revision and verify their row coverage. Sealed logical sizes include the stored validity bits. Growing workers track field payloads as they insert or recover batches. Missing required data fails the request. A known zero remains valid. For example, a recovered TEXT reference that carries no original length cannot supply a predicate size.

Proxy adds successful shard contributions and the returned values. It reports once after the request succeeds. A failed or canceled request emits no usage. Empty shard results retain their usage.

The first change uses the unary Query path for metered requests so it can receive complete usage from all selected workers. QueryIterator, Grep, Search and HybridSearch have their own request accounting.
