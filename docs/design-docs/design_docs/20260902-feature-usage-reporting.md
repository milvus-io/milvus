# MEP: Feature Usage Reporting

- **Created:** 2026-09-02
- **Author(s):** @czs007
- **Approver(s):** TBD
- **Status:** Draft
- **Component:** MixCoord, Proxy, QueryNode, DataCoord (index and segment metadata, import / compaction counters), QueryCoord (loaded state), internal proto
- **Related Issues:** #51149
- **Released:** TBD

## Summary

Milvus has no way to answer "which features does this instance actually use, and how many objects use
each one". The question comes up in three places: deciding whether a feature can be deprecated, measuring
adoption of a newly shipped feature, and support triage of a specific instance. Today the only sources
are ad-hoc `DescribeCollection` scripts, Prometheus counters keyed by RPC name, and access logs — none of
which can say how many collections declare a partition key, or how many search requests in the last day
used `group_by_field`.

This design adds an on-demand, pull-only report. Each role keeps its view of feature usage **in process
memory**: static usage is recomputed from metadata on every query, dynamic usage is a set of monotonic
atomic counters incremented on the request path of the role that sees the feature — the Proxy for request
options and expressions, the QueryNode for search-execution decisions, DataCoord for import and
compaction jobs. MixCoord collects the per-node views over a new internal RPC `GetFeatureUsage`, merges
them with its own metadata statistics, and exposes the result on the Proxy management port as
`GET /management/feature_usage`.

Nothing is persisted, no timer runs, no log line is written, and the request hot path gains exactly one
branch and one atomic add per counted feature. Each request counter also carries the time it was last
hit, so a consumer reading the report once a day can tell "used today" from "used once, months ago"
without keeping history of its own. The response contains only counts, timestamps, and values drawn from
closed, code-defined sets — never a user-supplied string.

The in-memory footprint is fixed at compile time: the set of counters is a constant, nothing is keyed by
collection, user, or time, and no structure outlives a single request except the counter array itself.
Consequently there is no cleanup, no eviction, and no reset — on read, on a timer, or otherwise.

The document has two parts. The first defines the mechanism: interfaces, value semantics, where counters
live, what the response may contain. The second is the initial feature catalog, which is the input to a
product decision about which request-level counters are worth implementing. Static statistics need no
such decision: they are enum- and key-driven and pick up new features without maintenance.

## Motivation

The report is consumed by an external collector (Zilliz Cloud, or a user's own tooling) that queries each
instance periodically and stores the history. Three decisions depend on it:

| Decision | Question the report must answer |
|---|---|
| Deprecation | How many collections / requests still depend on feature X? Is the number falling? |
| Adoption | Of the instances that upgraded, how many started using feature Y? |
| Support | What has this specific instance declared and what does it actually exercise? |

The three uses impose the constraints the design is built around:

- **Completeness must be detectable.** A report that silently omits a node or a role is read as "nobody
  uses this", which is the most dangerous possible misreading for a deprecation decision.
- **New features must show up without anyone editing a list.** A hand-maintained feature registry drifts
  within one release cycle. Wherever possible the statistics walk the enums and key sets the code already
  has.
- **The report must be safe to ship off-instance.** No collection names, field names, descriptions,
  model names, keys, or any other user-controlled string.

## Non-Goals

- **Persistence and history.** The instance reports its current state; the consumer keeps history.
- **Fleet aggregation.** Out of scope for the kernel; the consumer aggregates across instances.
- **Request frequency as a time series.** Per-RPC call counts already exist as
  `milvus_proxy_req_count`; this design does not rebuild them. Dynamic counters here answer "has this
  feature been used since the process started, and roughly how much", not "what is the QPS".
- **Per-collection detail.** The report is aggregated counts only. A per-collection breakdown would carry
  collection identifiers and needs a separate authorization design.
- **Per-collection attribution of request-level usage** ("which collections issued `group_by_field`
  searches"). A raw counter cannot answer it: ten million hits may come from one collection or ten
  thousand. Answering it requires state keyed by collection, which in turn requires a `DropCollection`
  hook (the pattern `CleanupProxyCollectionMetrics` already implements, `internal/proxy/impl.go:275`),
  alias handling, a time window, and a memory bound. That is a different design with a different cost
  model and, if wanted, gets its own MEP. This design deliberately keeps the request counters unkeyed so
  that none of that machinery is needed.
- **Object-level usage** ("which index, which field is used, how often"). That needs state keyed by
  index or field ID, registration and cleanup tied to load, drop and schema changes, and an answer for
  objects that no longer exist. It is a separate feature with its own MEP, not an extension of this one:
  the report here answers "is feature X used on this instance", never "is object Y used".
- **Filtering-effectiveness profiling** (selectivity, rows scanned per predicate, per-query execution
  plans). This report may say that a filter ran on a scalar index; how well it filtered is a query
  profiling question.
- **Capability discovery** ("can this build do X"). Build version and deploy mode are included for
  correlation; everything else is usage.
- **Changes to `milvus-proto` or any SDK.** All new messages live in the in-repo `pkg/proto`.

## Public Interfaces

### Internal RPC

One new rpc, identical name and signature, on three existing services:

| File | Service | Addition |
|---|---|---|
| `pkg/proto/proxy.proto` | `Proxy` | `rpc GetFeatureUsage(internal.GetFeatureUsageRequest) returns (internal.GetFeatureUsageResponse)` — one node's view |
| `pkg/proto/root_coord.proto` | `RootCoord` (served by MixCoord) | `rpc GetFeatureUsage(internal.GetFeatureUsageRequest) returns (internal.FeatureUsageReport)` — the merged report the HTTP endpoint returns |
| `pkg/proto/query_coord.proto` | `QueryNode` | `rpc GetFeatureUsage(internal.GetFeatureUsageRequest) returns (internal.GetFeatureUsageResponse)` — one node's view: the execution-path counters and the node's boolean configuration |

MixCoord has no proto service of its own; its RPCs live on the `RootCoord`, `QueryCoord` and
`DataCoord` services, and `GetQuotaMetrics` is on `RootCoord`, so the merged-report RPC follows it.

`DataNode` does **not** get the per-node RPC. Its two candidate counters, import file type and
compaction type, are counted in DataCoord instead, where the job is created. In pooled deployments a
DataNode executes tasks for many instances, so a per-DataNode count is not a per-instance number;
counting at job creation also keeps a retried task from counting twice. `QueryNode` does get the RPC:
the search path takes decisions (two-stage search, segment pruning) that neither
the request nor the coordinator metadata records, and the node's own configuration decides which
capabilities are available on it.

These are in-repo protos regenerated by `scripts/generate_proto.sh`. `milvus-proto` is untouched.

Precedent: `Proxy.GetQuotaMetrics` (`pkg/proto/proxy.proto:34`) is a purpose-specific RPC split out of
`GetMetrics`. This design follows the same pattern but with a typed response rather than a JSON string.

### Messages (`pkg/proto/internal.proto`)

```protobuf
message GetFeatureUsageRequest {
  common.MsgBase base = 1;
}

// One feature record.
message FeatureEntry {
  string group  = 1;   // see "Groups and value semantics"
  string name   = 2;   // feature identifier, see catalog
  int64  value  = 3;   // count
  string bucket = 4;   // for group = "distribution", and for request counters that are one bucket of a distribution
  int64  last_used_at = 5;  // unix seconds of the most recent hit; only for group = "request", 0 otherwise
}

// Response of a single node.
message GetFeatureUsageResponse {
  common.Status status          = 1;
  string        role            = 2;
  int64         node_id         = 3;
  int64         node_start_time = 4;   // unix seconds; a change means counters were reset
  int64         collected_at    = 5;   // unix seconds
  repeated FeatureEntry entries = 6;
}

// MixCoord's merged report, returned by the HTTP endpoint as JSON.
message FeatureUsageNode {
  string role            = 1;
  int64  node_id         = 2;
  int64  node_start_time = 3;
  bool   reachable       = 4;   // false: entries empty, error explains why
  string error           = 5;
  repeated FeatureEntry entries = 6;
}

message FeatureUsageReport {
  common.Status status   = 1;   // the merged report is itself an RPC response
  int64  collected_at    = 2;
  string build_version   = 3;
  string deploy_mode     = 4;
  repeated FeatureUsageNode nodes = 5;
}
```

### Groups and value semantics

`group` is a closed set. The meaning of `value` depends on the group; the consumer needs no other
schema.

| group | Emitted by | `value` means |
|---|---|---|
| `field_types` | MixCoord | collections with at least one user field of this `DataType`. Fields the server adds to every collection (RowID, Timestamp, the namespace field) are not counted, or every collection would report as an Int64 user |
| `index_types` | MixCoord | collections with at least one index of this `index_type` (vector and scalar share the namespace, as in `model.Index`) |
| `metric_types` | MixCoord | collections with at least one index of this `metric_type` |
| `functions` | MixCoord | collections with at least one `FunctionSchema` of this `FunctionType` |
| `providers` | MixCoord | collections with at least one embedding or rerank function of this provider |
| `declared` | MixCoord | collections for which a hand-written predicate holds (e.g. `is_partition_key`) |
| `properties` | MixCoord | collections whose **collection-level** properties contain this key; boolean-valued keys are split, see below |
| `database_properties` | MixCoord | databases whose properties contain this key; same boolean split |
| `field_params` | MixCoord | collections with at least one field whose `type_params` contain this key |
| `index_params` | MixCoord | collections with at least one index whose user index params contain this key |
| `objects` | MixCoord | count of objects of this kind in the instance (non-default databases, aliases) |
| `distribution` | MixCoord | collections falling into `bucket` for this quantity |
| `segment` | MixCoord (DataCoord meta) | collections with at least one segment having this materialized trait |
| `loaded` | MixCoord (QueryCoord meta) | collections currently loaded that have this property (a partial field load, a replica outside the default resource group) |
| `config` | QueryNode | the node reports each boolean configuration item it exposes as one entry named `key=true` or `key=false`, with `value=1`. The entry names come from the paramtable key constants, so the group carries no operator string |
| `request` | Proxy, MixCoord (DataCoord), QueryNode | monotonic count of uses of this feature since `node_start_time`; `last_used_at` is the unix time of the most recent hit, `0` if never hit in this process. Each counter is tagged with the role that owns it and appears only in that role's response. A counter with a non-empty `bucket` is one bucket of a request distribution (`ef`, `nprobe`, `limit`, `nq`, `hybrid_search_reqs`, `upsert_fields`); name and bucket together identify it, and the buckets are fixed in code like those of `distribution` |

Rules that apply across groups:

- **Boolean-valued keys are reported per value.** `mmap.enabled=true` and `mmap.enabled=false` are two
  entries, named `mmap.enabled=true` and `mmap.enabled=false`. Reporting only "key was set" would count a
  collection that explicitly *disabled* auto-compaction as a user of auto-compaction, which inverts the
  question a deprecation decision asks. The value is drawn from `{true, false}`, so the sanitization rule
  is not affected.
- **Non-boolean values are never reported.** `collection.ttl.seconds=86400` contributes one to
  `properties/collection.ttl.seconds` and nothing else. Quantities worth knowing (replica number, shard
  number) go through `distribution` with fixed buckets instead.
- **Keys that can be set at several levels are reported at each level.** `mmap.enabled` may appear in
  `properties`, `field_params` and `index_params`; the consumer decides whether to union them.
- **Only official keys are named.** A key is official if it is on the allowlist in
  `pkg/common/feature_usage_keys.go`: the key constants of `pkg/common`, listed by constant so a rename
  is a compile error, plus the Knowhere index parameters and type params that have no constant, listed as
  literals. Any other key in `properties` / `database_properties` / `field_params` / `index_params` is
  folded into a single entry `_custom` in that group, reporting only the count. The list is maintained by
  hand; `TestOfficialFeatureKeysCoverDottedConstants` fails when a dotted key constant is added to
  `common.go` without being listed, so the usual kind of new key (a property such as `mmap.enabled`) cannot
  be forgotten, while a new undotted key (an index parameter, say) folds into `_custom` until it is added.
- **`distribution` buckets are fixed in code**, not derived from data:

  | name | buckets |
  |---|---|
  | `num_partitions` | `1`, `2-16`, `17-64`, `65-1024`, `>1024` |
  | `shards_num` | `1`, `2`, `3-8`, `>8` |
  | `dim` (per vector field, max over the collection) | `<=128`, `129-512`, `513-1024`, `1025-2048`, `>2048` |
  | `max_length` (max over every field whose type params carry `max_length`, struct sub-fields and Array elements included) | `<=256`, `257-4096`, `4097-65535`, `>65535` |
  | `max_capacity` (max over Array fields) | `<=64`, `65-1024`, `>1024` |
  | `replica_number` (from `collection.replica.number`, default 1) | `1`, `2`, `3+` |
  | `loaded_replica_number` (the effective replica count of a loaded collection) | `1`, `2`, `3+` |

### HTTP endpoint

| | |
|---|---|
| Path | `GET /management/feature_usage` |
| Port | Proxy management port (9091) |
| Response | `FeatureUsageReport` serialized as JSON |
| Registration | `internal/http` `Register(&Handler{...})`, alongside the other `/management/*` routes in `internal/http/router.go` |
| Gate | `common.security.featureUsageEnabled`, default `false` |
| Auth | HTTP Basic Auth as `root`, verified through the `passwordVerifyFunc` hook Proxy already registers (`internal/proxy/meta_cache.go:80`) |

The endpoint is registered on Proxy only. MixCoord's management port does not expose it: only Proxy can
verify a user password, and the `/management/*` routes carry no global authentication today. The 2.6
`/expr` endpoint (`common.security.exprEnabled`, root Basic Auth, Proxy-only) is the precedent for this
three-part gate.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `common.security.featureUsageEnabled` | `false` | registers the HTTP endpoint |
| `common.featureUsage.countersEnabled` | `true` | enables the request counters on every role that has them (Proxy request path, QueryNode search path, DataCoord task creation). Read once at component start. When `false` the hooks return on their first line: no parameter scan, no expression walk and no JSON decode, and the counters read back as zero |

No metrics and no log lines are added.

## Design Details

### Architecture

```
consumer ──HTTP──▶ Proxy:9091 /management/feature_usage
                       │  root Basic Auth
                       ▼
                 mixCoord.GetFeatureUsage()
                       │
      ┌────────────────┼──────────────────┐
      ▼                ▼                  ▼
 own metadata     rootcoord's proxy   querycoord cluster
                  client manager
 (static stats +       │                  │
  own counters)        ▼                  ▼
                   each Proxy         each QueryNode
                 GetFeatureUsage     GetFeatureUsage
```

| Layer | Responsibility | State |
|---|---|---|
| Each node | Answer `GetFeatureUsage` from memory: static parts computed on demand, dynamic parts read from atomic counters | none persisted |
| MixCoord | On a query, fan out to every node, merge with its own metadata statistics, return one `FeatureUsageReport` | stateless |
| Proxy management port | Gate, authenticate, call MixCoord, serialize | stateless |

Fan-out reuses what `GetMetrics` already uses: the Proxy list from rootcoord's proxy client manager
(`rootcoordServer.GetProxyClientManager()`, called from `internal/coordinator/feature_usage.go`) and the
QueryNode list from the QueryCoord node manager and cluster. Each node gets a fixed 10-second timeout
(`featureUsageFanoutTimeout` in `internal/coordinator/feature_usage.go` and
`internal/querycoordv2/feature_usage.go`, the same value quotaCenter uses for `GetMetrics`); it is not
configurable. There is no
DataNode fan-out; the counters that would have lived there are kept in DataCoord, which is in the
MixCoord process and contributes them with its own static entries.

**There is no timer.** If nobody queries, nothing is computed. One query per day costs one computation
per day.

### Static statistics (MixCoord)

All inputs are in-memory metadata already held by the coordinator process. No etcd read, no cross-node
call.

| Data | Source |
|---|---|
| Collection schema, properties, partitions, aliases, databases | rootcoord `MetaTable.FeatureUsageSnapshot` — databases, available collections and the alias total, taken under one read lock in one pass over each index. Listing collections per database would walk the whole collection map once per database |
| Index type, metric type, index params, `IsAutoIndex` | datacoord `indexMeta` — `model.Index{TypeParams, IndexParams, UserIndexParams, IsAutoIndex}` |
| Segment traits | datacoord `meta` — `SegmentInfo{storage_version, is_sorted, is_sorted_by_namespace, textStatsLogs, jsonKeyStats, bm25statslogs}` |
| Build version, deploy mode | process-level values already used by `GetMetrics(system_info)` |

Four statistics mechanisms, in order of preference. The first two need no maintenance when a feature is
added; the last two rest on lists kept in code.

1. **Enum walk.** `field_types`, `functions`: iterate `schemapb.DataType` / `schemapb.FunctionType`,
   count collections per value. A new enum value is counted the day it lands.
2. **Open-value count.** `index_types`, `metric_types`: count the values that actually occur in index
   meta. These sets are validated on write (`indexparamcheck`), so only legal values reach the metadata
   and the output stays inside a code-defined set. A new index type from a Knowhere upgrade appears
   without any Milvus-side change.
3. **Counts against a fixed list.**
   - `properties`, `database_properties`, `field_params`, `index_params`: count the keys that occur, name
     them if they are on the official-key allowlist, fold the rest into `_custom`.
   - `providers`: the provider of an embedding function, and of a rerank function whose `reranker` is
     `model`, named if it is one of the providers the code knows (13 embedding providers, 8 rerank
     providers, listed in `internal/featureusage/static.go`), else folded into `_other` (`rerank:_other`
     for rerank). The provider is a free string in the function params, so an unknown value must not
     reach the report; a new provider appears as `_other` until it is added to the list.
4. **Hand-written predicates.** `declared`, `objects`, `distribution`: one function per entry,
   enumerated in the catalog and deliberately short.

Two predicates need attention because the same feature has two declaration paths and reflection over
schema booleans would miss the second:

- `enable_dynamic_field`: `CollectionSchema.enable_dynamic_field` **or** the collection property
  `dynamicfield.enabled` (set by `AlterCollection` after creation). The predicate takes the union.
- `enable_namespace`: `CollectionSchema.enable_namespace` **or** the namespace property
  (`namespace.enabled` on 2.6, `namespace.sharding.enabled` on master). Union.

Reflection over `FieldSchema` / `CollectionSchema` booleans is **not** used. It would emit
`is_primary_key` (always one per collection), `is_dynamic` (the internal `$meta` field, redundant with
`enable_dynamic_field`), `is_function_output`, and the deprecated collection-level `autoID`. The
predicate list names the booleans that are features.

Cost: one pass over collections plus one over indexes. Thousands of collections take milliseconds. Every
query recomputes; there is no cache and no result is retained between calls. The expected consumer calls
once a day (see "Consumer contract"), so a cache would protect nothing and would be the only structure in
the design that outlives a request. It is deliberately not provided.

### Dynamic counters (Proxy)

**Hot-path rule: one branch and one atomic add per counted feature. No allocation, no I/O, no lock.**

Each counter is a pair of `atomic.Int64` — `value` and `last_used_at` — in a fixed-size array indexed
by a feature id; a `map` lookup is not on the path. On a hit, `value` is incremented and `last_used_at`
is advanced to the current unix second. The advance is a compare-and-swap that only moves forward: a
goroutine that read the clock and was then descheduled must not overwrite a later second another hit
already stored, which a plain store would do. The first comparison is also the fast path, so under load
each counter still takes at most one timestamp store per second and the extra cache-line traffic is
negligible. `time.Now()` goes through the vDSO and costs tens of nanoseconds.

#### The key space is closed at compile time

**Invariant: the set of counter ids is a compile-time constant. No request field, parameter value,
collection, database, user, or time period may create a counter.** This is the property that makes the
memory footprint fixed for the life of the process and makes cleanup unnecessary, and it must be stated
because two natural counter definitions violate it:

- `rank_params.strategy` is a raw user string. On master the Proxy does not validate `RankTypeKey`;
  legacy rank parameters are passed through `newRerankMetaFromLegacy` unvalidated. A counter named by
  the value would grow with whatever clients send.
- `function_score` function names come from `rerank.GetRerankName()`, which returns
  `strings.ToLower(param.Value)` — also a user string at the point of counting.

Both are therefore counted only for the values the code recognizes (`rrf`, `weighted`, and for
`function_score` also `decay`, `model`, `boost`); any other value increments a single `_other` slot in
that family. The same rule applies to every future per-value counter: enumerate the recognized values,
fold the rest.

The repository already carries the cost of not having this invariant. Proxy Prometheus metrics are
labeled by `db_name` and `collection_name`, so they need `CleanupProxyCollectionMetrics` on
`DropCollection`, and the comment above `proxyCollectionScopedMetrics()` in `pkg/metrics/proxy_metrics.go`
records that hybrid search and upsert each leaked series once because a cleanup enumerated label values
that later grew. A counter keyed by user input inherits that whole problem; a counter over a closed id
set has none of it.

#### No reset, of any kind

Counters increase monotonically from process start. Three forms of clearing were considered and all are
rejected:

| Clearing | Why not |
|---|---|
| Reset on read | A failed read or a retry loses data permanently; a second consumer (an operator with `curl`) silently steals the first one's delta |
| Reset on a timer (e.g. daily) | The server's reset instant and the consumer's poll instant must be aligned or a window is lost or double-counted; restarted nodes drift out of phase; and it turns "on demand" back into server-side windowed sampling with a timer |
| Reset to bound memory | Unnecessary — the array is fixed size. `int64` at one million hits per second overflows after roughly 292,000 years |

`last_used_at` is what replaces clearing for the question clearing was meant to answer. "Is this feature
still in use" is read off one response: `collected_at - last_used_at` within the consumer's period means
yes, larger means no, `0` means never in this process. `value` remains available for magnitude and for
consumers that do want to difference two reads.

Usage accumulated between the last query and a process restart is lost, and a Proxy that disappears
takes its counters with it. That is accepted: dynamic counters inform adoption, deprecation decisions
rest on static statistics, and static statistics are recomputed from metadata on every query. The
consequence for interpretation is stated in "Consumer contract".

Counters are **per node**. The report lists one `FeatureUsageNode` per Proxy and per QueryNode, plus one
for MixCoord itself; MixCoord does not merge them, so the consumer can apply per-node reasoning (restart
detection, node disappearance) before summing. Every counter carries the role that owns it (Proxy for
request-level features, MixCoord for the DataCoord import-file-type and compaction-type counters,
QueryNode for the execution-path counters), and a node's `GetFeatureUsage` returns only its role's
counters: in standalone, where all roles share one process and one counter array, no slot is reported
twice.

#### One user request counts once

A counter answers "how many requests used this feature", so a request must move a counter at most
once however many times the Proxy parses or re-runs it. Two things make that true on the read paths.

**The search task accumulates into a set.** `searchTask` carries a `featureusage.FeatureSet`, a
fixed-size bitmap over the counter ids. Every hook in `PreExecute` marks into it; the set is flushed
once, with `HitAll`, when `PreExecute` returns. This is what a set buys:

- a hybrid search parses every sub-request, so `ignore_growing`, `hints` and `analyzer_name` would
  otherwise be counted once per sub-request;
- the same set is marked from the sub-request's `search_params` and from the request-level
  `rank_params`, which is where `group_size`, `strict_group_size` and `rank_group_scorer` live on the
  hybrid path — without that second scan they were never counted there at all;
- a feature named twice in one request (two unknown rerankers folding into `reranker=_other`)
  is one request that used it.

**Per-subrequest counters use a tally.** The search parameter distributions (`ef`, `nprobe`, `limit`,
`nq`) and the retrieval kind (`retrieval=*`) describe one ANN search, not one request: a hybrid search
with three subrequests is three ANN searches, and each adds one to its buckets. The task carries a
`featureusage.Tally` next to the set, 27 `uint16` slots covering exactly these counters, marked per
subrequest and flushed with the set. The request-level shape of a hybrid search — how many subrequests
(`hybrid_search_reqs`) and which retrieval families they combine (`hybrid_search=*`) — goes into the set
and counts once.

**Only the first task built for a user request counts.** `Proxy.Search` can run `node.search` several
times for one client call: the un-optimized re-search when `resultSizeInsufficient && isTopkReduce`
(`autoIndex.resultLimitCheck`, default true), the ground-truth search for recall evaluation, and a
`retry.Handle` re-entry on `ErrInconsistentRequery`. Each builds a fresh `searchTask`. A
`countFeatures` flag, set from a closure variable that the first call clears, marks the first task
only; `HybridSearch` does the same. Without it, N user searches were reported as 2N–3N.

The flush is deferred at the top of `PreExecute`, not placed at its end: a request the Proxy rejects
still asked for the feature, and the parse that marks a feature can be the step that fails.

**The query path marks internal tasks.** The Proxy synthesizes a `queryTask` for itself in three
places — the requery that fetches vectors after a search (`requeryOperator`), the retrieval an upsert
does to read the rows it replaces (`retrieveByPKs`), and the retrieval a search-by-primary-key turns
into (`handleIfSearchByPK`). Each ran the full `queryTask.PreExecute`, so one user request moved the
query counters twice and moved counters the user never set: the synthesized request pins its own
consistency level and output fields. Neither `QueryLabel` nor the existing `reQuery` flag separates
all three — `handleIfSearchByPK` uses `metrics.QueryLabel`, the same value a user query carries — so
they are marked with an explicit `internalTask` field that the counting hooks skip.

#### Where the counting hooks live

Every counted request feature is detected at a place that already parses it, so no second parse is
introduced:

| Feature class | Hook |
|---|---|
| `search_params` keys (`group_by_field` / `group_by_fields`, `iterator`, `search_iter_v2`, `radius` inside the `params` object, `group_size`, `strict_group_size`, `rank_group_scorer`, `hints`, `analyzer_name`, `ignore_growing`, `order_by_fields`) | marked into the search task's set at the `parseSearchInfo` call site in `tryGeneratePlan`, once per sub-request, plus one scan of the request-level `search_params` in `PreExecute`. `parseSearchInfo` itself stays a pure parser: it does not know whether its caller is a user request. The query-side keys are marked at the `parseQueryParams` call site in `queryTask.PreExecute`, for the same reason |
| Legacy `rank_params.strategy`, `norm_score` | `searchTask.PreExecute`, at the `selectHybridRerankMeta` call site, on the branch that neither `function_chains` nor `function_score` took |
| Request fields (`namespace`, `highlighter.type`, `function_score`, `not_return_all_meta`, `use_default_consistency` + `consistency_level`, `travel_timestamp`, and the output fields behind `output_fields=dynamic` / `output_fields=vector`) | the start of `searchTask.PreExecute` and the end of `queryTask.PreExecute`, reading the proto fields and the output fields `translateOutputFields` resolved |
| `primary_key_search` | `Proxy.search`, before it calls `handleIfSearchByPK`. It cannot be read in the task: that function resolves the ids into vectors and overwrites `search_input` with the placeholder group, and it returns before a task exists both when the resolution fails and when every id resolves to a null vector |
| `auth_method=api_key` / `auth_method=password` | both entry points that authenticate, each after the credential verifies: the gRPC interceptor (`AuthenticationInterceptorWithMetaCache`) and the RESTful middleware (`authenticate` in `internal/distributed/proxy/service.go`, also reached from `metricsPortAuthMiddleware` when the console API on the metrics port authenticates that way), which is a separate code path with the opposite decision order |
| Expression features (`text_match`, `json_contains`, `st_*`, `is null`, `like`, ...) and `filter_templating` (the `expr_template_values` map) / `expr_use_json_stats` | **after** the plan is built, by one walk over the plan's predicate tree at the three plan-creation sites (`tryGeneratePlan` for search, `QueryTask.createPlanArgs` for query, the delete runner through `tasks_alias.go`) — see below |
| Search shape (`ef`, `nprobe`, `limit`, `nq`, `retrieval=*`, `hybrid_search_reqs`, `hybrid_search=*`) | `ef` / `nprobe` / `limit` in `tryGeneratePlan` after `parseSearchInfo`, once per ANN subrequest; `nq` and the retrieval kind where the placeholder type is known (`initSearchRequest`, and each subrequest in `initAdvancedSearchRequest`); the hybrid shape once, after the subrequest loop. The per-subrequest counters go into the task's `Tally`, the rest into its set |
| Query aggregation, `order_by`, `search_aggregation` | `QueryTask.createPlanArgs` after `translateOutputFields` resolves the aggregates; the query params scan in `queryTask.PreExecute`; the start of `searchTask.PreExecute` |
| Upsert and delete modes | `upsertTask.PreExecute` right after `partial_update` is normalized (`recordUpsertFeatures`); `deleteRunner.Run` where it decides between a primary-key delete and a query-then-delete (`recordDeleteMode`); both in `internal/proxy/feature_usage_hooks.go` |
| Execution features | the Proxy sets `PlanOption.collect_feature_bits` where it builds the plan of a counted search or query, and counts the OR of the results' `feature_bits` in `SearchTask.PostExecute` / `QueryTask.PostExecute`; see "Execution features" |
| Import file type, compaction type (DataCoord) | where DataCoord accepts an import job (`ImportV2` in `datacoord/services.go`, after the duplicate-job check) and where it persists a compaction task (`enqueueCompaction` in `compaction_inspector.go`). The import hook counts the job's **distinct** file types once each, not once per file: one job carrying a thousand Parquet files is one use of Parquet |
| QueryNode execution decisions | `two_stage_search` where the delegator takes the two-stage branch (`delegator.search`), `segment_prune` where pruning removed at least one segment (`PruneSegments`), `run_analyzer` at the QueryNode `RunAnalyzer` RPC |
| QueryNode configuration (`config` group) | read at report time from the paramtable, not counted: `QueryNode.GetFeatureUsage` renders each boolean `queryNode.*` item as `key=true` / `key=false` |
| Loaded state (`loaded` group) | read at report time from QueryCoord's `CollectionManager` and `ReplicaManager`, not counted |

#### Expression counters and the parser cache

`planparserv2` caches parsed expressions by string (`exprCache`, LRU of 1024 entries with a 10-minute
TTL, `internal/parser/planparserv2/plan_parser_v2.go:30`). A counter placed inside the ANTLR visitor
would fire only on cache misses and undercount every repeated expression by orders of magnitude.

Counters therefore hang off the **output** of `ParseExpr`: one walk over the `planpb.Expr` tree per
request, setting a bitmask of expression features encountered, then one atomic add per set bit. The walk
is linear in expression size and does not allocate. This is the only counted class whose cost is more
than one branch; it is bounded by the size of the expression the request already had to parse.

The walk is invoked from all four paths that parse an expression: `Search`, `Query`, `Delete`, and each
sub-request of `HybridSearch`.

#### Counting rules that decide whether a counter carries signal

These rules exist because the SDKs populate several request fields unconditionally. A counter that
fires on "field present" for such a field measures request volume, not feature use.

| Rule | Why |
|---|---|
| Count `ignore_growing` on its **effective value** (`== true`), never on key presence; `round_decimal` and `offset` are not counted | pymilvus sends `ignore_growing` and `round_decimal` in every `search_params` |
| Do **not** count `guarantee_timestamp` | pymilvus sets it on every search and query (`ts_utils.construct_guarantee_ts`: the cached write timestamp, `1`, or `0` for Strong); `SearchIterator` pins it as well. There is no request-side signal for "the user chose a timestamp" |
| `request_consistency_level` counts `use_default_consistency == false` and records the level as the entry name (`consistency_level=Strong`, ...) | It is the only field that distinguishes a per-request override. Clients that predate `use_default_consistency` leave it `false`; the catalog notes this bias |
| Do **not** count `reduce_stop_for_best` separately | pymilvus sets it only inside `QueryIterator`; the Proxy parses it only on the query path. It is the query iterator, which `query_iterator` already counts |
| `rank_params.strategy` is counted per **recognized** value (`strategy=rrf`, `strategy=weighted`, else `strategy=_other`); `function_score` is counted per **recognized** function type (`reranker=rrf`, `weighted`, `decay`, `model`, `boost`, else `reranker=_other`) | The two are distinct client paths (`RRFRanker`/`WeightedRanker` versus a `Function` object). Neither is deprecated. Counting "rrf" or "weights" on their own would double-count across the two paths. Both values are user strings at the counting point, hence the `_other` fold (see "The key space is closed at compile time") |
| `filter_templating` counts only when the `expr_template_values` map contains a key other than `expr_use_json_stats` | The JSON-stats hint travels in the same map |
| `travel_timestamp` counts `> 0`, and is named `deprecated_travel_timestamp` | The Proxy no longer reads it for semantics; the counter measures how many clients still send a removed field, which is what the decision to drop the proto field needs |
| `highlighter` counts per `HighlightType` (`highlighter=Lexical`, `highlighter=Semantic`) | The two have different dependencies and adoption meaning |
| `primary_key_search` counts a non-empty `search_input.ids`, **not** the `search_by_primary_keys` bool | The proto marks that field "use search_input instead", nothing in the server sets it, and `ConvertHybridSearchToSearch` hard-codes it false. `handleIfSearchByPK` decides on the ids. A counter reading the flag stays at zero for every real search-by-primary-key request, which reads as "nobody uses it" |
| `norm_score` is read from the `params` JSON of the legacy `rank_params`, and from a rerank function's params on the `function_score` path; counted on its effective value | `convertLegacyParams` recognizes only `strategy` and `params` at the top level and drops the rest, so a top-level `norm_score` key cannot change behavior and no client sends one. A substring check short-circuits the unmarshal for every request that does not ask for normalization, so this, the one hook that parses, costs nothing on the requests that do not use it |
| `auth_method=*` counts **after** the credential verifies | Counting the attempt would turn the report into a brute-force log. Neither entry point runs at all unless `common.security.authorizationEnabled` is on, so on an instance without authentication both counters stay at zero, which is the truth about that instance |

`recall_eval` is not counted; it is an internal evaluation switch, not a user feature. `sub_reqs` is
not counted; the Proxy fills it itself when folding a `HybridSearch`, and the number of hybrid searches
is already `milvus_proxy_req_count{function_name="HybridSearch"}`.

### Sanitization

**The response must not contain any user-controlled string.** Allowed: integers, and strings drawn from
sets defined in code.

| Data | User-controlled | Handling |
|---|---|---|
| Collection / field / function / alias / database / role names, descriptions | yes | never emitted |
| Property values (`cipher.key`, `cipher.ezID`, TTL seconds, resource group names) | yes | never emitted; booleans are the only values reported, as `key=true` / `key=false` |
| Embedding and rerank model names, endpoints, credentials in function params | yes | never emitted |
| Property / type-param / index-param **keys** | partly — official keys are constants, users may set arbitrary keys | official keys emitted verbatim; others folded into `_custom` |
| Index type, metric type | no — validated by `indexparamcheck` | emitted |
| Embedding / rerank provider | partly — a string in the function params, validated for embedding functions on creation | named if on the code's provider list, else folded into `_other` |
| Field type, function type, consistency level, highlight type | no — enums | emitted |
| Expression feature names | no — fixed by the counter table | emitted |

The only `FunctionSchema.params` values the design reads are the provider name, the `reranker` value of a
rerank function (to count it under `reranker=*` and to decide whether it has a provider), and the
effective `norm_score`; each maps to a name drawn from a code-defined set.

### Failure handling

- **Unreachable node.** A node that times out or errors during fan-out is reported with
  `reachable=false` and `error` set; its `entries` are empty; the report is still returned. Partial
  results are always distinguishable from "no usage".
- **Static computation error.** Propagates as an HTTP 500; a half-computed static section is never
  returned, because the consumer could not tell it from a complete one.
- **Endpoint disabled.** HTTP 404 (the route is not registered), identical to any other unknown path,
  so an attacker learns nothing about the configuration.
- **Auth failure.** HTTP 401.

### Cost summary

Measured on a 32-core machine with `go test ./internal/proxy/dql/ -run XXX -bench FeatureUsage -benchmem`;
the full table, median of three runs, is under "Test Plan / Cost". The benchmarks
are checked in, so the numbers below can be re-measured rather than trusted.

| Path | Cost |
|---|---|
| Request hot path, per counted feature | one branch + one bit set in the task's `FeatureSet` |
| Request hot path, when not counting | one branch per hook. Whether a search task counts is decided once at the top of `PreExecute` (counting enabled, and the first task for the user request); otherwise every hook receives a nil set and returns before scanning a parameter list, walking an expression or decoding JSON |
| Request hot path, flush when `PreExecute` returns | 297 ns, 0 allocations, for a request that used six features; 44 ns for a request that used none. The scan is over the fixed counter-id space, so it is bounded and does not grow with the number of features a request uses. Hitting the six counters directly, which cannot dedupe, measures 261 ns |
| Request hot path, per-subrequest counters | 313 ns and 0 allocations per ANN subrequest: reading `ef` and `nprobe` from the params JSON, bucketing `limit` and `nq`, classifying the retrieval kind, and flushing the five counters it moved. 2.4 ns when the task does not count |
| Execution features, when not collected | one null check per recording site; the plan option is off on every request the Proxy does not count |
| Execution features, when collected | per expression per segment, one relaxed atomic load, plus one `fetch_or` the first time a bit is set in the request; one C call per QueryNode task to read the set; one `uint64` per result |
| Request hot path, expressions | one walk of the parsed `planpb.Expr`: 4.0 ns at one term, 740 ns at a hundred, 0 allocations |
| Request hot path, legacy `norm_score` | 20 ns and no allocation when the key is absent, which is every request that does not ask for normalization; about 450 ns and 795 B when it is present and the `params` object is unmarshalled. This is the only hook that allocates |
| Resident memory per search task | 165 bytes for the `FeatureSet` (one byte per counter) and 54 bytes for the per-subrequest `Tally`, on a struct the request already allocates |
| Counter update itself | one `atomic.Add`, plus one forward-only compare-and-swap of the timestamp at most once per second per counter |
| Resident memory per Proxy | number of counters × 16 bytes (`value` + `last_used_at`): 165 × 16 = 2,640 bytes in this build, constant for the life of the process |
| `GetFeatureUsage` on a node | copy of a fixed-size counter array |
| Static statistics | one pass over collections + one over indexes; milliseconds at thousands of collections |
| Segment traits | one pass over segment meta; tens of milliseconds at tens of thousands of segments |
| Steady state with no queries | zero |

### Consumer contract

The expected consumer polls each instance on a fixed period (once a day for the cloud collector) and
stores the response in its own database. The instance keeps no history and makes no judgment; the
consumer does both. The rules below are what the consumer must implement for the data to mean what the
three motivating questions need it to mean.

**What to store.** Every entry, verbatim, with its node context:
`(instance, node_id, node_start_time, group, name, bucket, value, last_used_at, collected_at)`. Static
and dynamic entries are stored the same way.

**Zero values are omitted from the JSON.** The endpoint serializes the generated protobuf structs with
`encoding/json`-compatible marshalling (`internal/json`), so keys are the snake_case field names, int64
values are JSON numbers, and every field tagged `omitempty` is dropped when it holds its zero value:

- an entry that carries only `group` and `name` has `value` 0 and `last_used_at` 0; "present with value
  0" below means present in that form;
- a node whose `reachable` key is missing is **unreachable** (`false` is omitted); such a node has `role`,
  `node_id` and `error`, no `node_start_time` and no entries;
- `<` and `>` in bucket labels are HTML-escaped in the raw text (`"\u003c=10"`); any JSON parser decodes
  them.

The consumer must default missing numbers to 0 and a missing `reachable` to false, rather than treat the
record as malformed.

**Snapshot groups** (every group except `request`) are a complete recomputation on every read. Store them as
a snapshot and overwrite; do not difference them. Absence rules differ by group and matter for
"nobody uses this":

| Group kind | Entry with `value = 0` | Entry absent |
|---|---|---|
| enum walk (`field_types`, `functions`) | emitted — this enum value exists in the build and no collection uses it | the enum value does not exist in this build |
| open value / fixed list (`index_types`, `metric_types`, `providers`, `properties`, `database_properties`, `field_params`, `index_params`) | never emitted | not present in any metadata |
| fixed names (`declared` except `consistency_level=*`, `objects`, the `loaded` entries `collections` / `load_fields` / `resource_groups`, the `segment` traits except `storage_version=*`) | emitted | the entry does not exist in this build |
| `config` | never 0: every switch is present as exactly one of `key=true` / `key=false`, value 1 | the switch does not exist in this build |
| per-value or per-bucket entries (`declared/consistency_level=*`, `segment/storage_version=*`, every `distribution` bucket) | never emitted | no collection falls into that value or bucket |

**The `request` group** is per-node cumulative. Two readings are supported:

- *Is it in use?* — from a single response: `collected_at - last_used_at <= period` means used within
  the last period on that node; larger means not; `last_used_at = 0` means never since that process
  started. No history needed.
- *How much?* — per node: if `node_start_time` is unchanged since the previous read, usage in the period
  is `value(now) - value(prev)`; if it changed, the process restarted and usage is `value(now)`. Sum over
  nodes after the per-node step, never before. This is the Prometheus counter contract.

A `node_id` present in the previous read and absent now took its final period of usage with it. An entry
absent from the `request` group means this build has no such counter, **not** that its value is zero.

**What the dynamic data can and cannot prove.** A non-zero delta or a recent `last_used_at` proves the
feature was used. The absence of either proves non-use only for the nodes that were alive and reachable
for the whole period. A node that restarted or vanished between reads leaves a gap that reads as "not
used". Deprecation decisions on request-level features must therefore be made on a run of consecutive
periods with stable node membership, or treated as weaker evidence than the static groups, which are
authoritative on every read. The catalog lists both under deprecation; this asymmetry is why request
counters are the smaller half of the design.

**Counters that are structurally zero.** Four entries in the `request` group can never be non-zero in
this build, and a consumer must not read them as "nobody uses this":

| Entry | Why it is always zero |
|---|---|
| `field_ops=_other` | the fold slot for a partial-update operator this build does not know; `resolveFieldPartialUpdateOps` rejects such an operator before the upsert is counted |
| `compaction=_other` | the fold slot for an unrecognized `CompactionType`; nothing produces one |
| `compaction=partition_key_sort` | no code path assigns this type. The seven places in DataCoord that construct a compaction task write only level-zero delete, mix, clustering, sort and schema-version bump, and a partition-key collection is sorted as a plain `SortCompaction` |
| `compaction=clustering_partition_key_sort` | the same |

The last two are declared in `data_coord.proto` and handled by the large-object compaction strategy
downstream, but nothing constructs a task carrying them. Sorting a partition-key collection is therefore
indistinguishable from sorting any other collection in this report. The entries are kept rather than
removed because a counter name is part of the consumer's schema: dropping one and re-adding it if the
type is ever produced would be two breaking changes instead of none. Whether the type should be produced
at all is a question for the compaction owners, not for this report.

**Polling period.** One read a day is sufficient for the static groups. For the request group the
maximum loss on a node restart equals the polling period, and Proxies restart routinely under
autoscaling and rolling upgrades; a consumer that cares about dynamic counters should poll hourly. The
call costs milliseconds, so the period is a consumer choice, not a server constraint.

## Feature Catalog (initial)

Identifiers follow the official Milvus and Zilliz Cloud documentation, in this order:

1. **Official feature name** — where the docs have a page for the feature, its title in lowercase with
   underscores: `range_search`, `primary_key_search`, `grouping_search`, `filter_templating`. Expression
   operators are named by the operator the docs show: `text_match`, `like`, `json_contains`.
2. **Official parameter key** — where the docs have no page but tell users to set a key: `group_size`,
   `analyzer_name`, `ignore_growing`; configuration keys keep their full dotted form: `mmap.enabled`,
   `collection.ttl.seconds`.
3. **Documented literal values** after `=`: `reranker=boost`, `consistency_level=Bounded`,
   `hints=iterative_filter`.
4. **Undocumented features** keep their code name and are marked *(undocumented)* in the catalog, so a
   consumer does not look for them in the public docs: `namespace`, `two_stage_search`, `exists`,
   `st_isvalid`, `consistency_level=Customized`, `highlighter=Semantic`, `import_file_type=CSV`,
   `rank_group_scorer`, `not_return_all_meta`, `deprecated_travel_timestamp`, `expr_use_json_stats`,
   `is_sorted`, `is_sorted_by_namespace`, `compaction=sort`, `compaction=mix`, `enable_namespace`,
   `scalar_index_type=FMINDEX`.

Entries that neither the docs nor the code name are named by hand, in lowercase with underscores, and are
marked `(named)` below so reviewers can see the full set.

The names were checked against milvus.io (v3.0.x) and docs.zilliz.com in September 2026. `namespace` and
`enable_namespace` are the public `CollectionSchema.enable_namespace` feature and the request `namespace`
field, which have no docs page yet; they are unrelated to what the Zilliz docs call a "namespace" (the
Partition Key).

Entries whose source is enum walk, open value or a fixed key list are listed here for reviewers to see what the
output will contain; they need no product decision and no per-item code. Entries marked **decision** are
the ones that need a yes/no.

Verified against the 2.6 branch and master (go-api v3) at the time of writing; where the two differ the
mechanism handles both.

### Modeling (`field_types`, `declared`, `distribution`)

| Entry | Group | Source | Note |
|---|---|---|---|
| every `schemapb.DataType` value | `field_types` | enum walk | 2.6: 23 types incl. `Geometry`, `Text`, `Timestamptz`, `SparseFloatVector`, `Int8Vector`, `ArrayOfVector`, `ArrayOfStruct`, `Struct`; master adds `Mol`, `Date`, `Time`, `Decimal`, `UUID` — no change needed |
| `is_partition_key` | `declared` | predicate | |
| `is_clustering_key` | `declared` | predicate | also the only declaration behind clustering compaction |
| `enable_dynamic_field` | `declared` | predicate | schema flag **or** `dynamicfield.enabled` property |
| `enable_namespace` *(undocumented)* | `declared` | predicate | schema flag **or** namespace property |
| `nullable` | `declared` | predicate | at least one nullable field |
| `default_value` | `declared` | predicate | at least one field with a default |
| `auto_id` | `declared` | predicate | primary key is auto-generated |
| `multi_vector_field` (named) | `declared` | predicate | more than one vector field |
| `struct_array` | `declared` | predicate | `CollectionSchema.struct_array_fields` non-empty |
| `num_partitions`, `shards_num`, `dim`, `max_length`, `max_capacity`, `replica_number` | `distribution` | predicate | buckets fixed above |

### Index and search configuration (`index_types`, `metric_types`, `index_params`, `field_params`)

| Entry | Group | Source | Note |
|---|---|---|---|
| every `index_type` that occurs | `index_types` | open value | vector (`HNSW`, `IVF_*`, `DISKANN`, `SCANN`, `SPARSE_*`, `GPU_*`, RaBitQ, MinHash LSH, ...) and scalar (`INVERTED`, `BITMAP`, `HYBRID`, `NGRAM`, `RTREE`, `TRIE`, `STL_SORT`) in one namespace, `AUTOINDEX` included |
| every `metric_type` that occurs | `metric_types` | open value | 2.6 has 14 values incl. the `MAX_SIM_*` family; master adds `MAX_SIM_L2` |
| every official key in `UserIndexParams` | `index_params` | fixed list | includes `refine`, `refine_type`, `sq_type`, `nbits`, `drop_ratio_build`, `inverted_index_algo`, `hybrid_low_cardinality_index_type`, `bitmap_cardinality_limit`, `json_cast_type`, `json_path`, `json_cast_function`, `mmap.enabled`, `index.nonEncoding`, `indexoffsetcache.enabled`, and the scalar index parameters `min_gram` / `max_gram` (NGRAM) and `fm_sa_sample_rate` / `fm_block_bytes` (FMINDEX). Keys that only exist as Knowhere parameters are still counted: they are keys, not values. A key the allowlist does not have is reported as `_custom`; the local verification run found the NGRAM parameters missing that way, and `TestScalarIndexParamsAreOfficialFeatureKeys` in `internal/util/indexparamcheck` now keeps the scalar index parameters defined there on the list. **Note:** with `AUTOINDEX`, `refine` / `sq_type` are chosen server-side and do not appear in user params |
| `autoindex` | `declared` | predicate | `model.Index.IsAutoIndex` |
| every official key in field `type_params` | `field_params` | fixed list | `enable_analyzer`, `analyzer_params`, `multi_analyzer_params`, `enable_match`, `mmap.enabled`, `field.skipLoad`, `dim`, `max_length`, `max_capacity` |

Removed from the earlier draft after verification: `materialized_view_search_info` (no such key; the
user-facing switch is the `partitionkey.isolation` property), `EMB_LIST_META` (segcore-internal),
`rbq_bits_query` (a query-time parameter, never in index metadata).

### Functions (`functions`, `providers`)

| Entry | Group | Source | Note |
|---|---|---|---|
| every `FunctionType` | `functions` | enum walk | `BM25`, `TextEmbedding`, `Rerank`, `MinHash`, `MolFingerprint` on master |
| every embedding provider that occurs | `providers` | fixed list, else `_other` | 13: `openai`, `azure_openai`, `bedrock`, `dashscope`, `vertexai`, `voyageai`, `cohere`, `siliconflow`, `tei`, `zilliz`, `gemini`, `huggingface`, `yc` (12 on 2.6, without `yc`) |
| every rerank provider that occurs | `providers` | fixed list, else `rerank:_other` | `ali`, `cohere`, `huggingface`, `siliconflow`, `tei`, `vllm`, `voyageai`, `zilliz`; prefixed `rerank:` to keep the namespace separate; only a rerank function whose `reranker` is `model` has a provider |

Model names are user strings and are never emitted.

### Collection and database properties (`properties`, `database_properties`, `objects`)

All official keys are counted against the allowlist; the table lists the ones reviewers asked about.

| Entry | Group | Note |
|---|---|---|
| `collection.ttl.seconds`, `collection.replica.number`, `collection.resource_groups`, `collection.autocompaction.enabled=true/false`, `cipher.enabled=true/false`, `mmap.enabled=true/false`, `warmup.*`, `indexoffsetcache.enabled=true/false`, `load_priority`, `field.skipLoad`, `partitionkey.isolation=true/false`, `index.nonEncoding=true/false`, `timezone`, `query_mode`, `allow_insert_auto_id=true/false` | `properties` | fixed list; 2.6 also has `lazyload.enabled` (deprecated, removed on master) |
| `collection.*Rate.*`, `collection.diskProtection.diskQuota.mb`, `partition.diskProtection.diskQuota.mb` | `properties` | fixed list. Quotas are tuning knobs, not features; they are reported because the mechanism reports every official key. **decision:** whether the consumer filters them out, not whether the kernel emits them |
| `database.replica.number`, `database.resource_groups`, `cipher.enabled=true/false`, `database.diskQuota.mb`, `database.max.collections`, `database.force.deny.*=true/false` | `database_properties` | fixed list. The deny flags are operational state, same remark as quotas |
| `consistency_level=<level>` | `declared` | enum; the collection default; `consistency_level=Customized` is *(undocumented)* |
| `databases` (non-default), `aliases` | `objects` | predicate; each is an object count, read from the MetaTable cache. Role, grant and privilege-group counts were considered and left out of the first version: listing them reads the KV catalog rather than memory, and nothing yet needs them |

### Segment traits (`segment`)

| Entry | Note |
|---|---|
| `storage_version=<V1\|V2\|V3>` | collections with at least one segment on that storage format version; the direct measure of columnar-storage migration. The legacy format is stored as `0` (`storage.StorageV1`) and reported as `V1` |
| `is_sorted` *(undocumented)*, `is_sorted_by_namespace` *(undocumented)* (named after `SegmentInfo.is_sorted`, `is_sorted_by_namespace` — `is_partition_key_sorted` on 2.6), `text_match_index`, `json_shredding`, `full_text_search` (read from `textStatsLogs`, `jsonKeyStats`, `bm25statslogs`) | collections with at least one live segment (not dropped, not invisible) carrying the trait |

These differ in kind from the rest: they report what was **materialized**, not what the user declared.
They require a pass over segment metadata, which is why they were planned as a separate phase.

Import file types and compaction types are not coordinator metadata (the job records are
garbage-collected), so they are **request-group counters**: `import_file_type=<JSON|JSONLine|NumPy|Parquet|CSV>` (`CSV` is
*(undocumented)*) and `compaction=<mix|l0|clustering|sort|partition_key_sort|clustering_partition_key_sort|bump_schema_version>`,
one value per `CompactionType` (`sort` and `mix` are *(undocumented)*; unrecognized types fold to
`compaction=_other`). They are counted in
**DataCoord**, where the job is created, not on the DataNode that executes it. On a deployment where
DataNodes are pooled across instances a per-DataNode count answers a different question than the report
asks, and counting at creation also keeps a retried task from counting twice. Counters carry a role tag,
and each role's RPC returns only its own slots, so a standalone process (one shared counter array) never
reports a slot twice.

### Loaded state (`loaded`, `distribution`)

QueryCoord holds the only record of what is loaded right now, which is not derivable from the collection
metadata MixCoord already walks. It contributes four entries, all computed on demand from
`CollectionManager` and `ReplicaManager`:

| Entry | Group | Note |
|---|---|---|
| `collections` | `loaded` | how many collections are loaded |
| `load_fields` | `loaded` | collections whose load request named fewer fields than the schema has. Load metadata written before `load_fields` existed is upgraded to the full list on recovery, so an empty list counts as "everything" |
| `resource_groups` | `loaded` | collections with at least one replica outside `__default_resource_group`. Counted once per collection, not per replica. Resource group names are operator strings and are never emitted |
| `loaded_replica_number` | `distribution` | the effective replica count per loaded collection, in the `replica_number` buckets. It differs from the declared `collection.replica.number` property, which is what the `distribution` entry of the same name reports |

### QueryNode configuration (`config`)

`QueryNode.GetFeatureUsage` reports the twenty boolean `queryNode.*` configuration items that switch a
capability on or off, each as one entry named by its configuration key, `key=true` or `key=false`, with
`value=1` (`queryNodeConfigEntries` in `internal/querynodev2/services.go`):

- `queryNode.enableDisk`
- `queryNode.segcore.interimIndex.enableIndex`
- `queryNode.segcore.tieredStorage.evictionEnabled`, `queryNode.segcore.tieredStorage.backgroundEvictionEnabled`
- `queryNode.segcore.multipleChunkedEnable`, `queryNode.segcore.enableGeometryCache`,
  `queryNode.segcore.enableGISSplitFusion`
- `queryNode.mmap.vectorField`, `queryNode.mmap.vectorIndex`, `queryNode.mmap.scalarField`,
  `queryNode.mmap.scalarIndex`, `queryNode.mmap.growingMmapEnabled`, `queryNode.mmap.jsonShredding`
- `queryNode.exprCache.enabled`
- `queryNode.enableSegmentPrune`, `queryNode.enableSegmentFilter`, `queryNode.skipGrowingSegmentBF`
- `queryNode.search.enableResultZeroCopy`, `queryNode.preferFieldDataWhenIndexHasRawData`
- `queryNode.idfOracle.preload`

Only booleans are reported. A non-boolean configuration value can be an operator string (a path, a host,
a size expression), which would break the rule that every emitted string is drawn from a code-defined
set; a boolean has exactly two values, and the key is a constant in the paramtable.

### QueryNode execution path (`request`)

Three counters record decisions taken inside the node that neither the request nor the metadata shows.

A QueryNode counter is per **QueryNode request**, which is not the user's request: a delegator fans one
user search out to one request per shard, and to a separate request per segment group, since a search
task carries a single `DataScope`. Compare these counters against each other, not against the Proxy's.

| Entry | Signal |
|---|---|
| `two_stage_search` *(undocumented)* | the delegator took the two-stage search branch |
| `segment_prune` | segment pruning removed at least one sealed segment from a search or query |
| `run_analyzer` | the `RunAnalyzer` RPC, the one user-facing feature only a QueryNode serves |

A `brute_force_search` counter was implemented and removed during review. It fired when a search reached
a segment with no Go-side index metadata on the search field, but that does not establish that the
segment was scanned brute force: a growing segment may still use an interim index built in segcore, and
filter-only requests reach the same code. A counter whose name promises more than its signal can show is
worse than no counter, so it was left out until the execution path itself reports which index ran. The
next section is that report; `interim_index_search` there is what `brute_force_search` could not be.

### Execution features (`request`, counted by the Proxy)

These answer "did a feature the user set up actually take effect at run time": a scalar index that was
built, the interim index, the expression result cache, JSON shredding, the second phase of a strict
grouping search. They are feature statistics, not per-object usage: nothing names the index, field or
collection that was used (see Non-Goals).

**How they travel.** The Proxy sets `PlanOption.collect_feature_bits` on the plan of a request it counts
and on no other. segcore then gives the plan a `FeatureRecorder`, a 64-bit atomic set shared by every
copy of the plan options, so every segment the plan runs on, concurrently or not, records into one set;
a feature already recorded costs one relaxed load. After the segments finish, the QueryNode reads the set
through `GetSearchPlanFeatureBits` / `GetRetrievePlanFeatureBits` into `feature_bits` on
`internalpb.SearchResults` / `RetrieveResults`. The delegator ORs it wherever it merges results, next to
the storage cost it already sums, and the Proxy ORs the results of one request and counts each feature
once. The bit positions are a wire format shared by `common/FeatureBits.h` and
`internal/featureusage/execbits.go`, append only, and a Go test parses the header so the two cannot
drift. A plan without the option records nothing, and every recording site is then one null check.

| Entry | Recorded where | Signal |
|---|---|---|
| `filter_exec_path=scalar_index` / `pk_index` / `text_match_index` / `json_shredding` / `brute_force` | `SegmentExpr::EnsureExecPathDetermined`, the one place every expression settles its path | the path a filter expression took on a segment |
| `filter_exec_path=ngram_index` | the NGRAM phase-one calls, single LIKE and batched LIKE | the NGRAM index served a LIKE; it is chosen outside the path above |
| `scalar_index_type=BITMAP` / `STL_SORT` / `Trie` / `INVERTED` / `HYBRID` / `RTREE` / `NGRAM` / `FMINDEX` *(undocumented)* / `json_flat` | the pinned index object behind a `scalar_index` path | which kind of scalar index served it, from the index object, never from user input |
| `filter_index_declined` | the same place | the field has a scalar index but the expression ran on raw data: the operator or literal is one the index does not serve, or a cost guard declined it |
| `expr_cache_hit` | the expression result cache and the whole-filter cache | a filter result was served from the cache |
| `interim_index_search` | growing-segment search and the sealed binlog-index branch | the interim index served a vector search |
| `strict_group_size_effective` | the strict grouping search | its second phase, the re-search restricted to unfinished groups, ran |
| `tiered_storage_cold_read` | the QueryNode in Go, and the Proxy | the request read bytes from remote storage. The QueryNode sets the bit from the storage cost before it is split across merged requests, since the split rounds small counts to zero; the Proxy also counts it when the summed `scanned_remote_bytes` of the results is positive. Needs `queryNode.segcore.tieredStorage.storageUsageTrackingEnabled` |

Semantics the consumer relies on:

- **Determined, not always executed.** A filter's path is recorded when it is settled, which can be
  during prefetch; a later whole-filter cache hit or a conjunction that has run out of rows may skip the
  evaluation. That changes how often, never whether a feature is in use.
- **Merged requests share a set.** A QueryNode merges searches with identical plans into one execution;
  each gets the union.
- **Internal filters are not features.** The TTL filter segcore adds on its own records nothing, nor does
  the raw-data refine pass of a fused GIS filter. Raw data is not counted as a declined index where it is
  the designed path (membership filters, timestamptz arithmetic, IS NULL over a JSON path index).
- **Only searches and queries report.** The retrieval behind a delete or an upsert, and the Proxy's own
  requery, do not.

### Request-level features (`request`) — **decision per row**

Each row is one counter and one hook; cost grows linearly with the number of rows. The recommended set
is a ceiling, not a target. The "signal" column states the detection rule where it is not simply "field
present". Every row marked "yes" is implemented on the implementation branch; see "Local Verification"
for which of them were also exercised end to end.

#### Search and query controls

| Entry | Signal | Recommend |
|---|---|---|
| `grouping_search` | a group-by field resolved: `group_by_field`, or `group_by_fields` when that is absent, in the search params, or the request-level `rank_params` on the hybrid path | yes |
| `group_size` / `strict_group_size` | key present | yes |
| `rank_group_scorer` *(undocumented)* | key present | yes |
| `group_by_fields` (query path) | key present | yes |
| `search_iterator=v1` | search request with `iterator` set to `true` / `True` **and** `search_iter_v2` not set | yes — old search iterator protocol. pymilvus's v2 search iterator sends both keys, so without the exclusion every v2 page would also count as the old protocol |
| `search_iterator=v2` | `search_iter_v2` parses as true (it requires `iterator` as well; without it the request is rejected before counting) | yes — new protocol; keep separate to watch migration |
| `query_iterator` | query request with `iterator=true` | yes — the query iterator; previously counted under the same name as the old search iterator |
| `range_search` | `radius` present in the `params` object of the search params (a `range_filter` without `radius` is rejected before counting) | yes |
| `ignore_growing` | `== true` | yes |
| `hints=iterative_filter`, `hints=_other` | `hints` key present; `iterative_filter` is the documented value, any other value is a user string and folds to `hints=_other` | yes |
| `analyzer_name` | key present | yes |
| `primary_key_search` | `search_input` carries a non-empty `ids` | yes — not the deprecated `search_by_primary_keys` bool, which nothing sets |
| `namespace` *(undocumented)* | field set | yes |
| `output_fields=dynamic` | `translateOutputFields` resolved a dynamic field | yes |
| `output_fields=vector` | `translateOutputFields` resolved at least one vector field, including a vector inside a struct array, and including the ones a `*` expands to | yes — asked for by the Cloud side: whether a read carries raw vectors back is what response size and network cost hang on |
| `auth_method=password`, `auth_method=api_key` | the credential a request authenticated with, counted after it verifies | yes — asked for by the Cloud side, to see which authentication path clients use. Neither moves unless `common.security.authorizationEnabled` is on; `auth_method=api_key` additionally needs a hook extension that can verify a key, so in a build without one it stays at zero by construction |
| `not_return_all_meta` *(undocumented)* | field `true` | yes |
| `consistency_level=<level>` | `use_default_consistency == false` | yes — see bias note above; `consistency_level=Customized` is *(undocumented)* |
| `deprecated_travel_timestamp` *(undocumented)* | `travel_timestamp > 0` | yes — measures clients still sending a removed field |
| `partition_names` | non-empty | no — broadly used, no information |
| `limit` (search) | — | yes, as a distribution; see "Search shape" below |
| `offset` / `round_decimal` | — | no — basic paging |
| `query_aggregation=count` / `sum` / `min` / `max` / `avg` | the aggregation operators a query's output fields resolve to, once each | yes — `avg` reaches the Proxy as a sum and a count sharing the name `avg(x)`, so the operator is read back from that name and an `avg` does not also count as `sum` and `count` |
| `order_by` (query) | `order_by_fields` key present in the query params | yes |
| `order_by_fields` (search) | `order_by_fields` key present in the search params | yes |
| `search_aggregation` | `SearchRequest.search_aggregation` set | yes |
| `guarantee_timestamp` | — | **no — no request-side signal** (see counting rules) |
| `reduce_stop_for_best` | — | no — subsumed by `query_iterator` |
| `recall_eval` | — | no — internal |

#### Expression features (one AST walk, all rows are set bits)

| Entry | Recommend |
|---|---|
| `text_match` | yes |
| `phrase_match` | yes |
| `random_sample` | yes |
| `json_contains` (incl. `_all`, `_any`) | yes |
| `json_path` (access to a JSON path) | yes |
| `array_contains` (incl. `_all`, `_any`) | yes |
| `array_length` | yes |
| `st_contains`, `st_within`, `st_intersects`, `st_crosses`, `st_overlaps`, `st_touches`, `st_equals`, `st_dwithin`, `st_isvalid` (`st_isvalid` is *(undocumented)*) | yes — nine bits (one per `GISOp`); **decision:** report separately or collapse to `geospatial` on the consumer side |
| `timestamptz_compare` (named) | yes |
| `like` | yes |
| `exists` *(undocumented)* | yes |
| `is_null` / `is_not_null` (named) | yes |
| `regex_match`, `element_filter`, `struct_array_match` | yes — added in implementation: the regex operator and the struct-array element filter / match predicates are distinct capabilities the walk sees for free |
| `filter_templating` | yes — the request carries `expr_template_values`, excluding the `expr_use_json_stats` key |
| `expr_use_json_stats` *(undocumented)* | yes — passed through `expr_template_values`; this is a request-level hint, not metadata |
| `comparison_operators=equality` (`==`, `!=`), `comparison_operators=relational` (`>`, `>=`, `<`, `<=`, and a range such as `1 < x < 5`), `in_operator` (`in`, `not in`) | yes — added after review: they show which filter shapes a scalar index would serve. Documented under Basic Operators; a field-to-field comparison counts under its operator |
| arithmetic, logical operators | no — basic syntax |

#### Hybrid search, rerank, highlight

| Entry | Signal | Recommend |
|---|---|---|
| `strategy=rrf`, `strategy=weighted`, `strategy=_other` | recognized `strategy` value in the legacy rank params (`RRFRanker` / `WeightedRanker`); anything else folds to `_other`; absent key counts nothing | yes |
| `norm_score` | `norm_score` true inside the `params` object of `rank_params`, or on a rerank function's params | yes — a top-level `rank_params` key is dropped before it reaches the reranker |
| `reranker=rrf` / `weighted` / `decay` / `model` / `boost` / `_other` | recognized function name in `function_score`; anything else folds to `_other` | yes |
| `highlighter=Lexical`, `highlighter=Semantic` *(undocumented)* | `SearchRequest.highlighter.type` | yes — **decision:** Semantic maturity; if not GA, the consumer should label it |
| `fragment_size` / `num_of_fragments` | key present in highlighter params | yes |
| `hybrid_search_reqs` | subrequest count, buckets `<=2`, `3`, `4-5`, `6-10`, `>10` | yes — once per hybrid search |
| `hybrid_search=<families>` | which retrieval families the subrequests combine: the seven non-empty combinations of `dense_vector`, `sparse_vector`, `full_text_search`, joined with `+` in that order | yes — once per hybrid search; embedding-list and element-level subrequests count as `dense_vector` |

#### Search shape (per ANN subrequest)

Counted once per ANN search: a plain search is one, a hybrid search is one per subrequest. The values
are the ones the client sent. The Proxy does not know which index serves a field, so a search on an
HNSW field also reports `nprobe` as omitted and an IVF search reports `ef` as omitted; the consumer reads
the two together with `index_types`.

| Entry | Buckets | Recommend |
|---|---|---|
| `ef` | `omitted`, `<=16`, `17-64`, `65-256`, `257-1024`, `>1024` | yes |
| `nprobe` | `omitted`, `<=8`, `9-32`, `33-128`, `129-1024`, `>1024` | yes |
| `limit` | `<=10`, `11-100`, `101-1000`, `1001-16384`, `>16384` — the requested limit, before an iterator clamps it or the offset is added; `>16384` is reachable only where a limit above the top-k cap is accepted, such as the old search iterator | yes |
| `nq` | `1`, `2-10`, `11-100`, `101-1000`, `>1000` | yes |
| `retrieval=dense_vector` / `sparse_vector` / `full_text_search` / `embedding_list` / `element_level` | what the subrequest searches: a dense vector field; a sparse field with vectors; a sparse field with text through a BM25 function; a struct's vector array as an embedding list, or per element. Exactly one per subrequest | yes |

#### Writes

| Entry | Signal | Recommend |
|---|---|---|
| `upsert_mode=override` / `upsert_mode=merge` | `partial_update`, read after the Proxy normalizes it: a non-`REPLACE` field operator promotes the request to a merge | yes |
| `field_ops=REPLACE` / `ARRAY_APPEND` / `ARRAY_REMOVE` / `PATH_REPLACE` / `_other` | each partial-update operator the request carries, once per request; `_other` is the fold slot for an operator this build does not know, which is rejected before counting and so stays at zero | yes — what reaches the server depends on the client: pymilvus 3.1 drops an explicit `REPLACE` before sending (it is the default on the wire) and has no `PATH_REPLACE` in its proto, so from pymilvus only `ARRAY_APPEND` and `ARRAY_REMOVE` move |
| `upsert_fields` | fields the upsert carries, buckets `1`, `2-4`, `5-16`, `>16` | yes |
| `delete_mode=ids` / `delete_mode=filter` | whether the delete expression is only a primary-key match, which is how the SDKs send `delete(ids=...)` and which deletes without a query, or a filter the Proxy queries first | yes |

`output_fields=vector` and the two `auth_method` entries were added after the first review round, at the
request of the Cloud side, which wanted "does this client read raw vectors" and "API key or username and
password" per instance. The report answers both per instance; the daily and weekly shape, and any ratio,
are the collector's to derive, since the counters are cumulative and are never reset. A collector that
diffs two pulls has to treat a negative delta as a restart.

A starting subset that covers the deprecation and migration questions currently open, at 13 counters:
`search_iterator=v1`, `search_iterator=v2`, `deprecated_travel_timestamp`, `consistency_level=*`,
`grouping_search`, `range_search`, `primary_key_search`, `namespace`, `not_return_all_meta`,
`random_sample`, `phrase_match`, `reranker=*`, `highlighter=*`.

## Test Plan

Three layers, each answering a different question: unit tests for "does this
compute the right thing", a checked-in surface file for "did the report change
under the consumer", and an integration suite for "does a real request move the
right counter".

### Unit tests

**Static statistics.** Tests build the inputs the statistics functions take
(`CollectionInput` and `model.Index` / `datapb.SegmentInfo` values) directly and
assert the exact entry set: every `DataType`, every `FunctionType`, an index
of each type present, boolean properties at both values, a custom key folded
into `_custom`, `enable_dynamic_field` declared via the property only,
`enable_namespace` via the property only, each `distribution` bucket boundary, and a
collection with no fields of a type contributing zero (the entry is still
present with `value=0` for enum-walk groups, absent for open-value and fixed-list groups).

**Loaded state and node configuration.** Tests put collections into QueryCoord's
`CollectionManager` and `ReplicaManager` and assert the `loaded` entries: a full
load and a partial load, a replica in the default resource group and one outside
it, and the `loaded_replica_number` buckets. A test asserts the resource group
name never appears in the produced entries. For the `config` group, a test
asserts exactly one of `key=true` / `key=false` is present per item and that it
matches the paramtable value.

**Counters.** `last_used_at` is set to the current second on a hit and not
stored again within the same second; it is `0` for every non-`request` entry and
for a counter never hit in this process; a read modifies neither `value` nor
`last_used_at`. Role-scoped snapshots partition the array with no overlap, so a
standalone process never reports a slot twice.

**Fan-out.** A mocked cluster with one Proxy erroring at the transport, one
returning a non-success status and the rest returning entries: every node is
present, the failures carry `reachable=false` and an `error`, and node ids come
back in order. The QueryNode fan-out is covered the same way.

### The report surface is a checked-in file

Renaming a group, renaming a counter or dropping one breaks the consumer while
nothing in Milvus fails to compile. `internal/featureusage/testdata/report_surface.golden`
lists every group and every counter with its role; `TestReportSurfaceIsStable`
compares the code against it and prints the added and removed lines. Adding a
counter is expected and cheap:

```
go test ./internal/featureusage/ -run TestReportSurfaceIsStable -update-surface
```

and the updated file is committed with the change. The set of reported QueryNode
configuration keys is pinned the same way, in the QueryNode test.

### Integration suite

`tests/integration/featureusage` runs against a real cluster. Its core
assertion is a delta, not a threshold: it snapshots the Proxy counters, issues
**one** request, snapshots again, and asserts that **exactly** the expected
counters moved, by exactly the expected amounts. A hook that fires on the wrong
request fails as loudly as one that stops firing.

Every search moves the per-subrequest counters (`ef`, `nprobe`, `limit`, `nq`, `retrieval=*`), and
every filter moves an execution feature (`filter_exec_path=*`, ...), so the request-level cases compare
the report with both left out, while the tests for the per-subrequest counters and the execution features
compare only those. Each side is still an exact delta.

| Test | What it pins |
|---|---|
| `TestSearchCounters` | 21 search requests moving 22 search-side counters one request at a time (grouping, range search, both iterator protocols, hints, namespace, primary-key search, the highlighter and its fragment parameters, `reranker=decay` and `reranker=_other`, `output_fields=vector`, ...), including that a default-consistency search moves nothing, that `ignore_growing=false` is not counted, that iterator v2 excludes `search_iterator=v1`, and that a scalar output field does not count as a vector one |
| `TestQueryCounters` | the query-path hooks: the query iterator (`query_iterator`), `group_by_fields`, `output_fields=dynamic`, `output_fields=vector` including the vector a `*` expands to, `filter_templating`, an explicit consistency level |
| `TestExpressionCounters` | one query per expression kind, each asserting exactly the counters it moves: its own, plus `json_path` or `comparison_operators=*` where the expression contains one; and that an infix LIKE counts as `like`, not `regex_match` |
| `TestExpressionCounterIsNotCached` | five queries with the same expression string count five, proving the counter sits on the parser's output rather than inside it |
| `TestUnknownRankStrategyFoldsToOther` | an unknown `strategy` value increments `_other` and creates no new slot: the key space stays closed |
| `TestStaticGroupsAndSanitization` | the declared, properties and distribution groups on a freshly created collection, and that a sentinel used as the collection name, a field name, a property key and a property value appears nowhere in the serialized report |
| `TestLoadedGroup` | a collection loaded with a subset of its fields is counted as a partial load |
| `TestQueryNodeGroups` | every config entry is `key=true`/`key=false` with value 1 per node, a filtered pure-ANN search moves `two_stage_search`, and `RunAnalyzer` moves its counter |
| `TestUnreachableNodeIsReported` | a QueryNode killed without deregistering is still listed, with `reachable=false` and an error |
| `TestImportFileTypesAreCounted` | one import job each for the JSON, JSONLine and CSV formats moves that format's DataCoord counter |
| `TestProvidersAndCustomResourceGroup` | a text-embedding function reports its provider while its endpoint never appears, and a collection loaded into a named resource group is counted without the group's name leaving the node |
| `TestGeoAndTimeExpressions` | the nine geospatial predicates and the timestamp-with-timezone comparison, on a collection with a Geometry and a Timestamptz field |
| `TestStructArrayExpressions` | `element_filter` and `struct_array_match` on a struct array field, and `retrieval=embedding_list` / `retrieval=element_level` searches on its vector sub-field |
| `TestMoreSearchCounters` | the remaining consistency levels, the null predicates, the regex operator and the JSON-stats hint |
| `TestRerankCounters` | both rerank client paths: the legacy `strategy` values with `norm_score` inside the `params` object, plus a negative control that a top-level `norm_score` key counts nothing, and a `FunctionScore` naming `rrf`, `weighted`, `model` and `boost` (`decay` is in `TestSearchCounters`); also `highlighter=Semantic` |
| `TestCompactionTypesAreCounted` | sort, level-zero delete and schema-version-bump compactions, each triggered by the user action that produces it |
| `TestClusteringCompactionAndSegmentPrune` | clustering compaction, and the QueryNode `segment_prune` counter it makes reachable |
| `TestBinaryImportFileTypesAreCounted` | the Parquet and NumPy import formats |
| `TestSearchParameterDistributions` | every bucket of `ef`, `nprobe`, `limit` and `nq`, asserted on the per-subrequest counters only; a three-subrequest hybrid search adding three to its buckets and one to `hybrid_search_reqs`; the remaining `hybrid_search_reqs` buckets |
| `TestRetrievalKinds` | `retrieval=sparse_vector` and `retrieval=full_text_search` on a collection with a dense, a sparse and a BM25 output field, and every combination `hybrid_search=*` can report |
| `TestAggregationAndOrdering` | each query aggregation operator, `avg` next to its own parts counting each operator once, `order_by` on query, `order_by_fields` on search, `search_aggregation` |
| `TestExecFeatures` | every execution feature on a real cluster: each `filter_exec_path` and `scalar_index_type` one filter at a time on a collection with one field per index kind (above the 1024-row threshold below which a segment is never indexed), a filter the index declines, a conjunction of an indexed and a raw predicate, a cached filter result, JSON statistics, the interim index on growing data, the second phase of a strict grouping search on data with one rare group, and a cold read from a collection without scalar warmup. The suite turns on the interim index, the expression cache (admission threshold 1) and remote-read accounting to reach them |
| `TestDeleteAndUpsertModes` | both delete modes, both upsert modes including the promotion to a merge, every partial-update operator, and the `upsert_fields` buckets |
| `TestZZCoverage` | the acceptance gate, below |

`tests/integration/featureusageauth` is a second package, with its own cluster, for the two
`auth_method` counters. They only move when `common.security.authorizationEnabled` is on, and that flag
makes every request in a suite need a credential; turning it on for the main suite would mean rewriting
every unrelated test method there. It pins three things: an authenticated request moves
`auth_method=password` and leaves the API key counter alone; a wrong password moves neither, since the
counters sit after verification; and an API key moves nothing in a build whose hook extension cannot
verify one, which is what keeps that counter's `notDrivable` entry honest.

The suite reads the report through `MixCoord.GetFeatureUsage`, the same call the
HTTP endpoint makes. The endpoint's handler is covered by a unit test: 401 without
credentials, with a wrong password and for a non-root user, 405 for a non-GET
method, 200 for root, and 500 when MixCoord fails. That the route does not exist
when `common.security.featureUsageEnabled` is off (404) is registration-time
behavior and is checked on a running instance under "Local Verification".

### The acceptance gate

The last thing the suite does is read the golden surface file, take the report,
and require that **every counter it lists has a non-zero value**. A counter that
no test drove fails the run by name. This is what makes the catalog verifiable
rather than aspirational: adding a row to the catalog without exercising it does
not compile past CI.

Six entries are listed in the test with their reasons. `auth_method=password`
is driven end to end, only in the sibling suite that can turn authentication on;
`auth_method=api_key` is pinned at zero there; the rest cannot be driven at all:

| Entry | Why |
|---|---|
| `auth_method=password` | the authentication interceptor only runs when `common.security.authorizationEnabled` is on, which would make every other request in this suite need a credential. Driven in `tests/integration/featureusageauth` |
| `auth_method=api_key` | `VerifyAPIKey` delegates to the hook extension, and the built-in `DefaultHook` rejects every key, so no request in this tree can authenticate with one. `tests/integration/featureusageauth` pins that it stays at zero without a hook, so the day a build can verify a key, that test fails and this row moves out of the list |
| `field_ops=_other` | the fold slot for a partial-update operator this build does not know; `resolveFieldPartialUpdateOps` rejects such an operator before the upsert is counted |
| `compaction=_other` | the fold slot for an unrecognized `CompactionType`; no request can produce one |
| `compaction=partition_key_sort` | no DataCoord path constructs this type. `CompactionTriggerType.GetCompactionType` emits only level-zero delete, mix, clustering, sort and schema-version bump, and a partition-key collection is sorted as a plain `SortCompaction` |
| `compaction=clustering_partition_key_sort` | the same: declared in the proto and handled defensively downstream, but nothing in this tree produces it |

The two partition-key sort types are worth acting on separately. They are catalog rows that can
never be non-zero, so a consumer would read them as "this never happens" when
the truth is "this cannot happen". They should either be removed from the
catalog or kept with that note.

Two counters read zero on a default-configured instance for a different reason,
and the suite turns their switches on so they can be exercised:
`segment_prune` needs `queryNode.enableSegmentPrune`, a clustering key, more
than one segment out of clustering compaction and the partition statistics
delivered to the delegator; `two_stage_search` needs
`autoIndex.twoStageSearch.enabled` and a top-k above its threshold.

### Cost

`internal/proxy/dql/feature_usage_bench_test.go`, `go test ./internal/proxy/dql/ -run XXX -bench FeatureUsage
-benchmem -count=3` on a 32-core machine, median of three:

| Benchmark | Result |
|---|---|
| `SearchFlush/marked_and_flushed`: six features marked into the set, flushed once | 297 ns/op, 0 allocs |
| `SearchFlush/empty_request`: flushing a set nothing was marked in | 44 ns/op, 0 allocs |
| `SearchFlush/direct_hits`: the six counters hit directly, without the set | 261 ns/op, 0 allocs |
| `SearchUnits/one_subrequest`: `ef`/`nprobe` read from the params JSON, `limit`/`nq` bucketed, retrieval kind classified, five counters flushed | 313 ns/op, 0 allocs |
| `SearchUnits/counting_off`: the same hooks on a task that does not count | 2.4 ns/op, 0 allocs |
| `LegacyNormScore/without_key` / `with_key` | 20 ns/op, 0 allocs / about 450 ns/op, 795 B/op, 12 allocs |
| expression walk, 1 / 10 / 100 terms | 4.0 / 53 / 740 ns/op, 0 allocs |
| `ParseSearchInfo`, counters on / off | about 20 µs/op either way, 96 allocs: a guard that the parser stays free of counting, not a measure of the counters |

The flush scans the fixed counter-id space, so its cost does not grow with the number of features a
request uses; a counter update is one atomic add plus a forward-only compare-and-swap of `last_used_at`
at most once per second per counter. The expression walk is linear in the size of the predicate the
request already parsed and allocates nothing. A task that does not count pays a branch per hook.

## Rollout

1. Land P0 with `common.security.featureUsageEnabled=false`. Nothing changes for any deployment.
2. Enable on an internal cluster; compare the report against a `DescribeCollection` sweep.
3. The request counters are on by default (`common.featureUsage.countersEnabled=true`); the measured
   per-request cost is in "Cost summary". An operator who wants none of it sets the key to `false`, which
   also turns off execution-feature collection in segcore, since the Proxy then never asks for it.

## Delivery Phases

All four phases, and the counters added in the third review round (search shape, write modes, execution
features), are implemented in #53311. The table records how the work was split.

| Phase | Scope |
|---|---|
| P0 | Protos, `internal/featureusage/` static statistics and the counter array, MixCoord merge and Proxy fan-out, HTTP endpoint with gate and auth, and the Proxy hooks for the starter counters that are plain request fields or `search_params` keys (`grouping_search`, `search_iterator=v1`, `search_iterator=v2`, `range_search`, `primary_key_search`, `namespace`, `not_return_all_meta`, `deprecated_travel_timestamp`, `consistency_level=*`, `reranker=*`, `highlighter=*`) |
| P1 | The expression AST walk (`random_sample`, `phrase_match`, `text_match`, `json_contains`, geospatial, …) and any further rows the product selects |
| P2 | Segment traits from DataCoord meta; the import-file-type and compaction-type counters in DataCoord |
| P3 | QueryNode `GetFeatureUsage` RPC with the execution-path counters and the `config` group; QueryCoord fan-out to QueryNodes; the `loaded` group from QueryCoord metadata |

Adding a role or a group later changes nothing on the consumer side; the protocol is the same for every
node.

## Local Verification

Run on 2026-09-24 against a standalone instance built from the implementation branch
(`feat/feature-usage-report` at `3c06abe35a`, plus the allowlist fix described below; woodpecker WAL,
dependencies on the shared etcd / MinIO / Pulsar containers), driven by pymilvus 3.1.0rc8. Every counter
was reset by a restart before the workload, so the report below is one run. Besides
`common.security.featureUsageEnabled=true`, the instance turned on the configuration-gated paths the report
has counters for: `queryNode.enableSegmentPrune`, `autoIndex.twoStageSearch.enabled` (with `minTopk=1`,
`minNumSegments=1`), `queryNode.segcore.interimIndex.enableIndex`, `queryNode.exprCache.enabled` (with
`minEvalDurationUs=0`, `admissionThreshold=1`) and
`queryNode.segcore.tieredStorage.storageUsageTrackingEnabled`; and `dataCoord.segment.maxSize=4` with
`dataCoord.compaction.clustering.preferSegmentSizeRatio=0.5` so clustering compaction emits more than one
segment on a test-sized dataset.

**Workload.** Eleven collections and one database, then 130 request steps through the public pymilvus API
(searches, queries, upserts, deletes and one `run_analyzer`, including five searches on growing data; an iterator step sends
more than one request), 3 compaction and schema steps and 5 import jobs. A request the server rejects still moves its request
counters, because they are taken when the Proxy parses it; five steps were rejected that way and are
marked below.

- `fu_main`, 2,000 rows (above the 1,024-row threshold below which a segment is never indexed): one field
  per scalar index kind (`STL_SORT`, `INVERTED`, `AUTOINDEX` on an Int64, which builds `HYBRID`, `BITMAP`,
  `Trie`, `NGRAM` with `min_gram`/`max_gram`, `FMINDEX`, `RTREE`, a flat `INVERTED` JSON index and a
  `DOUBLE` JSON path index), an analyzer- and match-enabled VarChar, JSON, an Int32 array, a nullable VarChar,
  Timestamptz, an unindexed Int64, a group-by Int64, an 8-dim `IVF_FLAT`/`L2` vector, a dynamic field;
  properties `mmap.enabled=true`, `collection.ttl.seconds`, `timezone` and a custom key; one alias.
- `fu_hybrid`: dense (`HNSW`/`COSINE`), sparse (`SPARSE_INVERTED_INDEX`/`IP`) and a `BM25` function output,
  consistency `Strong`.
- `fu_struct`: a struct array with an Int32 and a vector sub-field (`HNSW`/`MAX_SIM_L2`).
- `fu_partkey`: VarChar partition key, 16 partitions, loaded with `load_fields` a strict subset.
- `fu_clustering`: Int64 clustering key, 100,000 rows in 20 flushes, then a clustering compaction.
- `fu_cold` (`warmup.scalarField=disable`), `fu_writes` (client-set primary key, an array, JSON and fourteen
  Int64 columns), `fu_growing` (20,000 rows searched while still growing), `fu_strict` (one rare group next
  to the query vector), `fu_text_embedding` (a `TEI` text-embedding function; a local stand-in served the
  endpoint), `fu_import`; database `fu_db` with two properties.
- Requests: every search and query option in the catalog that pymilvus can send; each expression kind; each
  bucket of `ef`, `nprobe`, `limit`, `nq`; each retrieval kind; hybrid searches over every family
  combination and subrequest-count bucket; each query aggregation; upserts in both modes with array
  operators and every field-count bucket; both delete modes; one filter per execution path and index kind;
  a repeated JSON-statistics filter for the cache; five searches on growing data; a strict grouping search;
  a filter on the cold collection; a clustering-key-filtered search; `run_analyzer`; a mix and a
  level-zero compaction; an added field; one import job per file format.
- Rejected after counting: an undocumented hint (segcore rejects it), a `SemanticHighlighter` (no model
  endpoint configured), an element-level search on an index built for embedding lists, and an unknown
  legacy strategy and an unknown reranker.

**Gate and auth**, on the management port: no credentials `401`; wrong root password `401`; root `200`.
Restarted without `common.security.featureUsageEnabled`, the same request returns `404`: the route does not
exist.

**Report.** `build_version=feat-feature-usage-report-20260925-3c06abe35a`, `deploy_mode=STANDALONE`, three
nodes, all `reachable=true`. Non-zero entries:

| node | group | entries |
|---|---|---|
| mixcoord | `field_types` | `Int64=11`, `FloatVector=11`, `VarChar=4`, `Array=3`, `JSON=2`, `SparseFloatVector=1`, `ArrayOfVector=1`, `Geometry=1`, `Timestamptz=1` (19 other types present at 0; the dynamic `$meta` field is not counted as `JSON`, a struct's sub-fields count as arrays) |
| mixcoord | `functions` / `providers` | `BM25=1`, `TextEmbedding=1` / `tei=1` |
| mixcoord | `declared` | `auto_id=7`, `consistency_level=Bounded=10`, `consistency_level=Strong=1`, `enable_dynamic_field=1`, `is_clustering_key=1`, `is_partition_key=1`, `multi_vector_field=2`, `nullable=2`, `struct_array=1`, `autoindex=1` |
| mixcoord | `index_types` / `metric_types` | `IVF_FLAT=5`, `FLAT=3`, `HNSW=2`, `SPARSE_INVERTED_INDEX=1`, `STL_SORT=1`, `INVERTED=1`, `HYBRID=1`, `BITMAP=1`, `Trie=1`, `NGRAM=1`, `FMINDEX=1`, `RTREE=1` / `L2=7`, `IP=2`, `COSINE=1`, `BM25=1`, `MAX_SIM_L2=1` |
| mixcoord | `index_params` / `field_params` | `nlist=5`, `M=2`, `efConstruction=2`, `json_cast_type=1`, `json_path=1`, `min_gram=1`, `max_gram=1` / `dim=11`, `max_length=4`, `max_capacity=3`, `enable_analyzer=true=2`, `enable_match=true=1` |
| mixcoord | `properties` / `database_properties` | `timezone=11`, `namespace.sharding.enabled=false=11`, `mmap.enabled=true=1`, `collection.ttl.seconds=1`, `collection.replica.number=1`, `warmup.scalarField=1`, `_custom=1` / `database.max.collections=1`, `database.force.deny.writing=false=1` |
| mixcoord | `objects` | `databases=1`, `aliases=1` |
| mixcoord | `distribution` | `dim\|<=128=11`, `shards_num\|1=11`, `replica_number\|1=11`, `num_partitions\|1=10`, `num_partitions\|2-16=1`, `max_length\|<=256=4`, `max_capacity\|<=64=3`, `loaded_replica_number\|1=9` |
| mixcoord | `segment` | `storage_version=V2=10`, `is_sorted=10`, `json_shredding=2`, `text_match_index=1`, `full_text_search=1` |
| mixcoord | `loaded` | `collections=9`, `load_fields=1` (`resource_groups=0`: one resource group on a standalone instance) |
| mixcoord | `request` | `import_file_type=JSON=1`, `JSONLine=1`, `NumPy=1`, `Parquet=1`, `CSV=1`; `compaction=sort=41`, `mix=9`, `l0=1`, `clustering=1` |
| proxy | `request` | 141 of 149 counters non-zero; the 8 at zero are listed below |
| querynode | `config` | all 20 switches, each at exactly one of `key=true` / `key=false`; the ones the run turned on read back as `true` |
| querynode | `request` | `two_stage_search=2`, `segment_prune=1`, `run_analyzer=1` |

The Proxy counters that moved, by class (full values in the report file kept with the run):

- search and query options: `grouping_search=4`, `group_size=2`, `strict_group_size=2`,
  `rank_group_scorer=1`, `range_search=1`, `search_iterator=v1=2`, `search_iterator=v2=2`,
  `query_iterator=2`, `primary_key_search=1`, `ignore_growing=1`, `hints=iterative_filter=1`,
  `hints=_other=1`, `analyzer_name=1`, `output_fields=dynamic=1`, `output_fields=vector=1`,
  `group_by_fields=1`, `order_by=1`, `order_by_fields=1`, `search_aggregation=1`, `consistency_level=` each
  of `Strong=6`, `Session`, `Bounded`, `Eventually`, `Customized` once;
- ranking and highlighting: `strategy=rrf=11`, `strategy=weighted=1`, `strategy=_other=1`, `reranker=` each
  of `rrf`, `weighted`, `decay`, `model`, `boost`, `_other` once, `norm_score=2`, `highlighter=Lexical=1`,
  `highlighter=Semantic=1`, `fragment_size=1`, `num_of_fragments=1`;
- expressions: every expression counter, from `comparison_operators=relational=19` and
  `comparison_operators=equality=14` down to one each for the nine `st_*` operators (`st_intersects=2`);
- search shape: every bucket of `ef`, `nprobe`, `limit` (including `>16384`, through the old search
  iterator, which clamps the limit instead of rejecting it), `nq`, `retrieval=*`, `hybrid_search_reqs` and
  `hybrid_search=*`, e.g. `ef|omitted=94`, `nprobe|<=8=38`, `limit|<=10=95`, `retrieval=dense_vector=82`;
- aggregation and writes: `query_aggregation=count=17` and one each of `sum`, `min`, `max`, `avg`;
  `upsert_mode=override=1`, `upsert_mode=merge=5`, `field_ops=ARRAY_APPEND=1`, `field_ops=ARRAY_REMOVE=1`,
  every `upsert_fields` bucket, `delete_mode=ids=2`, `delete_mode=filter=1`;
- execution features: `filter_exec_path=brute_force=20`, `scalar_index=16`, `pk_index=10`,
  `json_shredding=6`, `text_match_index=2`, `ngram_index=1`; `scalar_index_type=RTREE=9` and one each of
  the other eight kinds; `filter_index_declined=2`, `expr_cache_hit=3`, `interim_index_search=5`,
  `strict_group_size_effective=1`, `tiered_storage_cold_read=1`.

The counts reconcile with the workload. For example `strategy=rrf=11` is the seven family combinations,
the three subrequest-count buckets and the grouped hybrid search, all with `RRFRanker`;
`scalar_index_type=RTREE=9` is the R-Tree case plus the eight geospatial predicates the index serves
(`st_isvalid` is the ninth and does not use it); `query_aggregation=count=17` is every `count(*)` the workload sent;
`interim_index_search=5` is the five searches on growing data; `consistency_level=Strong=6` is the one
explicit Strong search plus the five growing-data searches.

**Zero, and why.** On the Proxy: `namespace`, `not_return_all_meta` and `deprecated_travel_timestamp` have
no pymilvus 3.1 API; `field_ops=REPLACE` is dropped by pymilvus before sending and `field_ops=PATH_REPLACE`
is not in its proto; `auth_method=*` need `common.security.authorizationEnabled`; `field_ops=_other` is
structurally zero. On MixCoord: `compaction=bump_schema_version` needs
`dataCoord.compaction.bumpSchemaVersion.enabled` and storage V3 (`common.storage.useLoonFFI`), which this run left
off because V3 changes every collection's storage; the other three are structurally zero. The integration
suite drives every one of these that can be driven.

**Report shape.** An excerpt of the report the endpoint returned, decoded and reformatted:

```json
{
  "collected_at": 1790298786,
  "build_version": "feat-feature-usage-report-20260925-3c06abe35a",
  "deploy_mode": "STANDALONE",
  "nodes": [
    {
      "role": "mixcoord", "node_id": 6, "node_start_time": 1790298357, "reachable": true,
      "entries": [
        { "group": "field_types", "name": "FloatVector", "value": 11 },
        { "group": "properties", "name": "mmap.enabled=true", "value": 1 },
        { "group": "properties", "name": "_custom", "value": 1 },
        { "group": "distribution", "name": "num_partitions", "value": 10, "bucket": "1" },
        { "group": "distribution", "name": "num_partitions", "value": 1, "bucket": "2-16" }
      ]
    },
    {
      "role": "proxy", "node_id": 6, "node_start_time": 1790298357, "reachable": true,
      "entries": [
        { "group": "request", "name": "grouping_search", "value": 4, "last_used_at": 1790298694 },
        { "group": "request", "name": "limit", "value": 95, "bucket": "<=10", "last_used_at": 1790298694 },
        { "group": "request", "name": "limit", "value": 1, "bucket": "11-100", "last_used_at": 1790298685 },
        { "group": "request", "name": "filter_exec_path=scalar_index", "value": 16, "last_used_at": 1790298694 },
        { "group": "request", "name": "namespace" }
      ]
    },
    {
      "role": "querynode", "node_id": 6, "node_start_time": 1790298357, "reachable": true,
      "entries": [
        { "group": "config", "name": "queryNode.enableDisk=false", "value": 1 },
        { "group": "request", "name": "two_stage_search", "value": 2, "last_used_at": 1790298694 }
      ]
    }
  ]
}
```

The excerpt is decoded: the raw text escapes `<` and `>` in bucket labels (`"\u003c=10"`). Zero-valued
fields are omitted (see "Consumer contract"): an entry with only `group` and `name`, like `namespace`
above, has `value` 0 and `last_used_at` 0, and an unreachable node would carry `role`, `node_id` and
`error` with no `reachable` key. A standalone instance runs every role in one process, so all three nodes
share one `node_id`; each role still reports only its own entries.

**Sanitization.** The serialized report contains none of the collection, alias, database, field or
function names, the custom property key or its value, the partition key values, the inserted text, the
text-embedding endpoint, or the unknown strategy, reranker and hint strings the rejected requests carried.

**Found by this run.** The first run of the workload reported the `NGRAM` index's `min_gram` / `max_gram`
as `_custom`: the official-key allowlist lacked the scalar index parameters that have no constant in
`pkg/common`. They are now on it, with `fm_sa_sample_rate` / `fm_block_bytes` for `FMINDEX`, and a test in
`internal/util/indexparamcheck` keeps the two in step. The run also confirmed two behaviors a reader of the
report needs: a segment under 1,024 rows is never indexed, so filters on it report `brute_force` even with
an index declared; and JSON statistics take precedence over a JSON index on the same field unless the
request turns them off.

**The integration suite run.** The manual run above and `tests/integration/featureusage` verify different
things and both are kept. The manual run is the only place the HTTP endpoint, its gate and its Basic Auth
are exercised against a real Proxy management port, with a real SDK's request shapes; the suite reads the
report over `MixCoord.GetFeatureUsage` and is the one that proves every catalog row is reachable, with
requests built directly where no SDK sends a shape. Its latest full run on the same branch:

| | |
|---|---|
| Test methods | 24, all passing |
| Wall clock | 271 s for the package, one cluster |
| Counters exercised | every entry in the surface file except the six listed under "The acceptance gate"; `auth_method=password` is driven in the sibling authentication suite, which also passes |
| Groups present | all 16 |

Two entries in the report need state that outlives the test that created it, so those tests deliberately
leave it behind: the collection carrying a text-embedding function, because `providers` is a group whose
entries disappear when no collection has a function, and a database carrying a property, for
`database_properties`. Everything else is cleaned up.

**Observations for the consumer.** `timezone` and `namespace.sharding.enabled=false` are written by the
server on every collection and show `N/N`; they are official keys and are reported as designed, but they do
not indicate a user choice. `max_field_id` is server-managed and excluded. The compaction counts are
inflated by the small `dataCoord.segment.maxSize` this run used. Configuration-gated counters read zero on
a default instance (`segment_prune`, `two_stage_search`, `interim_index_search`, `expr_cache_hit`,
`tiered_storage_cold_read`): a zero there means "not enabled", not "not used". For three of them the
`config` group tells the two apart (`queryNode.enableSegmentPrune`,
`queryNode.segcore.interimIndex.enableIndex`, `queryNode.exprCache.enabled`); the gates of the other two,
`autoIndex.twoStageSearch.*` and `queryNode.segcore.tieredStorage.storageUsageTrackingEnabled`, are not in
the report.

## Design Decisions

### D1. A dedicated RPC, not a new `metric_type` on `GetMetrics`

`GetMetrics` is a hot path: quotaCenter calls it on every Proxy, QueryNode and DataNode every
`quotaCenterCollectInterval` (3 s, `configs/milvus.yaml:1387`). Feature usage is queried once a day.
Sharing the entry point couples the two, and `GetMetrics` already multiplexes more than a dozen
`metric_type` strings over an untyped JSON request and response. `GetQuotaMetrics` set the precedent for
splitting a purpose-specific RPC out of it.

### D2. Typed proto response, not a JSON string

`GetQuotaMetricsResponse.metrics_info` is a `string`. This design does not follow that part of the
precedent: the catalog will change across releases and the consumer should get schema evolution from
proto rather than from a documented JSON convention.

### D3. Monotonic counters with `last_used_at`; no reset of any kind

Reset-on-read breaks with two consumers and loses data when a read fails. Reset-on-timer needs the
server and the consumer to agree on a phase and turns an on-demand design into a sampled one. Neither is
needed for memory, which is fixed. The consumer's actual question — "still in use?" — is answered by
`last_used_at` from a single read, and "how much?" by differencing `value` with `node_start_time` as the
reset signal, which is the counter contract every metrics consumer already implements.

### D3a. The counter id set is a compile-time constant

Stated as an invariant because it is what makes cleanup unnecessary, and because the two per-value
counters in the catalog (`strategy`, `reranker`) are named by user strings at the counting point
and had to be given an `_other` fold to satisfy it. The precedent for what happens without this
invariant is `CleanupProxyCollectionMetrics` and the leak history recorded next to it.

### D4. Partial reports with per-node `reachable`, never a whole-report failure

An unreachable node must not make the report fail (the rest is still useful) and must not be silently
omitted (silence reads as "unused"). The per-node flag is the only shape that satisfies both.

### D5. Boolean property values are reported; nothing else is

The deprecation question for a boolean switch is "who turned it off", which "key was set" cannot answer.
`true` / `false` are not user strings. Every other value stays unreported.

### D6. Counters fire on effective values, not on key presence

SDKs send several keys unconditionally. A presence-based counter for those measures request volume.
The rule is stated per counter in the catalog so that the implementer does not have to rediscover it.

### D7. Expression counters run on the parsed tree, after the cache

The parser cache makes visitor-side counting wrong by construction. One walk over `planpb.Expr` per
request is the cheapest correct placement and covers all four expression-bearing request types.

### D8. No feature registry in code

Static statistics walk enums and key sets. There is no registry of every feature, which would drift
within a release. The lists that are kept in code are short and enumerated in this document: the predicate
table for `declared` / `objects` / `distribution`, the official-key allowlist (D9), the known embedding and
rerank providers, the server-managed property keys left out of `properties`, and the twenty QueryNode
configuration switches. Two of them are guarded by tests: the allowlist
(`TestOfficialFeatureKeysCoverDottedConstants`, `TestScalarIndexParamsAreOfficialFeatureKeys`) and the
QueryNode switch list (the QueryNode test pins it). The provider lists and the server-managed keys are
not: a provider added upstream reports as `_other`, a new server-written property as itself, until the
list is updated.

### D9. Custom keys folded, official keys named, allowlist in `pkg/common`

The allowlist is `pkg/common/feature_usage_keys.go`. It lists the key constants of `pkg/common` by
constant, so a rename is a compile error, and the Knowhere index parameters and type params that have no
constant as literals. It is a second list, so it is guarded: `TestOfficialFeatureKeysCoverDottedConstants`
parses `common.go` and fails when a dotted key constant (the shape every collection or database property
has) is missing from it. A new key that is not dotted, such as an index parameter, reports as `_custom`
until it is added; that is the safe direction, since `_custom` never carries a user string.

### D10. Off by default, root Basic Auth, Proxy only

The management port has no global authentication. The endpoint is gated three ways like the 2.6 `/expr`
endpoint. Default-on would need an assessment of whether root Basic Auth is sufficient exposure for a
report that, while sanitized, describes an instance's shape.

### D11. No per-collection breakdown

It would carry collection identifiers, which conflicts with the sanitization rule, and would need its
own authorization design. Aggregates answer the three motivating questions.

### D12. Request counters are not attributed to collections

Attribution is the one extension that would force periodic cleanup: state keyed by collection needs a
drop hook, alias handling, a time window and a memory bound. The static groups already attribute
declared features to collections exactly; request-level attribution is deferred to its own MEP rather
than admitted here in a reduced form.

### D13. The v1 record shape is not adopted

An earlier draft modeled each feature as a thirteen-field record (`stage`, `since`, `deprecated_in`,
`available`, `currently_used`, `first/last_detected_at`, `detected/total_samples`, `usage_count`,
`detail`, …) persisted to etcd and refreshed by a sampler. This design keeps `value` and adds only
`last_used_at`. Of the rest: `layer` is `group`; `currently_used` is derived by the consumer from
`last_used_at` or `value > 0`; `first_detected_at` and the sample ratios are consumer-side history or
artefacts of a sampler this design does not have; `stage` / `since` / `deprecated_in` are properties of
a release, keyed by the `build_version` already in the report, and belong in the consumer's tables;
`available` is capability discovery, a non-goal; `detail` is an open structure and conflicts with the
sanitization rule. The complexity of the earlier model came from the instance owning history and
judgment; moving both to the consumer is what lets the record stay at five fields.

## Rejected Alternatives

### Prometheus metrics

Three mismatches. **Open values versus label cardinality:** index types, property keys and providers
are reported as whatever occurs, and property keys can be user-defined; as labels their cardinality is
unbounded, and constraining them to an allowlist reintroduces the maintenance burden D8 removes.
**Continuous versus on-demand:** a gauge must be current at every scrape, so a once-a-day statistic
either recomputes on every 15-second scrape or is refreshed by a timer, which is periodic sampling by
another name. **Shape:** the data is node → group → feature → value with named buckets; Prometheus is
flat, and its histogram buckets are cumulative with a different meaning. Request frequency as a time
series is what Prometheus is for, and `milvus_proxy_req_count` already exists; this design does not
replace it.

### A periodic snapshot log line

A complete design for this existed. It was not chosen because: a log stream carrying "current state"
needs batch head/tail markers and a line count for the consumer to detect an incomplete batch, and a
process crashing mid-batch produces data indistinguishable from "these features are unused"; dynamic
counters reset after each emission lose the pre-restart accumulation; the consumer must regroup lines by
role, node and timestamp and treat static and dynamic batches differently; and users without a log
pipeline have to grep. Its one advantage, that all nodes write to one place, is replaced by the MixCoord
fan-out.

### Persisting statistics in etcd

Adds a write path and a schema to migrate for data the consumer already stores. The instance is the
data source, not the historian.

### Reflection over schema booleans

Considered for `declared`. Rejected because it emits internal and deprecated flags (`is_primary_key`,
`is_dynamic`, `is_function_output`, collection-level `autoID`) and misses the property-based second
declaration path for dynamic fields and namespaces. The explicit predicate list is ten entries.

### Server-side usage windows (ring buffer of hourly buckets per counter)

Would let a single read answer "how many hits in each of the last 24 hours" without consumer history.
Rejected: it needs bucket rotation (lazy on write is possible, but it is still a clock-driven state
machine per counter), multiplies resident memory by the window length, fixes the window length in the
server while the consumer's period is the consumer's choice, and the consumer already gets the same
series by polling hourly and differencing `value`. `last_used_at` covers the single-read "still in use"
question at a cost of eight bytes.

### Reset on read, or on a timer

See D3. Both were the natural first answer to "the value is still there tomorrow, the consumer will think
it is still in use"; `last_used_at` answers that without destroying data or adding a timer.

## Open Questions

1. *(Resolved.)* Which rows of the request catalog are in P1? Every row marked "yes" is implemented.
2. *(Resolved: nine, one per `GISOp`.)* Geospatial predicates: one counter each or one in total?
3. Should the `objects` group be extended to RBAC objects (roles, grants, privilege groups)? They were left out because counting them reads the catalog.
4. *(Resolved: implemented.)* Segment traits: P2 as proposed, or in P0 given the cost is tens of milliseconds?
5. Endpoint default: off (proposed) or on?
6. Is the `_custom` fold sufficient for non-official keys, or is a prefix-level breakdown wanted?
7. Naming: `GetFeatureUsage` / `/management/feature_usage` (proposed) versus `GetFeatureStats`; `Stats`
   collides with segment statistics terminology in the repo.
8. Should `last_used_at` be second-granular (proposed) or coarser (minute)? Second granularity costs
   at most one store per second per counter; a coarser grain buys nothing measurable and loses
   resolution for consumers that poll hourly.

## Implementation Map

| Design element | Code |
|---|---|
| Messages | `pkg/proto/internal.proto` — `GetFeatureUsageRequest/Response`, `FeatureEntry`, `FeatureUsageNode`, `FeatureUsageReport` |
| RPCs | `pkg/proto/proxy.proto` (`Proxy`), `pkg/proto/root_coord.proto` (`RootCoord`, served by MixCoord), `pkg/proto/query_coord.proto` (`QueryNode`) |
| Static statistics | new `internal/featureusage/` — enum walk, open value, fixed key and provider lists, predicate table, `distribution` buckets, sanitization allowlist |
| Counter array (`value` + `last_used_at` per slot), the constant feature id set, `_other` folding | `internal/featureusage/counters.go` |
| Official key allowlist | `pkg/common/feature_usage_keys.go` — `IsOfficialFeatureKey`; `TestOfficialFeatureKeysCoverDottedConstants` keeps it in step with the constants file |
| Collection-side static entries | `internal/rootcoord/root_coord.go` — `Core.GetFeatureUsage`, `collectFeatureUsageInput` (rootcoord `MetaTable`) |
| Index-side static entries | `internal/datacoord/feature_usage.go` — `Server.FeatureUsageEntries`; `index_meta.go` — `indexMeta.ListAllIndexes` |
| MixCoord merge and Proxy fan-out | `internal/coordinator/feature_usage.go` — `GetFeatureUsage`, `collectProxyFeatureUsage` |
| Proxy RPC and HTTP handler | `internal/proxy/impl.go` — `GetFeatureUsage`; `internal/proxy/management.go` — `FeatureUsage` (route registered only when enabled); `internal/http/router.go` — `RouteFeatureUsage` |
| Proxy counter hooks | search and query: `internal/proxy/dql/feature_usage_hooks.go`, called from `SearchTask.PreExecute` / `tryGeneratePlan` / `initSearchRequest` / `initAdvancedSearchRequest` / `PostExecute` and `QueryTask.PreExecute` / `createPlanArgs` / `PostExecute`; upsert and delete: `internal/proxy/feature_usage_hooks.go` (`recordUpsertFeatures`, `recordDeleteMode`), called from `task_upsert.go` and `task_delete.go`; the delete path's expression walk reaches `dql.RecordPlanExprFeatures` through `tasks_alias.go`; primary-key search and authentication in `internal/proxy/impl.go`, `internal/proxy/authentication_interceptor.go` and `internal/distributed/proxy/service.go` |
| Expression walk | `internal/featureusage/exprwalk.go` — `CollectExprFeatures`; the request-level wrappers `collectPlanExprFeatures` / `recordPlanExprFeatures` in `internal/proxy/dql/feature_usage_hooks.go` |
| Per-subrequest counters | `internal/featureusage/tally.go` — `Tally`; bucket helpers in `internal/featureusage/counters.go` |
| Execution features | segcore: `internal/core/src/common/FeatureBits.h` (`FeatureBit`, `FeatureRecorder`), `query/PlanNode.h` / `PlanProto.cpp` (the plan option), `exec/expression/Expr.h` (`RecordExecPath`, `RecordScalarIndexType`), `Expr.cpp` (binding in `CompileExpression` and the GIS split), `UnaryExpr.cpp` (NGRAM), `ExprCacheHelper.h` / `Expr.h` / `FilterBitsNode.cpp` (cache hits), `SearchOnGrowing.cpp` / `ChunkedSegmentSealedImpl.cpp` / `VectorSearchNode.cpp` (interim index), `StrictGroupFilteredSearch.cpp` / `SearchGroupByNode.cpp` (strict grouping), `segcore/plan_c.*` (`GetSearchPlanFeatureBits`, `GetRetrievePlanFeatureBits`); Go: `internal/util/segcore/plan.go` (`FeatureBits`), `internal/querynodev2/tasks/search_task.go`, `search_task_go_reduce.go` (`attributeStorageCost`), `query_task.go`, `internal/querynodev2/segments/result.go` (`orFeatureBits`, `ColdReadFeatureBit`), `segments/query_pipeline.go`, `pkg/util/fastpb/query_insert.go`; bit layout `internal/featureusage/execbits.go` |
| Segment traits | `internal/featureusage/segment.go` — `ComputeSegmentEntries`; `internal/datacoord/feature_usage.go` — `FeatureUsageEntries` |
| Import and compaction counters | `internal/datacoord/feature_usage.go` — `recordImportFileTypes`, `recordCompactionType`; hooks in `internal/datacoord/services.go` (`ImportV2`) and `internal/datacoord/compaction_inspector.go` (`enqueueCompaction`) |
| QueryNode RPC, `config` group and execution counters | `pkg/proto/query_coord.proto` (`QueryNode.GetFeatureUsage`); `internal/querynodev2/services.go` — `GetFeatureUsage`, `queryNodeConfigEntries`; hooks in `delegator.go`, `segment_pruner.go` and `RunAnalyzer` |
| QueryNode fan-out and the `loaded` group | `internal/querycoordv2/feature_usage.go` — `CollectQueryNodeFeatureUsage`, `FeatureUsageEntries`; `internal/querycoordv2/session/cluster.go` — `Cluster.GetFeatureUsage`; `internal/featureusage/loaded.go` — `ComputeLoadedEntries`, `BoolConfigEntry` |
| Config | `pkg/util/paramtable/component_param.go` — `common.security.featureUsageEnabled`, `common.featureUsage.countersEnabled`; `configs/milvus.yaml` regenerated |
| Report surface guard | `internal/featureusage/surface_test.go` + `internal/featureusage/testdata/report_surface.golden` — `TestReportSurfaceIsStable`, regenerated with `-update-surface` |
| Hot-path benchmarks | `internal/proxy/dql/feature_usage_bench_test.go` — `BenchmarkFeatureUsageSearchFlush`, `…SearchUnits`, `…LegacyNormScore`, `…ParseSearchInfo`, `…ExprWalk`; C++ unit tests for the recorder in `internal/core/unittest/test_determine_use_index.cpp` |
| Integration suite | `tests/integration/featureusage/` — `helper_test.go` (suite, report accessors, the exact-delta assertion), `counters_test.go` and `extra_counters_test.go` (one request per Proxy counter), `expressions_test.go` (geospatial, timestamptz, struct array), `compaction_test.go` (compaction types, clustering, segment pruning), `groups_test.go` (static, loaded, config, provider, resource group, reachability, and the JSON, JSONLine and CSV imports), `import_files_test.go` (Parquet, NumPy), `shape_counters_test.go` (search parameter distributions, retrieval kinds, aggregation and ordering, delete and upsert modes), `exec_features_test.go` (execution features), `coverage_test.go` (the acceptance gate). `tests/integration/featureusageauth/auth_method_test.go` is the sibling suite for the two `auth_method` counters |
| Other tests | as in Test Plan |

## References

- `pkg/proto/proxy.proto:34` — `GetQuotaMetrics`, precedent for a purpose-specific RPC
- `internal/coordinator/mix_coord.go:859` — `GetMetrics` fan-out; `internal/coordinator/feature_usage.go` — `GetFeatureUsage`, which reaches the Proxies through `rootcoordServer.GetProxyClientManager()`
- `internal/http/verifier.go:72` — `RegisterPasswordVerifyFunc`; `internal/proxy/meta_cache.go:80` — registration
- `internal/http/router.go` — `/management/*` routes
- `pkg/common/common.go` — official property, type-param and index-param keys; `pkg/common/feature_usage_keys.go` — the allowlist the report names keys from
- `internal/metastore/model/collection.go`, `internal/metastore/model/index.go` — the metadata walked
- `internal/rootcoord/meta_table.go` — `FeatureUsageSnapshot`, `listCollectionFromCache` (the per-database scan it replaces)
- `internal/proxy/dql/search_util.go` — `parseSearchInfo`, `parseGroupByInfo`, `parseRankParams`; `internal/proxy/dql/util_dql.go:174` — `translateOutputFields`
- `internal/parser/planparserv2/plan_parser_v2.go:30` — `exprCache`
- `internal/util/function/rerank/function_score.go` — built-in rerank function names; `GetRerankName` returns a lowercased user string
- `internal/proxy/dql/rerank_meta.go:119` (`newRerankMetaFromLegacy`), reached through `selectHybridRerankMeta` (`internal/proxy/dql/function_chain_validator.go:54`, called at `internal/proxy/dql/task_search.go:626`) — legacy `rank_params` passed through unvalidated
- `pkg/metrics/proxy_metrics.go` — `proxyCollectionScopedMetrics`, `CleanupProxyCollectionMetrics`, and the leak history recorded above them; `internal/proxy/impl.go:275` — the `DropCollection` cleanup hook
- `internal/util/function/embedding/text_embedding_function.go` — provider switch
- `pymilvus/client/ts_utils.py` — `construct_guarantee_ts`; `pymilvus/client/prepare.py` — default `search_params`
- `configs/milvus.yaml:1387` — `quotaCenterCollectInterval`
