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
  hook (the pattern `CleanupProxyCollectionMetrics` already implements, `internal/proxy/impl.go:241`),
  alias handling, a time window, and a memory bound. That is a different design with a different cost
  model and, if wanted, gets its own MEP. This design deliberately keeps the request counters unkeyed so
  that none of that machinery is needed.
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
the search path takes decisions (two-stage search, segment pruning, brute-force search) that neither
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
  string bucket = 4;   // only for group = "dist"
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
| `field_types` | MixCoord | collections with at least one field of this `DataType` |
| `index_types` | MixCoord | collections with at least one index of this `index_type` (vector and scalar share the namespace, as in `model.Index`) |
| `metric_types` | MixCoord | collections with at least one index of this `metric_type` |
| `functions` | MixCoord | collections with at least one `FunctionSchema` of this `FunctionType` |
| `providers` | MixCoord | collections with at least one embedding or rerank function of this provider |
| `declared` | MixCoord | collections for which a hand-written predicate holds (e.g. `is_partition_key`) |
| `properties` | MixCoord | collections whose **collection-level** properties contain this key; boolean-valued keys are split, see below |
| `db_properties` | MixCoord | databases whose properties contain this key; same boolean split |
| `field_params` | MixCoord | collections with at least one field whose `type_params` contain this key |
| `index_params` | MixCoord | collections with at least one index whose user index params contain this key |
| `objects` | MixCoord | count of objects of this kind in the instance (databases, aliases, roles, grants, privilege groups) |
| `dist` | MixCoord | collections falling into `bucket` for this quantity |
| `segment` | MixCoord (DataCoord meta) | collections with at least one segment having this materialized trait |
| `loaded` | MixCoord (QueryCoord meta) | collections currently loaded that have this property (a partial field load, a replica outside the default resource group) |
| `config` | QueryNode | the node reports each boolean configuration item it exposes as one entry named `key=true` or `key=false`, with `value=1`. The entry names come from the paramtable key constants, so the group carries no operator string |
| `request` | Proxy, MixCoord (DataCoord), QueryNode | monotonic count of uses of this feature since `node_start_time`; `last_used_at` is the unix time of the most recent hit, `0` if never hit in this process. Each counter is tagged with the role that owns it and appears only in that role's response |

Rules that apply across groups:

- **Boolean-valued keys are reported per value.** `mmap.enabled=true` and `mmap.enabled=false` are two
  entries, named `mmap.enabled=true` and `mmap.enabled=false`. Reporting only "key was set" would count a
  collection that explicitly *disabled* auto-compaction as a user of auto-compaction, which inverts the
  question a deprecation decision asks. The value is drawn from `{true, false}`, so the sanitization rule
  is not affected.
- **Non-boolean values are never reported.** `collection.ttl.seconds=86400` contributes one to
  `properties/collection.ttl.seconds` and nothing else. Quantities worth knowing (replica number, shard
  number) go through `dist` with fixed buckets instead.
- **Keys that can be set at several levels are reported at each level.** `mmap.enabled` may appear in
  `properties`, `field_params` and `index_params`; the consumer decides whether to union them.
- **Only official keys are named.** A key is official if it is one of the constants in
  `pkg/common/common.go`. Any other key in `properties` / `db_properties` / `field_params` /
  `index_params` is folded into a single entry `_custom` in that group, reporting only the count. New
  official keys are added to `common.go` by construction, so the allowlist maintains itself.
- **`dist` buckets are fixed in code**, not derived from data:

  | name | buckets |
  |---|---|
  | `partition_count` | `1`, `2-16`, `17-64`, `65-1024`, `>1024` |
  | `shards_num` | `1`, `2`, `3-8`, `>8` |
  | `dim` (per vector field, max over the collection) | `<=128`, `129-512`, `513-1024`, `1025-2048`, `>2048` |
  | `max_length` (max over VarChar fields) | `<=256`, `257-4096`, `4097-65535`, `>65535` |
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
| Auth | HTTP Basic Auth as `root`, verified through the `passwordVerifyFunc` hook Proxy already registers (`internal/proxy/meta_cache.go:76`) |

The endpoint is registered on Proxy only. MixCoord's management port does not expose it: only Proxy can
verify a user password, and the `/management/*` routes carry no global authentication today. The 2.6
`/expr` endpoint (`common.security.exprEnabled`, root Basic Auth, Proxy-only) is the precedent for this
three-part gate.

### Configuration

| Key | Default | Meaning |
|---|---|---|
| `common.security.featureUsageEnabled` | `false` | registers the HTTP endpoint |
| `common.featureUsage.countersEnabled` | `true` | enables the request counters on every role that has them (Proxy request path, QueryNode search path, DataCoord task creation). Read once at component start. When `false` the branch is compiled in but never adds, and the counters read back as zero |

No metrics and no log lines are added.

## Design Details

### Architecture

```
consumer ──HTTP──▶ Proxy:9091 /management/feature_usage
                       │  root Basic Auth
                       ▼
                 mixcoord.CollectFeatureUsage()
                       │
      ┌────────────────┼──────────────────┐
      ▼                ▼                  ▼
 own metadata     proxyClientManager  querycoord cluster
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

Fan-out reuses what `GetMetrics` already uses: the Proxy list from `proxyClientManager`
(`internal/coordinator/mix_coord.go:66`) and the QueryNode list from the QueryCoord node manager and
cluster. Per-node timeout is the existing `GetMetrics` fan-out timeout; no new parameter. There is no
DataNode fan-out; the counters that would have lived there are kept in DataCoord, which is in the
MixCoord process and contributes them with its own static entries.

**There is no timer.** If nobody queries, nothing is computed. One query per day costs one computation
per day.

### Static statistics (MixCoord)

All inputs are in-memory metadata already held by the coordinator process. No etcd read, no cross-node
call.

| Data | Source |
|---|---|
| Collection schema, properties, partitions, aliases, databases, roles, grants, privilege groups | rootcoord `MetaTable` — `ListAllAvailCollections`, `ListDatabases`, `ListAliases`, `SelectRole`, `SelectGrant`, `ListPrivilegeGroups` |
| Index type, metric type, index params, `IsAutoIndex` | datacoord `indexMeta` — `model.Index{TypeParams, IndexParams, UserIndexParams, IsAutoIndex}` |
| Segment traits (phase 2) | datacoord `meta` — `SegmentInfo{storage_version, is_sorted, is_partition_key_sorted, textStatsLogs, jsonKeyStats, bm25statslogs}` |
| Build version, deploy mode | process-level values already used by `GetMetrics(system_info)` |

Four statistics mechanisms, in order of preference. The first three need no maintenance when a feature is
added; only the fourth does.

1. **Enum walk.** `field_types`, `functions`: iterate `schemapb.DataType` / `schemapb.FunctionType`,
   count collections per value. A new enum value is counted the day it lands.
2. **Open-value count.** `index_types`, `metric_types`, `providers`: count the values that actually
   occur in index meta and function params. These sets are validated on write (`indexparamcheck`, the
   provider `switch` in `text_embedding_function.go`), so only legal values reach the metadata and the
   output stays inside a code-defined set. A new index type from a Knowhere upgrade appears without any
   Milvus-side change.
3. **Open-key count.** `properties`, `db_properties`, `field_params`, `index_params`: count the keys that
   occur, name them if official, fold the rest into `_custom`.
4. **Hand-written predicates.** `declared`, `objects`, `dist`: one function per entry. This is the only
   list a human maintains; it is enumerated in the catalog and is deliberately short.

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
is set to the current unix second, the latter only if the stored second differs from the current one, so
under load each counter takes at most one timestamp store per second and the extra cache-line traffic
is negligible. `time.Now()` goes through the vDSO and costs tens of nanoseconds.

#### The key space is closed at compile time

**Invariant: the set of counter ids is a compile-time constant. No request field, parameter value,
collection, database, user, or time period may create a counter.** This is the property that makes the
memory footprint fixed for the life of the process and makes cleanup unnecessary, and it must be stated
because two natural counter definitions violate it:

- `rank_params.strategy` is a raw user string. On master the Proxy does not read `RankTypeKey` at all;
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

#### Where the counting hooks live

Every counted request feature is detected at a place that already parses it, so no second parse is
introduced:

| Feature class | Hook |
|---|---|
| `search_params` keys (`group_by_field`, `iterator`, `search_iter_v2`, `radius` / `range_filter`) | end of `parseSearchInfo` in `internal/proxy/search_util.go` (runs once per search and once per hybrid sub-request); the query iterator flag in `parseQueryParams` in `task_query.go` |
| Legacy `rank_params.strategy` | the legacy branch of `searchTask.PreExecute`, next to `newRerankMetaFromLegacy` |
| Request fields (`namespace`, `search_by_primary_keys`, `highlighter.type`, `function_score`, `not_return_all_meta`, `use_default_consistency` + `consistency_level`, `travel_timestamp`) | the start of `searchTask.PreExecute` and the end of `queryTask.PreExecute`, reading the proto fields |
| Expression features (`text_match`, `json_contains`, `st_*`, `is null`, `like`, ...) and `expr_template_values` / `expr_use_json_stats` | **after** the plan is built, by one walk over the plan's predicate tree at the three plan-creation sites (`tryGeneratePlan` for search, both query plan paths, delete) — see below |
| Import file type, compaction type (DataCoord) | where DataCoord accepts an import job (`ImportV2` in `datacoord/services.go`, after the duplicate-job check) and where it persists a compaction task (`enqueueCompaction` in `compaction_inspector.go`) |
| QueryNode execution decisions | `two_stage_search` where the delegator takes the two-stage branch (`delegator.search`), `segment_prune` where pruning removed at least one segment (`PruneSegments`), `brute_force_search` where a search reached segments with no index (`searchSegmentsAttempt`), `run_analyzer` at the QueryNode `RunAnalyzer` RPC |
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
| Count `ignore_growing`, `round_decimal`, `offset` on their **effective value** (`ignore_growing == true`, `offset > 0`), never on key presence | pymilvus sends `ignore_growing` and `round_decimal` in every `search_params` |
| Do **not** count `guarantee_timestamp` | pymilvus sets it on every search and query (`ts_utils.construct_guarantee_ts`: the cached write timestamp, `1`, or `0` for Strong); `SearchIterator` pins it as well. There is no request-side signal for "the user chose a timestamp" |
| `request_consistency_level` counts `use_default_consistency == false` and records the level as the entry name (`consistency_level=Strong`, ...) | It is the only field that distinguishes a per-request override. Clients that predate `use_default_consistency` leave it `false`; the catalog notes this bias |
| Do **not** count `reduce_stop_for_best` separately | pymilvus sets it only inside `QueryIterator`; the Proxy parses it only on the query path. It is the old iterator, which `iterator` already counts |
| `rank_params.strategy` is counted per **recognized** value (`strategy=rrf`, `strategy=weighted`, else `strategy=_other`); `function_score` is counted per **recognized** function type (`function_score=rrf`, `weighted`, `decay`, `model`, `boost`, else `function_score=_other`) | The two are distinct client paths (`RRFRanker`/`WeightedRanker` versus a `Function` object). Neither is deprecated. Counting "rrf" or "weights" on their own would double-count across the two paths. Both values are user strings at the counting point, hence the `_other` fold (see "The key space is closed at compile time") |
| `expr_template_values` counts only when the map contains a key other than `expr_use_json_stats` | The JSON-stats hint travels in the same map |
| `travel_timestamp` counts `> 0`, and is named `deprecated_travel_timestamp` | The Proxy no longer reads it for semantics; the counter measures how many clients still send a removed field, which is what the decision to drop the proto field needs |
| `highlighter` counts per `HighlightType` (`highlighter=Lexical`, `highlighter=Semantic`) | The two have different dependencies and adoption meaning |

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
| Embedding / rerank provider | no — validated by a `switch` on creation | emitted |
| Field type, function type, consistency level, highlight type | no — enums | emitted |
| Expression feature names | no — fixed by the counter table | emitted |

Nothing in the design reads `FunctionSchema.params` values except the provider name.

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

| Path | Cost |
|---|---|
| Request hot path, per counted feature | one branch + one `atomic.Add`; plus one atomic store of the timestamp at most once per second per counter |
| Request hot path, expressions | one walk of the parsed `planpb.Expr` |
| Resident memory per Proxy | number of counters × 16 bytes (`value` + `last_used_at`); on the order of one kilobyte, constant for the life of the process |
| `GetFeatureUsage` on a node | copy of a fixed-size counter array |
| Static statistics | one pass over collections + one over indexes; milliseconds at thousands of collections |
| Segment traits (phase 2) | one pass over segment meta; tens of milliseconds at tens of thousands of segments |
| Steady state with no queries | zero |

### Consumer contract

The expected consumer polls each instance on a fixed period (once a day for the cloud collector) and
stores the response in its own database. The instance keeps no history and makes no judgment; the
consumer does both. The rules below are what the consumer must implement for the data to mean what the
three motivating questions need it to mean.

**What to store.** Every entry, verbatim, with its node context:
`(instance, node_id, node_start_time, group, name, bucket, value, last_used_at, collected_at)`. Static
and dynamic entries are stored the same way.

**Static groups** (`field_types` … `segment`) are a complete recomputation on every read. Store them as
a snapshot and overwrite; do not difference them. Absence rules differ by group and matter for
"nobody uses this":

| Group kind | Entry with `value = 0` | Entry absent |
|---|---|---|
| enum walk (`field_types`, `functions`) | emitted — this enum value exists in the build and no collection uses it | the enum value does not exist in this build |
| open value / open key (`index_types`, `metric_types`, `providers`, `properties`, …) | never emitted | not present in any metadata |
| predicate (`declared`, `objects`, `dist`) | emitted | the predicate does not exist in this build |

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

**Counters that are structurally zero.** Three entries in the `request` group can never be non-zero in
this build, and a consumer must not read them as "nobody uses this":

| Entry | Why it is always zero |
|---|---|
| `compaction=_other` | the fold slot for an unrecognized `CompactionType`; nothing produces one |
| `compaction=PartitionKeySortCompaction` | no code path assigns this type. The seven places in DataCoord that construct a compaction task write only level-zero delete, mix, clustering, sort and schema-version bump, and a partition-key collection is sorted as a plain `SortCompaction` |
| `compaction=ClusteringPartitionKeySortCompaction` | the same |

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

Identifiers follow three rules, in order of preference:

1. **Code symbols** — proto field names, enum value names, parameter keys: `is_partition_key`,
   `group_by_field`, `HNSW`, `text_match`.
2. **Configuration keys** — the full key with dots: `mmap.enabled`, `collection.ttl.seconds`.
3. **Hand-named** — only where no symbol exists, lowercase with underscores. These are marked `(named)`
   below so reviewers can see the full set.

Entries whose source is enum walk, open-value or open-key are listed here for reviewers to see what the
output will contain; they need no product decision and no per-item code. Entries marked **decision** are
the ones that need a yes/no.

Verified against the 2.6 branch and master (go-api v3) at the time of writing; where the two differ the
mechanism handles both.

### Modeling (`field_types`, `declared`, `dist`)

| Entry | Group | Source | Note |
|---|---|---|---|
| every `schemapb.DataType` value | `field_types` | enum walk | 2.6: 23 types incl. `Geometry`, `Text`, `Timestamptz`, `SparseFloatVector`, `Int8Vector`, `ArrayOfVector`, `ArrayOfStruct`, `Struct`; master adds `Mol`, `Date`, `Time`, `Decimal`, `UUID` — no change needed |
| `is_partition_key` | `declared` | predicate | |
| `is_clustering_key` | `declared` | predicate | also the only declaration behind clustering compaction |
| `enable_dynamic_field` | `declared` | predicate | schema flag **or** `dynamicfield.enabled` property |
| `enable_namespace` | `declared` | predicate | schema flag **or** namespace property |
| `nullable` | `declared` | predicate | at least one nullable field |
| `default_value` | `declared` | predicate | at least one field with a default |
| `auto_id` | `declared` | predicate | primary key is auto-generated |
| `multi_vector_field` (named) | `declared` | predicate | more than one vector field |
| `struct_array_fields` | `declared` | predicate | `CollectionSchema.struct_array_fields` non-empty |
| `partition_count`, `shards_num`, `dim`, `max_length`, `max_capacity`, `replica_number` | `dist` | predicate | buckets fixed above |

### Index and search configuration (`index_types`, `metric_types`, `index_params`, `field_params`)

| Entry | Group | Source | Note |
|---|---|---|---|
| every `index_type` that occurs | `index_types` | open value | vector (`HNSW`, `IVF_*`, `DISKANN`, `SCANN`, `SPARSE_*`, `GPU_*`, RaBitQ, MinHash LSH, ...) and scalar (`INVERTED`, `BITMAP`, `HYBRID`, `NGRAM`, `RTREE`, `TRIE`, `STL_SORT`) in one namespace, `AUTOINDEX` included |
| every `metric_type` that occurs | `metric_types` | open value | 2.6 has 14 values incl. the `MAX_SIM_*` family; master adds `MAX_SIM_L2` |
| every official key in `UserIndexParams` | `index_params` | open key | includes `refine`, `refine_type`, `sq_type`, `nbits`, `drop_ratio_build`, `inverted_index_algo`, `hybrid_low_cardinality_index_type`, `bitmap_cardinality_limit`, `json_cast_type`, `json_path`, `json_cast_function`, `mmap.enabled`, `index.nonEncoding`, `indexoffsetcache.enabled`. Keys that only exist as Knowhere parameters are still counted: they are keys, not values. **Note:** with `AUTOINDEX`, `refine` / `sq_type` are chosen server-side and do not appear in user params |
| `is_auto_index` (named) | `declared` | predicate | `model.Index.IsAutoIndex` |
| every official key in field `type_params` | `field_params` | open key | `enable_analyzer`, `analyzer_params`, `multi_analyzer_params`, `enable_match`, `mmap.enabled`, `field.skipLoad`, `dim`, `max_length`, `max_capacity` |

Removed from the earlier draft after verification: `materialized_view_search_info` (no such key; the
user-facing switch is the `partitionkey.isolation` property), `EMB_LIST_META` (segcore-internal),
`rbq_bits_query` (a query-time parameter, never in index metadata).

### Functions (`functions`, `providers`)

| Entry | Group | Source | Note |
|---|---|---|---|
| every `FunctionType` | `functions` | enum walk | `BM25`, `TextEmbedding`, `Rerank` |
| every embedding provider that occurs | `providers` | open value | 12 on 2.6 (`openai`, `azure_openai`, `bedrock`, `dashscope`, `vertexai`, `voyageai`, `cohere`, `siliconflow`, `tei`, `zilliz`, `gemini`, `huggingface`), 13 on master |
| every rerank provider that occurs | `providers` | open value | `ali`, `cohere`, `huggingface`, `siliconflow`, `tei`, `vllm`, `voyageai`, `zilliz`; prefixed `rerank:` to keep the namespace separate |

Model names are user strings and are never emitted.

### Collection and database properties (`properties`, `db_properties`, `objects`)

All official keys are counted by the open-key mechanism; the table lists the ones reviewers asked about.

| Entry | Group | Note |
|---|---|---|
| `collection.ttl.seconds`, `collection.replica.number`, `collection.resource_groups`, `collection.autocompaction.enabled=true/false`, `cipher.enabled=true/false`, `replicate.id`, `mmap.enabled=true/false`, `warmup.*`, `indexoffsetcache.enabled=true/false`, `load_priority`, `field.skipLoad`, `partitionkey.isolation=true/false`, `index.nonEncoding=true/false`, `timezone`, `query_mode`, `allow_insert_auto_id=true/false` | `properties` | open key; 2.6 also has `lazyload.enabled` (deprecated, removed on master) |
| `collection.*Rate.*`, `collection.diskProtection.diskQuota.mb`, `partition.diskProtection.diskQuota.mb` | `properties` | open key. Quotas are tuning knobs, not features; they are reported because the mechanism reports every official key. **decision:** whether the consumer filters them out, not whether the kernel emits them |
| `database.replica.number`, `database.resource_groups`, `cipher.enabled=true/false`, `database.diskQuota.mb`, `database.max.collections`, `database.force.deny.*=true/false` | `db_properties` | open key. The deny flags are operational state, same remark as quotas |
| `consistency_level=<level>` | `declared` | enum; the collection default |
| `databases` (non-default), `aliases`, `custom_roles` (excluding built-in), `grants`, `privilege_groups` | `objects` | predicate; each is an object count. **decision:** these measure use of Milvus administration rather than a data feature; include or drop as a set |

### Segment traits (`segment`) — phase 2, **decision**

| Entry | Note |
|---|---|
| `storage_version=<n>` | collections with at least one segment on storage format version n; the direct measure of columnar-storage migration |
| `is_sorted`, `is_sorted_by_namespace`, `text_stats`, `json_key_stats`, `bm25_stats` (named after `SegmentInfo.is_sorted`, `is_sorted_by_namespace` — `is_partition_key_sorted` on 2.6 — `textStatsLogs`, `jsonKeyStats`, `bm25statslogs`) | collections with at least one live segment (not dropped, not invisible) carrying the trait |

These differ in kind from the rest: they report what was **materialized**, not what the user declared.
They require a pass over segment metadata, which is why they are a separate phase.

Import file types and compaction types are not coordinator metadata (the job records are
garbage-collected), so they are **request-group counters**: `import_file_type=<JSON|JSONLines|Numpy|Parquet|CSV>`
and `compaction=<CompactionType>` (unrecognized types fold to `compaction=_other`). They are counted in
**DataCoord**, where the job is created, not on the DataNode that executes it. On a deployment where
DataNodes are pooled across instances a per-DataNode count answers a different question than the report
asks, and counting at creation also keeps a retried task from counting twice. Counters carry a role tag,
and each role's RPC returns only its own slots, so a standalone process (one shared counter array) never
reports a slot twice.

### Loaded state (`loaded`, `dist`)

QueryCoord holds the only record of what is loaded right now, which is not derivable from the collection
metadata MixCoord already walks. It contributes four entries, all computed on demand from
`CollectionManager` and `ReplicaManager`:

| Entry | Group | Note |
|---|---|---|
| `collections` | `loaded` | how many collections are loaded |
| `load_fields_subset` | `loaded` | collections whose load request named fewer fields than the schema has. Load metadata written before `load_fields` existed is upgraded to the full list on recovery, so an empty list counts as "everything" |
| `custom_resource_groups` | `loaded` | collections with at least one replica outside `__default_resource_group`. Counted once per collection, not per replica. Resource group names are operator strings and are never emitted |
| `loaded_replica_number` | `dist` | the effective replica count per loaded collection, in the `replica_number` buckets. It differs from the declared `collection.replica.number` property, which is what the `dist` entry of the same name reports |

### QueryNode configuration (`config`)

`QueryNode.GetFeatureUsage` reports the twenty boolean `queryNode.*` configuration items that switch a
capability on or off, each as one entry named `key=true` or `key=false` with `value=1`: `enableDisk`,
`enableInterminSegmentIndex`, the two tiered-eviction switches, `multipleChunkedEnable`,
`enableGeometryCache`, `enableGISSplitFusion`, the four `mmap.*` switches plus `growingMmapEnabled` and
`mmap.jsonStats`, `exprResCacheEnabled`, `enableSegmentPrune`, `enableSegmentFilter`,
`skipGrowingSegmentBF`, `enableResultZeroCopy`, `preferFieldDataWhenIndexHasRawData` and `idfPreload`.

Only booleans are reported. A non-boolean configuration value can be an operator string (a path, a host,
a size expression), which would break the rule that every emitted string is drawn from a code-defined
set; a boolean has exactly two values, and the key is a constant in the paramtable.

### QueryNode execution path (`request`)

Four counters record decisions taken inside the node that neither the request nor the metadata shows:

| Entry | Signal |
|---|---|
| `two_stage_search` | the delegator took the two-stage search branch |
| `segment_prune` | segment pruning removed at least one sealed segment from a search or query |
| `brute_force_search` | a search reached at least one segment with no index on the search field, so it ran brute force |
| `run_analyzer` | the `RunAnalyzer` RPC, the one user-facing feature only a QueryNode serves |

### Request-level features (`request`) — **decision per row**

Each row is one counter and one hook; cost grows linearly with the number of rows. The recommended set
is a ceiling, not a target. The "signal" column states the detection rule where it is not simply "field
present". Every row marked "yes" is implemented on the implementation branch; see "Local Verification"
for which of them were also exercised end to end.

#### Search and query controls

| Entry | Signal | Recommend |
|---|---|---|
| `group_by_field` | key present | yes |
| `group_size` / `strict_group_size` | key present | yes |
| `rank_group_scorer` | key present | yes |
| `group_by_fields` (query path) | key present | yes |
| `iterator` | `iterator=true` **and** `search_iter_v2` absent | yes — old iterator protocol. pymilvus's v2 search iterator sends both keys, so without the exclusion every v2 page would also count as the old protocol |
| `search_iter_v2` | key present | yes — new protocol; keep separate to watch migration |
| `radius` / `range_filter` | key present | yes |
| `ignore_growing` | `== true` | yes |
| `hints` | key present | yes |
| `analyzer_name` | key present | yes |
| `search_by_primary_keys` | field `true` | yes |
| `namespace` | field set | yes |
| `output_dynamic_field` (named) | `translateOutputFields` resolved a dynamic field | yes |
| `not_return_all_meta` | field `true` | yes |
| `consistency_level=<level>` | `use_default_consistency == false` | yes — see bias note above |
| `deprecated_travel_timestamp` (named) | `travel_timestamp > 0` | yes — measures clients still sending a removed field |
| `partition_names` | non-empty | no — broadly used, no information |
| `offset` / `topk` / `limit` / `round_decimal` | — | no — basic paging |
| `guarantee_timestamp` | — | **no — no request-side signal** (see counting rules) |
| `reduce_stop_for_best` | — | no — subsumed by `iterator` |
| `recall_eval` | — | no — internal |

#### Expression features (one AST walk, all rows are set bits)

| Entry | Recommend |
|---|---|
| `text_match` | yes |
| `phrase_match` | yes |
| `random_sample` | yes |
| `json_contains` (incl. `_all`, `_any`) | yes |
| `json_identifier` (named; access to a JSON path) | yes |
| `array_contains` (incl. `_all`, `_any`) | yes |
| `array_length` | yes |
| `st_contains`, `st_within`, `st_intersects`, `st_crosses`, `st_overlaps`, `st_touches`, `st_equals`, `st_dwithin`, `st_isvalid` | yes — nine bits (one per `GISOp`); **decision:** report separately or collapse to `geospatial` on the consumer side |
| `timestamptz_compare` (named) | yes |
| `like` | yes |
| `exists` | yes |
| `is_null` / `is_not_null` (named) | yes |
| `regex_match`, `element_filter`, `struct_match` (named) | yes — added in implementation: the regex operator and the struct-array element filter / match predicates are distinct capabilities the walk sees for free |
| `expr_template_values` | yes — excluding the `expr_use_json_stats` key |
| `expr_use_json_stats` | yes — passed through `expr_template_values`; this is a request-level hint, not metadata |
| arithmetic, comparison, logical operators | no — basic syntax |

#### Hybrid search, rerank, highlight

| Entry | Signal | Recommend |
|---|---|---|
| `strategy=rrf`, `strategy=weighted`, `strategy=_other` | recognized `strategy` value in the legacy rank params (`RRFRanker` / `WeightedRanker`); anything else folds to `_other`; absent key counts nothing | yes |
| `norm_score` | key present in `rank_params` | yes |
| `function_score=rrf` / `weighted` / `decay` / `model` / `boost` / `_other` | recognized function name in `function_score`; anything else folds to `_other` | yes |
| `highlighter=Lexical`, `highlighter=Semantic` | `SearchRequest.highlighter.type` | yes — **decision:** Semantic maturity; if not GA, the consumer should label it |
| `fragment_size` / `num_of_fragments` | key present in highlighter params | optional |
| `sub_reqs` | — | no — Proxy-internal; use `milvus_proxy_req_count` |

A starting subset that covers the deprecation and migration questions currently open, at 13 counters:
`iterator`, `search_iter_v2`, `deprecated_travel_timestamp`, `consistency_level=*`, `group_by_field`,
`radius`, `search_by_primary_keys`, `namespace`, `not_return_all_meta`, `random_sample`, `phrase_match`,
`function_score=*`, `highlighter=*`.

## Test Plan

Three layers, each answering a different question: unit tests for "does this
compute the right thing", a checked-in surface file for "did the report change
under the consumer", and an integration suite for "does a real request move the
right counter".

### Unit tests

**Static statistics.** Tests construct an in-memory `MetaTable` and `indexMeta`
and assert the exact entry set: every `DataType`, every `FunctionType`, an index
of each type present, boolean properties at both values, a custom key folded
into `_custom`, `enable_dynamic_field` declared via the property only,
`enable_namespace` via the property only, each `dist` bucket boundary, and a
collection with no fields of a type contributing zero (the entry is still
present with `value=0` for enum-walk groups, absent for open-value groups).

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

| Test | What it pins |
|---|---|
| `TestSearchCounters` | 18 search-side counters one request at a time, including that a default-consistency search moves nothing, that `ignore_growing=false` is not counted, that iterator v2 excludes the old `iterator` counter, and that an unknown rerank function folds into `function_score=_other` |
| `TestQueryCounters` | the query-path hooks: the old iterator protocol, `group_by_fields`, `output_dynamic_field`, expression template values, an explicit consistency level |
| `TestExpressionCounters` | one query per expression kind, each asserting only its own counter moved |
| `TestExpressionCounterIsNotCached` | five queries with the same expression string count five, proving the counter sits on the parser's output rather than inside it |
| `TestUnknownRankStrategyFoldsToOther` | an unknown `strategy` value increments `_other` and creates no new slot: the key space stays closed |
| `TestStaticGroupsAndSanitization` | the declared, properties and dist groups on a freshly created collection, and that a sentinel used as the collection name, a field name, a property key and a property value appears nowhere in the serialized report |
| `TestLoadedGroup` | a collection loaded with a subset of its fields is counted as a partial load |
| `TestQueryNodeGroups` | every config entry is `key=true`/`key=false` with value 1 per node, a search over unflushed data moves `brute_force_search`, and `RunAnalyzer` moves its counter |
| `TestUnreachableNodeIsReported` | a QueryNode killed without deregistering is still listed, with `reachable=false` and an error |
| `TestImportFileTypesAreCounted` | one import job per file format moves that format's DataCoord counter |
| `TestProvidersAndCustomResourceGroup` | a text-embedding function reports its provider while its endpoint never appears, and a collection loaded into a named resource group is counted without the group's name leaving the node |

| `TestGeoAndTimeExpressions` | the nine geospatial predicates and the timestamp-with-timezone comparison, on a collection with a Geometry and a Timestamptz field |
| `TestStructArrayExpressions` | `element_filter` and `struct_match` on a struct array field |
| `TestMoreSearchCounters` | the remaining consistency levels, the null predicates, the regex operator and the JSON-stats hint |
| `TestRerankCounters` | both rerank client paths: the legacy `strategy` values with `norm_score`, and a `FunctionScore` naming each recognized reranker |
| `TestCompactionTypesAreCounted` | sort, level-zero delete and schema-version-bump compactions, each triggered by the user action that produces it |
| `TestClusteringCompactionAndSegmentPrune` | clustering compaction, and the QueryNode `segment_prune` counter it makes reachable |
| `TestBinaryImportFileTypesAreCounted` | the Parquet and Numpy import formats |
| `TestZZCoverage` | the acceptance gate, below |

The suite reads the report through `MixCoord.GetFeatureUsage`, the same call the
HTTP endpoint makes; the endpoint's gate and auth are covered by a unit test
(404 when disabled, 401 for a non-root user or a wrong password, 200 for root).

### The acceptance gate

The last thing the suite does is read the golden surface file, take the report,
and require that **every counter it lists has a non-zero value**. A counter that
no test drove fails the run by name. This is what makes the catalog verifiable
rather than aspirational: adding a row to the catalog without exercising it does
not compile past CI.

Three entries cannot be driven and are listed in the test with their reasons:

| Entry | Why |
|---|---|
| `compaction=_other` | the fold slot for an unrecognized `CompactionType`; no request can produce one |
| `compaction=PartitionKeySortCompaction` | no DataCoord path constructs this type. `CompactionTriggerType.GetCompactionType` emits only level-zero delete, mix, clustering, sort and schema-version bump, and a partition-key collection is sorted as a plain `SortCompaction` |
| `compaction=ClusteringPartitionKeySortCompaction` | the same: declared in the proto and handled defensively downstream, but nothing in this tree produces it |

The last two are worth acting on separately. They are catalog rows that can
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

Measured on the implementation branch, `-benchtime 500000x`, three runs, median:

| Benchmark | Result |
|---|---|
| `parseSearchInfo`, counters off | 19140 ns/op, 6725 B/op, 96 allocs/op |
| `parseSearchInfo`, counters on | 19452 ns/op, 6725 B/op, 96 allocs/op |
| expression walk, 1 term | 4.2 ns/op, 0 allocs |
| expression walk, 10 terms | 54.8 ns/op, 0 allocs |
| expression walk, 100 terms | 693 ns/op, 0 allocs |

The counters add about 1.6% to a search parameter parse that already costs 19
microseconds, and no allocation: the allocation counts are identical with the
counters on and off. The per-counter cost is above a bare atomic add because a
hit also stores `last_used_at`, which is bounded to one store per counter per
second. The expression walk is linear in the size of the predicate the request
already parsed, at roughly 7 ns per term, and allocates nothing.

## Rollout

1. Land P0 with `common.security.featureUsageEnabled=false`. Nothing changes for any deployment.
2. Enable on an internal cluster; compare the report against a `DescribeCollection` sweep.
3. Land P1 counters behind `common.featureUsage.countersEnabled=true`; confirm the `parseSearchInfo`
   benchmark delta is within noise before enabling by default.

## Delivery Phases

| Phase | Scope |
|---|---|
| P0 | Protos, `internal/featureusage/` static statistics and the counter array, MixCoord merge and Proxy fan-out, HTTP endpoint with gate and auth, and the Proxy hooks for the starter counters that are plain request fields or `search_params` keys (`group_by_field`, `iterator`, `search_iter_v2`, `radius`, `search_by_primary_keys`, `namespace`, `not_return_all_meta`, `deprecated_travel_timestamp`, `consistency_level=*`, `function_score=*`, `highlighter=*`) |
| P1 | The expression AST walk (`random_sample`, `phrase_match`, `text_match`, `json_contains`, geospatial, …) and any further rows the product selects |
| P2 | Segment traits from DataCoord meta; the import-file-type and compaction-type counters in DataCoord |
| P3 | QueryNode `GetFeatureUsage` RPC with the execution-path counters and the `config` group; QueryCoord fan-out to QueryNodes; the `loaded` group from QueryCoord metadata |

Adding a role or a group later changes nothing on the consumer side; the protocol is the same for every
node.

## Local Verification

Run on 2026-09-08 against a standalone instance built from the implementation branch
(`feat/feature-usage-report`, master `d6179da4f7`; woodpecker WAL with local storage; pymilvus 2.6.14rc1),
with `common.security.featureUsageEnabled=true`. To make the configuration-gated QueryNode paths reachable
on a small dataset the instance also set `queryNode.enableSegmentPrune=true`,
`autoIndex.twoStageSearch.enabled=true` with `minTopk=1` and `minNumSegments=1`, and
`dataCoord.segment.maxSize=4` with `dataCoord.compaction.clustering.preferSegmentSizeRatio=0.5` so
clustering compaction emits more than one segment. The workload created four collections and issued a
fixed set of requests, then read `GET /management/feature_usage` on the Proxy management port.

**Workload.**

- `fu_a`: auto-id primary key, VarChar partition key, nullable VarChar, Int32 array (`max_capacity=32`),
  8-dim float vector with `HNSW`/`COSINE` (`M=8, efConstruction=64`), dynamic field enabled, 16 partitions,
  properties `mmap.enabled=true`, `collection.ttl.seconds=86400` and a custom key `my.custom.key`.
- `fu_b`: two float vectors (`IVF_FLAT`/`L2` with `nlist=8`, `FLAT`/`IP`), a VarChar with
  `enable_analyzer`/`enable_match`, a sparse vector fed by a `BM25` function with `SPARSE_INVERTED_INDEX`,
  consistency level `Strong`, one alias.
- `fu_qn`: partition key, analyzer-enabled VarChar and an 8-dim vector, six flushed batches, loaded with
  `load_fields=[pk, tenant, vec]` so the load is a strict subset of the schema.
- `fu_clust`: Int64 clustering key and an 8-dim vector, 100k rows in 20 flushed batches, then a clustering
  compaction, which produced four result segments.
- Requests: 2 `group_by_field` searches; 1 range search (`radius`); 3 searches with explicit `Strong` plus
  1 with the default consistency; one v2 search iterator; one query iterator; one hybrid search with
  `RRFRanker`; one query per expression feature; one search with `filter_params`; one delete with a `like`
  filter. Then, against `fu_qn`: 3 searches over freshly inserted growing data, 4 filtered searches, 2
  filtered pure-ANN searches, one `run_analyzer`. Then, against `fu_clust`: 5 searches and 3 queries
  filtered on the clustering key.

**Gate and auth.** No credentials: `401`. Wrong root password: `401`. Root: `200`.

**Report** (non-zero entries; `build_version` and `deploy_mode=STANDALONE` present; three nodes, all `reachable=true`):

| node | group | entries |
|---|---|---|
| mixcoord | `declared` | `auto_id=3`, `consistency_level=Bounded=3`, `consistency_level=Strong=1`, `enable_dynamic_field=1`, `is_clustering_key=1`, `is_partition_key=2`, `multi_vector_field=1`, `nullable=1` |
| mixcoord | `field_types` | `Int64=4`, `VarChar=3`, `FloatVector=4`, `Array=1`, `SparseFloatVector=1` (23 other types present at 0; the dynamic `$meta` field is not counted as `JSON`) |
| mixcoord | `functions` | `BM25=1` |
| mixcoord | `index_types` / `metric_types` | `HNSW=3`, `IVF_FLAT=1`, `FLAT=1`, `SPARSE_INVERTED_INDEX=1` / `COSINE=1`, `L2=3`, `IP=1`, `BM25=1` |
| mixcoord | `index_params` | `M=3`, `efConstruction=3`, `nlist=1` (opened from the `params` JSON) |
| mixcoord | `field_params` | `dim=4`, `max_length=3`, `max_capacity=1`, `enable_analyzer=true=2`, `enable_match=true=1` |
| mixcoord | `properties` | `mmap.enabled=true=1`, `collection.ttl.seconds=1`, `_custom=1`, `timezone=4`, `namespace.sharding.enabled=false=4` |
| mixcoord | `dist` | `partition_count\|2-16=2`, `partition_count\|1=2`, `shards_num\|1=4`, `dim\|<=128=4`, `max_length\|257-4096=2`, `max_length\|<=256=1`, `max_capacity\|<=64=1`, `replica_number\|1=4`, `loaded_replica_number\|1=4` |
| mixcoord | `loaded` | `collections=4`, `load_fields_subset=1`, `custom_resource_groups=0` |
| mixcoord | `objects` | `aliases=1`, `grants=3` |
| mixcoord | `segment` | `storage_version=2=4`, `is_sorted=4`, `text_stats=1`, `json_key_stats=1`, `bm25_stats=1` |
| mixcoord | `request` | `compaction=SortCompaction=77`, `compaction=MixCompaction=24`, `compaction=ClusteringCompaction=1` (10 other counters present at 0) |
| proxy | `request` | `group_by_field=2`, `radius=1`, `search_iter_v2=4`, `iterator=6`, `output_dynamic_field=1`, `consistency_level=Strong=6`, `strategy=rrf=1`, `like=2`, `is_null=1`, `exists=1`, `json_identifier=3`, `array_contains=1`, `array_length=1`, `random_sample=1`, `text_match=1`, `phrase_match=1`, `expr_template_values=1` (44 other counters present at 0) |
| querynode | `config` | all 20 boolean switches, each at exactly one of `key=true` / `key=false`; the three the run enabled read back as enabled (`queryNode.enableSegmentPrune=true`, `queryNode.segcore.interimIndex.enableIndex=true`, `queryNode.enableSegmentFilter=true`) |
| querynode | `request` | `two_stage_search=16`, `brute_force_search=28`, `segment_prune=8`, `run_analyzer=1` |

Every request counter matches the workload: `iterator=6` is the query iterator's pages only (the v2 search
iterator's pages count under `search_iter_v2`), `like=2` is one query and one delete, `json_identifier=3`
is the two `$meta` queries plus the dynamic-field output query, and the default-consistency search moved
nothing. `function_score=*` stayed at 0 because `RRFRanker` uses the legacy `strategy` path.
`segment_prune=8` is exactly the 5 searches and 3 queries filtered on the clustering key.
`two_stage_search` and `brute_force_search` exceed the number of requests that named them because every
filtered pure-ANN search takes the two-stage branch and every search that reaches growing data runs brute
force; both are per-search-execution counts, which is what the counter is defined to report.

**Two configuration-gated paths.** `segment_prune` stays at 0 until three things hold: the collection has a
**clustering key** (a partition key does not reach the pruner), clustering compaction has produced more
than one segment, and the leader checker has pushed the resulting partition statistics to the delegator.
The first run of this workload recorded 0 for exactly that reason. `two_stage_search` is off by default
(`autoIndex.twoStageSearch.enabled=false`, `minTopk=2000`, `minNumSegments=5`). Both are worth knowing
when reading a report from a default-configured instance: a zero there means "not enabled", not "not used",
and the `config` group is what tells the two apart.

**Sanitization.** The serialized report contains none of: the four collection names, the alias name, the
custom property key or its value, the partition key values, the clustering key field name, or the inserted
text.

**The integration suite run.** The manual run above and `tests/integration/featureusage` verify different
things and both are kept. The manual run is the only place the HTTP endpoint, its gate and its Basic Auth
are exercised against a real Proxy management port; the suite reads the report over
`MixCoord.GetFeatureUsage` and is the one that proves every catalog row is reachable. The suite's latest
full run on the same branch:

| | |
|---|---|
| Test methods | 19, all passing |
| Wall clock | 155 s for the package, one cluster |
| Counters exercised | every entry in the surface file except the three listed under "The acceptance gate" |
| Groups present | all 16 |

Two entries in the report need state that outlives the test that created it, so those tests deliberately
leave it behind: the collection carrying a text-embedding function, because `providers` is an open-value
group that disappears when no collection has a function, and a database carrying a property, for
`db_properties`. Everything else is cleaned up.

**Observations for the consumer.** `grants=3` on an otherwise empty instance are the built-in role
policies. `timezone` and `namespace.sharding.enabled=false` are written by the server on every collection
and show `N/N`; they are official keys and are reported as designed, but they do not indicate a user
choice. `max_field_id` is server-managed and excluded. The compaction counts are inflated by the small
`dataCoord.segment.maxSize` this run used.

## Design Decisions

### D1. A dedicated RPC, not a new `metric_type` on `GetMetrics`

`GetMetrics` is a hot path: quotaCenter calls it on every Proxy, QueryNode and DataNode every
`quotaCenterCollectInterval` (3 s, `configs/milvus.yaml:1275`). Feature usage is queried once a day.
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
counters in the catalog (`strategy`, `function_score`) are named by user strings at the counting point
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

Static statistics walk enums and key sets. The one hand-maintained list is the predicate table for
`declared` / `objects` / `dist`, which is short and enumerated in this document. A registry of every
feature would drift within a release.

### D9. Custom keys folded, official keys named, allowlist is `common.go`

The allowlist must not be a second list to maintain. Using the constants file that every official key
is already added to makes it self-maintaining.

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

1. Which rows of the request catalog are in P1? The 13-counter subset above is the proposed start.
2. Geospatial predicates: eight counters or one?
3. Should the `objects` group (databases, aliases, roles, grants, privilege groups) be included at all?
4. Segment traits: P2 as proposed, or in P0 given the cost is tens of milliseconds?
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
| Static statistics | new `internal/featureusage/` — enum walk, open-value, open-key, predicate table, `dist` buckets, sanitization allowlist |
| Counter array (`value` + `last_used_at` per slot), the constant feature id set, `_other` folding | `internal/featureusage/counters.go` |
| Official key allowlist | `pkg/common/feature_usage_keys.go` — `IsOfficialFeatureKey`; `TestOfficialFeatureKeysCoverDottedConstants` keeps it in step with the constants file |
| Collection-side static entries | `internal/rootcoord/root_coord.go` — `Core.GetFeatureUsage`, `collectFeatureUsageInput` (rootcoord `MetaTable`) |
| Index-side static entries | `internal/datacoord/server.go` — `Server.FeatureUsageEntries`; `index_meta.go` — `indexMeta.ListAllIndexes` |
| MixCoord merge and Proxy fan-out | `internal/coordinator/feature_usage.go` — `GetFeatureUsage`, `collectProxyFeatureUsage` |
| Proxy RPC and HTTP handler | `internal/proxy/impl.go` — `GetFeatureUsage`; `internal/proxy/management.go` — `FeatureUsage` (route registered only when enabled); `internal/http/router.go` — `RouteFeatureUsage` |
| Proxy counter hooks | `internal/proxy/feature_usage_hooks.go`, called from `parseSearchInfo`, `searchTask.PreExecute`, `queryTask.PreExecute`, `parseQueryParams`; expression walk via `recordPlanExprFeatures` at the three plan-creation sites (`tryGeneratePlan`, query, delete) |
| Expression walk | `internal/featureusage/exprwalk.go` — `CollectExprFeatures`, `RecordExpr` |
| Segment traits | `internal/featureusage/segment.go` — `ComputeSegmentEntries`; `internal/datacoord/feature_usage.go` — `FeatureUsageEntries` |
| Import and compaction counters | `internal/datacoord/feature_usage.go` — `recordImportFileTypes`, `recordCompactionType`; hooks in `internal/datacoord/services.go` (`ImportV2`) and `internal/datacoord/compaction_inspector.go` (`enqueueCompaction`) |
| QueryNode RPC, `config` group and execution counters | `pkg/proto/query_coord.proto` (`QueryNode.GetFeatureUsage`); `internal/querynodev2/services.go` — `GetFeatureUsage`, `queryNodeConfigEntries`; hooks in `delegator.go`, `segment_pruner.go`, `segments/search.go` and `RunAnalyzer` |
| QueryNode fan-out and the `loaded` group | `internal/querycoordv2/feature_usage.go` — `CollectQueryNodeFeatureUsage`, `FeatureUsageEntries`; `internal/querycoordv2/session/cluster.go` — `Cluster.GetFeatureUsage`; `internal/featureusage/loaded.go` — `ComputeLoadedEntries`, `BoolConfigEntry` |
| Config | `pkg/util/paramtable/component_param.go` — `common.security.featureUsageEnabled`, `common.featureUsage.countersEnabled`; `configs/milvus.yaml` regenerated |
| Report surface guard | `internal/featureusage/surface_test.go` + `internal/featureusage/testdata/report_surface.golden` — `TestReportSurfaceIsStable`, regenerated with `-update-surface` |
| Hot-path benchmarks | `internal/proxy/feature_usage_bench_test.go` — `BenchmarkFeatureUsageParseSearchInfo`, `BenchmarkFeatureUsageExprWalk` |
| Integration suite | `tests/integration/featureusage/` — `helper_test.go` (suite, report accessors, the exact-delta assertion), `counters_test.go` and `extra_counters_test.go` (one request per Proxy counter), `expressions_test.go` (geospatial, timestamptz, struct array), `compaction_test.go` (compaction types, clustering, segment pruning), `groups_test.go` (static, loaded, config, provider, resource group, reachability), `import_files_test.go` (Parquet, Numpy), `coverage_test.go` (the acceptance gate) |
| Other tests | as in Test Plan |

## References

- `pkg/proto/proxy.proto:34` — `GetQuotaMetrics`, precedent for a purpose-specific RPC
- `internal/coordinator/mix_coord.go:66,734` — `proxyClientManager`, `GetMetrics` fan-out
- `internal/http/server.go:53` — `RegisterPasswordVerifyFunc`; `internal/proxy/meta_cache.go:76` — registration
- `internal/http/router.go` — `/management/*` routes
- `pkg/common/common.go` — official property, type-param and index-param keys
- `internal/metastore/model/collection.go`, `internal/metastore/model/index.go` — the metadata walked
- `internal/rootcoord/meta_table.go` — `ListAllAvailCollections`, `ListDatabases`, `ListAliases`, `SelectRole`, `SelectGrant`, `ListPrivilegeGroups`
- `internal/proxy/search_util.go` — `parseSearchInfo`, `parseGroupByInfo`, `parseRankParams`; `internal/proxy/util.go` — `translateOutputFields`
- `internal/parser/planparserv2/plan_parser_v2.go:30` — `exprCache`
- `internal/util/function/rerank/function_score.go` — built-in rerank function names; `GetRerankName` returns a lowercased user string
- `internal/proxy/rerank_meta.go:62`, `internal/proxy/task_search.go:506` — legacy `rank_params` passed through unvalidated
- `pkg/metrics/proxy_metrics.go` — `proxyCollectionScopedMetrics`, `CleanupProxyCollectionMetrics`, and the leak history recorded above them; `internal/proxy/impl.go:241` — the `DropCollection` cleanup hook
- `internal/util/function/embedding/text_embedding_function.go` — provider switch
- `pymilvus/client/ts_utils.py` — `construct_guarantee_ts`; `pymilvus/client/prepare.py` — default `search_params`
- `configs/milvus.yaml:1275` — `quotaCenterCollectInterval`
