# DQL Package

The `dql` package owns the proxy's **DQL (data query language)** task
implementations: search, query, and statistics, together with the search/query
pipelines that execute them. It was extracted from the proxy root package
(issue #44761) as part of the proxy task-package split.

The package keeps the search/query TASKS and their PIPELINE together: the
`searchTask`/`queryTask`/statistics tasks, the pipeline operators, the
highlighter, plan-size checks, rerank metadata, vector-type conversion, and the
util closure they share all live here. The only cross-group edge is
`dml -> dql`: the upsert requery (currently in the proxy root package, to be
extracted into `internal/proxy/dml`) builds a `QueryTask` and executes it
through `QueryRunner`. The graph stays acyclic.

## Overview

DQL requests arrive at the proxy's gRPC/REST handlers, which construct a task
via the exported constructors, enqueue it on the scheduler's `DqQueue`/`DdQueue`,
and wait for the result. Execution flows through the search/query pipelines
(`PreExecute -> Execute -> PostExecute`), which fan out to query nodes via the
shard client, reduce per-shard results, and reconstruct the final response.

### Tasks

- **`SearchTask`** — a search request (`*milvuspb.SearchRequest`). `NewSearchTask`
  wires the host node (`taskmodel.TaskNode`) and scheduler, derives
  `MetaCache`/LB policy/shard manager/channel manager from the node, and stores
  request-specific inputs only.
- **`QueryTask`** — a query-by-PK / expression request. `NewQueryTask` is also
  used by the upsert requery path (the accepted `dml -> dql` edge).
- **`GetStatisticsTask` / `GetCollectionStatisticsTask` /
  `GetPartitionStatisticsTask`** — collection/partition statistics.
- **`HighlightTask`** — the post-search lexical highlight task, enqueued on the
  scheduler's `DqQueue` by the highlighter operator.

### Pipelines

The search pipeline is a chain of operators built from the request and the
schema: query-plan generation, partition-key resolution, query execution,
reduce, rerank, highlight, and result organization. The requery operator
rebuilds a `QueryTask` from the search result IDs and runs it back through the
host node's `taskmodel.QueryRunner` (`Proxy.ExecuteQuery` in the root package).

## Responsibilities

1. **Task construction** — exported constructors that take only request-specific
   inputs; everything derived from the host node comes from the
   `taskmodel.TaskNode` contract and `paramtable`, so the root package never
   reaches into private task fields.
2. **Search/query execution** — plan building (`planparserv2`), placeholder
   conversion, output-field translation, aggregation, iteration, group-by and
   hybrid search, and multi-vector-field handling.
3. **Reduce & rerank** — per-shard result merging, top-k selection, group-by
   reduce, function-chain rerank metadata, and rank/score merging.
4. **Query-param constants** — the search/query `*Key` constants (in `keys.go`)
   consumed across the proxy.
5. **Shared util closure** — `util_dql.go` holds the dql-only helpers plus copies
   of small shared helpers (name/partition-tag validation, guarantee-ts parsing,
   namespace routing), so DQL does not depend on the root package.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                            dql                                │
│                                                               │
│   SearchTask ──► searchPipeline ──► operator chain             │
│      │              │  plan · partition · requery · reduce    │
│      │              │  rerank · highlight · organize          │
│      └──► highlighter ──► HighlightTask ──► sched.DqQueue     │
│                                                               │
│   QueryTask ──► queryPipeline ──► shard read ──► reduce        │
│      │                                                         │
│      └──► (requery from dml/upsert via QueryRunner)            │
│                                                               │
│   GetStatisticsTask / GetCollection / GetPartitionStatistics  │
│   HighlightTask                                               │
└──────────────────────────────────────────────────────────────┘
```

### Key constructors

```go
func NewSearchTask(node taskmodel.TaskNode, sched *scheduler.TaskScheduler,
    ctx context.Context, request *milvuspb.SearchRequest,
    optimizedSearch bool, isRecallEvaluation bool,
    tr *timerecord.TimeRecorder) *SearchTask

func NewQueryTask(node taskmodel.TaskNode, ctx context.Context,
    request *milvuspb.QueryRequest, plan *planpb.PlanNode,
    retrieveReq *internalpb.RetrieveRequest, cache metacache.Cache) *QueryTask

func NewGetStatisticsTask(node taskmodel.TaskNode, ctx context.Context,
    request *milvuspb.GetStatisticsRequest, tr *timerecord.TimeRecorder) *GetStatisticsTask

func ConvertHybridSearchToSearch(req *milvuspb.HybridSearchRequest) *milvuspb.SearchRequest
func PickFieldData(ids *schemapb.IDs, pkOffset map[any]int,
    fields []*schemapb.FieldData, schema *schemapb.CollectionSchema,
    collectionID int64) ([]*schemapb.FieldData, error)
```

## Host-node contract

Tasks hold a `taskmodel.TaskNode` (implemented by `*Proxy` in the root package)
and read everything they need through it: `GetMetaCache`, `LBPolicy`, `ShardMgr`,
`ChMgr`, and `TsoAllocator`. Requery execution goes through the separate
`taskmodel.QueryRunner` interface (`Proxy.ExecuteQuery`), whose concrete task
type is asserted inside the root implementation.

## Dependency rule

`dql` imports `taskmodel`, `scheduler`, `metacache`, `channelmgr`, `shardclient`,
`fieldvalidator`, `search_agg`, and `accesslog` (all leaf/earlier-extracted
proxy sub-packages), plus `internal/types`, `agg`, `planparserv2`, `reduce`,
`segcore`, the function-chain packages, and `pkg/v3`. It never imports the
`internal/proxy` root package — verified by `go list -deps` — so the edges
`root -> dql -> taskmodel/...` stay acyclic.

## Related components

- **taskmodel** (`internal/proxy/taskmodel/`): the `Task`/`DMLTask` contracts,
  `TaskNode`, `QueryRunner`, and the shared value types.
- **scheduler** (`internal/proxy/scheduler/`): the `DqQueue`/`DdQueue` the DQL
  tasks are enqueued on; the highlighter injects its `*TaskScheduler`.
- **shardclient / channelmgr / metacache** — shard-leader selection, channel
  management, and schema/metadata cache used during execution.
- **proxy root** (`internal/proxy/`): constructs the tasks, implements
  `TaskNode`/`QueryRunner`, and owns `tasks_alias.go` (the only place allowed to
  reference this package). The upsert requery (`task_upsert.go`) consumes
  `QueryTask` — the single `dml -> dql` edge, to be extracted with the DML
  package.
