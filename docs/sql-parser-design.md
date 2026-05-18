# Milvus SQL Parser Design

## Purpose

This change adds a SQL query entrypoint to Milvus so users can run scalar queries, JSON-field analytics, aggregation, ordering, and vector search with PostgreSQL-style `SELECT` syntax.

The SQL parser PR should contain the runtime feature and its correctness tests. JSONBench benchmark coverage is intentionally kept in a separate PR so reviewers can evaluate the core SQL execution path without benchmark-specific data-loading code.

The split is intentional:

- PR 1, this document: SQL syntax support, request routing, plan generation, runtime execution, and correctness coverage.
- PR 2, `docs/jsonbench-e2e-design.md`: a manually-run JSONBench E2E harness stacked on top of the parser PR.

This means the parser PR must be reviewable and testable without any external benchmark dataset. JSONBench queries influence the supported SQL surface, but dataset loading and performance reporting are not part of this PR.

## Scope

Included in this PR:

- Parse PostgreSQL-style `SELECT` SQL with `github.com/pganalyze/pg_query_go/v6`.
- Convert parsed SQL into a Milvus-specific intermediate representation.
- Build Milvus query plans for scalar, JSON, aggregate, sort, projection, and limit queries.
- Route vector-distance SQL to the existing search pipeline.
- Add REST v2 endpoint support: `POST /v2/vectordb/entities/sql_query`.
- Add unit, integration, and SQL query E2E coverage.

Out of scope for this PR:

- JSONBench 1M benchmark runner and benchmark report generation.
- JOIN, subquery, HAVING, window functions, CTE, UNION, and DML.
- Stateful PostgreSQL session behavior such as `SET ivfflat.probes = ...`.
- SQL transaction semantics, prepared statement lifecycle, cursor behavior, or PostgreSQL wire-protocol compatibility.
- Automatic SQL optimization beyond the explicit translation described in this document.

## Review Map

Reviewers can split the PR by responsibility:

| Area | Files | Review focus |
| --- | --- | --- |
| SQL parsing and IR | `internal/sqlparser/parser.go`, `extract.go`, `types.go`, `json_path.go` | Correct AST coverage, alias handling, JSON path extraction, unsupported syntax errors |
| WHERE translation | `internal/sqlparser/where_converter.go` | Milvus filter equivalence, literal quoting, boolean precedence |
| Plan generation | `internal/sqlparser/plan_builder.go`, `pkg/proto/plan.proto` | Aggregate/sort/project plan shape, schema resolution, generated proto compatibility |
| Proxy execution | `internal/proxy/task_sql_query.go`, `internal/proxy/impl.go` | Task lifecycle, reducer integration, search routing, error propagation |
| REST bridge | `internal/distributed/proxy/httpserver/*` | Request/response mapping, consistency level, HTTP response format |
| Segcore support | `internal/core/src/*` | Plan deserialization, aggregate/project execution, sort/project tests |
| Correctness tests | `internal/sqlparser/*_test.go`, `internal/proxy/task_sql_query_test.go`, `tests/e2e/sql_query_test.go`, `tests/integration/httpserver/sql_query_test.go` | SQL coverage and end-to-end behavior |

## Architecture

```text
Client
  |
  | POST /v2/vectordb/entities/sql_query
  v
HTTP v2 handler
  |
  | milvuspb.SqlQueryRequest
  v
Proxy.SqlQuery
  |
  | ExtractSqlComponents(sql)
  v
SqlComponents IR
  |
  +-- QueryTypeQuery --------------------+
  |                                      |
  | BuildQueryPlan                       |
  | sqlQueryTask                         |
  | QueryNode.Query                      |
  | Reducer.Reduce                       |
  | ApplyProjection                      |
  |                                      |
  +-- QueryTypeSearch / QueryTypeHybrid -+
                                         |
                                         | sqlComponentsToSearchRequest
                                         | Proxy.search
                                         | searchResultsToSqlResults
                                         v
                                  SqlQueryResults
```

The design keeps parsing, planning, and execution routing separate:

- `internal/sqlparser` owns SQL parsing and SQL-to-plan translation.
- `internal/proxy` owns task lifecycle, shard execution, reducer integration, and search routing.
- `internal/distributed/proxy/httpserver` owns REST request/response adaptation.
- `internal/core` and `pkg/proto/plan.proto` provide execution-plan support for aggregate, sort, and projection nodes.

## PG Parser Integration

`pg_query_go` is used only as the PostgreSQL syntax frontend. It converts SQL text into the PostgreSQL parse tree, and Milvus owns the supported semantic subset, IR extraction, planning, and execution.

```text
SQL string
  -> pg_query_go.Parse()
  -> PostgreSQL AST
  -> ExtractSqlComponents()
  -> Milvus SQL IR
  -> BuildQueryPlan() or sqlComponentsToSearchRequest()
  -> existing Query/Search pipeline
  -> SqlQueryResults
```

### Parser wrapper

`internal/sqlparser/parser.go` wraps `pg_query.Parse(sql)` behind `Parse` and `ParseOne`.

The wrapper gives Milvus one place to:

- reject empty SQL.
- reject multiple statements for the SQL query endpoint.
- normalize parser errors before they reach proxy code.
- isolate direct dependency on `github.com/pganalyze/pg_query_go/v6`.

The parser returns PostgreSQL AST node types such as `SelectStmt`, `A_Expr`, `FuncCall`, `ColumnRef`, and `SortBy`. These nodes prove that the input is syntactically valid PostgreSQL, but they do not mean Milvus supports executing the statement.

### AST to Milvus IR

Milvus does not let proxy or plan-building code consume arbitrary PostgreSQL AST directly. Instead, `internal/sqlparser/extract.go` extracts the supported subset into `SqlComponents`.

Examples:

| SQL fragment | Milvus IR result |
| --- | --- |
| `FROM users` | `Collection = "users"` |
| `data->'commit'->>'collection'` | `ColumnRef{FieldName: "data", NestedPath: ["commit", "collection"], IsText: true}` |
| `WHERE age > 30` | Milvus bool expression |
| `count(*)`, `sum(score)` | Aggregate descriptor |
| `embedding <-> $1` | `VectorSearchDef` with vector field, metric, topk, and placeholder |

This IR boundary keeps later stages independent from PostgreSQL parser internals. Planning code only handles Milvus-oriented structures and can reject unsupported SQL early.

### Supported subset gate

`pg_query_go` can parse much more PostgreSQL syntax than this feature supports. Milvus intentionally gates functionality during extraction and planning.

The current feature supports the SQL subset listed in this document. Unsupported constructs such as JOIN, subquery, HAVING, window functions, CTE, UNION, and DML should fail before execution even if the PostgreSQL parser accepts them.

This distinction is important:

- `pg_query_go` answers whether the SQL is syntactically valid PostgreSQL.
- `internal/sqlparser` answers whether the SQL belongs to Milvus's supported SQL subset.
- `internal/proxy` and `internal/core` answer whether the supported SQL can be planned and executed against the target collection schema.

### Execution routing

After IR extraction, Milvus chooses one of two execution paths:

- `QueryTypeQuery`: scalar, JSON, aggregate, sort, projection, and limit queries use `BuildQueryPlan`, `sqlQueryTask`, `QueryNode.Query`, reducer, and proxy-side projection.
- `QueryTypeSearch` / `QueryTypeHybrid`: vector-distance SQL is translated into `SearchRequest` and reuses `Proxy.search`; SQL `WHERE` becomes the search filter DSL.

This keeps SQL support as a translation layer over existing Milvus query and search behavior instead of introducing a separate SQL execution engine.

## API Contract

The new REST entrypoint is:

```text
POST /v2/vectordb/entities/sql_query
```

Request body:

```json
{
  "dbName": "default",
  "sql": "SELECT id, data->>'kind' AS kind FROM events WHERE id > {min_id} LIMIT 10",
  "params": {
    "min_id": 100
  },
  "searchParams": {
    "$1": "[0.1, 0.2, 0.3]",
    "nprobe": "16"
  },
  "consistencyLevel": "Bounded"
}
```

Field semantics:

| Field | Required | Meaning |
| --- | --- | --- |
| `sql` | Yes | PostgreSQL-style `SELECT` statement. The collection name comes from the `FROM` clause. |
| `dbName` | No | Target database. Empty value follows the existing proxy default behavior. |
| `params` | No | SQL template values used by filter expressions and forwarded as `ExprTemplateValues`. |
| `searchParams` | No | Search tuning parameters and vector placeholder values for vector SQL. Keys starting with `$`, such as `$1`, are query vector payloads. Other keys are forwarded to the search pipeline. |
| `consistencyLevel` | No | REST consistency string converted to the Milvus consistency enum. |

Response body uses the existing high-level REST wrapper and returns `milvuspb.SqlQueryResults` data:

```json
{
  "code": 0,
  "data": [
    {
      "id": 1,
      "kind": "commit"
    }
  ]
}
```

At the gRPC boundary the response contains:

- `status`: Milvus status.
- `column_names`: SQL output column order.
- `fields_data`: column-oriented field data.

The REST layer converts column-oriented `fields_data` into row-oriented JSON for high-level API users.

## SQL IR

`SqlComponents` is the boundary between parser details and Milvus planning:

```go
type SqlComponents struct {
    Collection   string
    SelectItems  []SelectItem
    Where        *WhereClause
    GroupBy      []GroupByItem
    OrderBy      []OrderItem
    Limit        int64
    VectorSearch *VectorSearchDef
    QueryType    QueryType
}
```

IR invariants:

- `Collection` must be populated from a single `FROM` collection.
- `Limit` is `-1` when the SQL does not specify `LIMIT`.
- `VectorSearch` is non-nil only when a vector distance expression is found.
- `QueryTypeSearch` means vector search without a scalar filter.
- `QueryTypeHybrid` means vector search with a scalar `WHERE` filter.
- `QueryTypeQuery` covers scalar retrieve, projection, aggregation, sort, and limit.

The parser normalizes SQL into the following select item types:

| Type | Meaning | Example |
| --- | --- | --- |
| `SelectColumn` | Field or JSON field reference | `id`, `data->>'city'` |
| `SelectAgg` | Aggregate function | `count(*)`, `sum(score)` |
| `SelectComputed` | Scalar or arithmetic expression | `extract(hour FROM ...)`, `max(ts) - min(ts)` |
| `SelectVectorDist` | Vector distance expression | `embedding <-> $1`, `l2_distance(embedding, $1)` |
| `SelectStar` | Wildcard projection | `SELECT *` |

JSON field access is represented as a `ColumnRef`:

```go
type ColumnRef struct {
    FieldName  string
    NestedPath []string
    IsText     bool
    CastType   string
}
```

For example, `data->'commit'->>'collection'` becomes:

```text
FieldName:  data
NestedPath: [commit, collection]
IsText:     true
MilvusExpr: data["commit"]["collection"]
```

The IR keeps PostgreSQL parser nodes only as debug or fallback references. The execution path uses explicit Milvus-oriented fields (`MilvusExpr`, aggregate descriptors, vector descriptors) so later stages do not need to understand PostgreSQL AST details.

## Supported SQL

### Projection

```sql
SELECT id, name FROM users LIMIT 10;
SELECT * FROM users LIMIT 5;
SELECT data->>'category' AS category FROM events;
```

### Filtering

```sql
WHERE age > 30 AND score <= 100
WHERE name IN ('a', 'b', 'c')
WHERE age BETWEEN 25 AND 35
WHERE name LIKE 'user_%'
WHERE data->>'kind' = 'commit'
```

The WHERE converter maps PostgreSQL operators to Milvus filter syntax:

| PostgreSQL | Milvus expression |
| --- | --- |
| `=` | `==` |
| `<>` | `!=` |
| `AND` / `OR` / `NOT` | `and` / `or` / `not` |
| `IN (...)` | `in [...]` |
| `LIKE` | `like` |
| `BETWEEN a AND b` | `>= a and <= b` |
| `data->>'key'` | `data["key"]` |

### Aggregation

```sql
SELECT count(*) FROM users;
SELECT sum(score), avg(score), min(age), max(age) FROM users;
SELECT count(DISTINCT data->>'did') FROM events;
SELECT data->>'category' AS category, count(*) AS cnt
FROM events
GROUP BY category
ORDER BY cnt DESC;
```

### Computed expressions

```sql
SELECT extract(hour FROM to_timestamp((data->>'time_us')::bigint / 1000000.0)) AS hour_of_day
FROM events;

SELECT (data->>'did')::text AS user_id,
       max(to_timestamp((data->>'time_us')::bigint / 1000000.0)) -
       min(to_timestamp((data->>'time_us')::bigint / 1000000.0)) AS activity_span
FROM events
GROUP BY user_id;
```

Computed expressions are planned in two phases:

1. Extract any nested aggregate inputs required by segcore.
2. Evaluate the final expression in proxy after shard results are reduced.

This is required for queries such as:

```sql
SELECT max(to_timestamp((data->>'time_us')::bigint / 1000000.0)) -
       min(to_timestamp((data->>'time_us')::bigint / 1000000.0)) AS activity_span
FROM events
GROUP BY data->>'did';
```

Segcore computes the aggregate inputs. Proxy evaluates the final arithmetic expression after reducer output is available.

### Vector search

The parser supports both pgvector-style operators and compatibility functions:

```sql
SELECT id, embedding <-> $1 AS distance
FROM articles
ORDER BY distance
LIMIT 10;

SELECT id, l2_distance(embedding, $1) AS distance
FROM articles
WHERE category = 'tech'
ORDER BY distance
LIMIT 10;
```

| SQL syntax | Milvus metric |
| --- | --- |
| `<->`, `l2_distance(field, vector)` | `L2` |
| `<=>`, `cosine_distance(field, vector)` | `COSINE` |
| `<#>`, `ip_distance(field, vector)` | `IP` |
| `<~>`, `hamming_distance(field, vector)` | `HAMMING` |
| `<%>`, `jaccard_distance(field, vector)` | `JACCARD` |

Vector parameters are passed through `SqlQueryRequest.SearchParams`:

```json
{
  "sql": "SELECT id, embedding <-> $1 AS distance FROM articles ORDER BY distance LIMIT 10",
  "searchParams": {
    "$1": "[0.1, 0.2, 0.3]",
    "nprobe": "16"
  }
}
```

Vector literals are also supported for cases where the vector is embedded in SQL. The parser keeps the raw typed literal when needed and resolves the final placeholder group with the target vector field schema, so float, binary, sparse, and other vector field types can follow existing search placeholder rules.

## Query Planning

For scalar and aggregate SQL, `BuildQueryPlan` creates a `QueryPlanNode`:

```text
Scan
  -> Filter
  -> Aggregate
  -> Sort
  -> Project
  -> Limit
```

The plan extensions added in `pkg/proto/plan.proto` include:

- `AggregateOp.count_distinct`
- JSON-aware `Aggregate.nested_path`
- `AggregateNode`
- `SortNode`
- `ProjectNode`
- Additional fields on `QueryPlanNode` for aggregate, sort, and project metadata

The generated protobuf file `pkg/proto/planpb/plan.pb.go` is updated accordingly.

Planner responsibilities:

- Resolve selected fields against collection schema.
- Translate JSON column references into nested path metadata.
- Build aggregate descriptors for `count`, `sum`, `avg`, `min`, `max`, and `count(DISTINCT ...)`.
- Preserve output aliases for response column names.
- Distinguish sort-before-projection from sort-after-projection when `ORDER BY` references computed output.
- Attach limit metadata so execution can bound returned rows.

The planner deliberately does not introduce cost-based rewrites. It emits the direct plan implied by the SQL syntax and relies on existing Milvus query/search execution components.

## Execution

### Scalar and aggregate path

`Proxy.SqlQuery` creates a `sqlQueryTask` for `QueryTypeQuery`.

`PreExecute`:

- Validates the target collection.
- Loads schema metadata.
- Builds the query plan.
- Builds aggregation field mappings for reducer compatibility.
- Serializes the plan into `SerializedExprPlan`.
- Resolves consistency level, partitions, and timestamps.

`Execute`:

- Uses the existing load-balancing policy.
- Sends retrieve requests to QueryNode shards.
- Collects `RetrieveResults`.

`PostExecute`:

- Uses reducer to merge shard results.
- Applies computed projection.
- Applies post-projection sort when ordering depends on computed output.
- Returns `SqlQueryResults` with column names and field data.

### Vector path

`QueryTypeSearch` and `QueryTypeHybrid` are converted to `SearchRequest`.

- SQL `WHERE` is converted to the search filter DSL.
- SQL `LIMIT` becomes `topk`.
- SQL distance expression selects metric type.
- `$1`-style vector parameters are read from `SearchParams` and excluded from index tuning params.
- Non-placeholder `searchParams` keys such as `nprobe`, `ef`, `radius`, `range_filter`, and `offset` are forwarded to the search pipeline.
- The existing search pipeline executes the query.
- Search results are converted back to SQL tabular results.
- When the SQL gives the distance expression an alias, search scores are appended as that output column.

The search path is intentionally a translation layer over existing Milvus search behavior. It does not add a separate SQL-specific ANN executor.

## Validation And Error Handling

Validation happens in layers:

- Parser validation rejects unsupported statement kinds and unsupported SQL syntax.
- IR extraction rejects ambiguous or unsupported expressions before execution.
- Plan building validates schema-dependent references, aggregate arguments, and vector field compatibility.
- Proxy validation preserves existing collection, partition, consistency, and timestamp checks.
- Search conversion validates that vector placeholders such as `$1` exist in `searchParams`.

Errors are returned through `SqlQueryResults.status` for gRPC callers and through the high-level REST response wrapper for REST callers. The design keeps parse, planning, and execution errors distinct enough that tests can assert the failing layer where needed.

## Limitations

Current limitations are explicit and should not be treated as accidental gaps:

- Only a single collection in `FROM` is supported.
- Only `SELECT` is supported.
- `JOIN`, subquery, `HAVING`, window functions, CTE, `UNION`, and DML are not supported.
- PostgreSQL session settings and transaction state are not implemented.
- Vector SQL supports one query vector (`Nq = 1`) per request.
- SQL optimizer behavior is minimal; no cost-based rewrites are introduced.
- JSON field access is translated to Milvus JSON path semantics and does not attempt to reproduce every PostgreSQL JSON operator.

Unsupported syntax should fail early with a clear error rather than falling back to an incomplete execution.

## Correctness Coverage

The parser PR includes correctness tests at four levels:

| Level | Files | Coverage |
| --- | --- | --- |
| Unit | `internal/sqlparser/*_test.go` | IR representation, parser extraction, WHERE conversion, plan building, projection execution |
| Proxy task | `internal/proxy/task_sql_query_test.go` | task identity, enqueue behavior, request lifecycle helpers |
| REST integration | `tests/integration/httpserver/sql_query_test.go` | high-level REST endpoint, scalar queries, JSON paths, aggregate, sort, filters, vector search, invalid SQL |
| E2E | `tests/e2e/sql_query_test.go` | running Milvus SQL behavior through `/entities/sql_query` |

The JSONBench PR adds performance smoke coverage only. It should not be required to prove parser correctness.

## Code Layout

```text
internal/sqlparser/
  parser.go                 PG parser wrapper
  extract.go                AST -> SqlComponents extraction
  json_path.go              JSON path and type-cast extraction
  where_converter.go        WHERE AST -> Milvus expression
  plan_builder.go           SqlComponents -> PlanNode
  project_executor.go       Proxy-side computed projection
  *_test.go                 parser, extractor, planner, projection tests

internal/proxy/
  impl.go                   Proxy.SqlQuery entrypoint and routing
  task_sql_query.go         sqlQueryTask and search request conversion
  task_sql_query_test.go    proxy task tests

internal/distributed/proxy/httpserver/
  constant.go               sql_query REST action
  request_v2.go             SqlQueryReqV2 request model
  handler_v2.go             REST handler bridge

internal/core/
  exec/operator/*           aggregate/project execution support
  query/PlanProto.cpp       plan deserialization support
  unittest/test_sort_project.cpp

tests/e2e/sql_query_test.go
tests/integration/httpserver/sql_query_test.go
```

## PR Boundary

This SQL parser PR should include:

- SQL parser implementation.
- Proto and segcore plan support.
- Proxy and REST SQL query path.
- Unit tests.
- SQL query E2E and HTTP integration tests.

The JSONBench PR should be stacked on top of this PR and include only the benchmark E2E runner:

```text
tests/e2e/jsonbench_1m_test.go
```

Recommended stacked history:

```text
test: add JSONBench SQL query E2E
feat: add SQL query parser
origin/master
```

If parser review changes the SQL API or supported syntax, the JSONBench PR should be rebased and adjusted without moving benchmark-only code into the parser PR.

