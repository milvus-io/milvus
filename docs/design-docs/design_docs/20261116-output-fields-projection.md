# Output-Field Projection: Aliases and Built-in Virtual Columns

- **Created:** 2026-11-16
- **Author(s):** @czs007
- **Status:** Draft for discussion; not implemented
- **Component:** Parser / Proxy / QueryNode / SDK
- **Related Issues:** [#50923](https://github.com/milvus-io/milvus/issues/50923), [#55091](https://github.com/milvus-io/milvus/issues/55091)
- **Related Design:** [#55119](https://github.com/milvus-io/milvus/pull/55119) (Boost scoring in Query)
- **Source baseline:** Milvus `1a05c97196`
- **Released:** N/A

## Summary

Parse each `output_fields` item with the expression grammar instead of string
matching, and allow two things that are not possible today:

1. `<item> AS <alias>` — rename an output column.
2. Predefined virtual columns as output items: `$score`, `$distance`,
   `$seg_id`, `$seg_offset`, and in hybrid search `$score(x)`,
   `$distance(x)`, `$rank(x)`.

Each virtual column in `output_fields` is a projection item. The proxy assigns
it a request-local ID and records it in the query plan; segcore or the proxy
emits a column under that ID; the column then travels in `fields_data` like a
stored field.

This step supports **identity projections only**: an item is a stored field
or a predefined virtual column, optionally aliased. Arithmetic and function
expressions in `output_fields` are the work of #50923 and are added on the
same grammar rule, alias table, projection list, and segcore output points.

Both Query and Search are covered.

This document owns the grammar, the name-resolution scopes, and the virtual
column contract. #55119 (ranked Query) and #50923 (expression engine) consume
them.

## Terminology

Three terms, split by one property: whether the value comes from storage.

| Term | Meaning | Stored |
|---|---|---|
| Field | A user field declared in the collection schema | Yes |
| System field | RowID and Timestamp | Yes |
| Virtual column | A result column that does not come from storage; its value is produced while the request executes | No |

"Virtual" means only "not stored, produced at read time". A virtual column
is written in `output_fields` in one of two ways. These are two sources of the
same kind of column, not two categories:

| Written as | Examples | Defined by |
|---|---|---|
| A predefined name | `$score`, `$distance`, `$seg_id`, `$seg_offset`, `$rank(x)` | The system |
| An expression | `price * 0.8 AS d`; later `$distance * 2 AS d2` | The user, in the request |

Each element of `output_fields` is a projection item: a field, or a virtual
column in either form. This step implements predefined virtual columns only;
expressions are #50923.

## Motivation

- #55091 needs `$score` as an output column of Query. Today
  `translateOutputFields` (`internal/proxy/dql/util_dql.go:174`) matches each
  item against `*`, an aggregate regex, struct-array names, and schema field
  names, then falls back to a dynamic-field key. It has no `AS` and no virtual
  columns. #55091 depends on the virtual column and on grammar-based parsing
  of `output_fields`; it does not depend on aliases, because `post_filter` and
  `order_by` can reference `$score` directly.
- The similarity value of a Search hit has no single name and no single
  definition across clients. All of them read `SearchResultData.scores`:

  | Client | Name of the value | Source |
  |---|---|---|
  | pymilvus | `distance` | `search_result.py:66`, `:343` |
  | REST | `distance` | `httpserver/utils.go:4451` |
  | Java SDK | `score` | `SearchResultsWrapper.java:199` |
  | Node SDK | `score` | example output in the Basic Vector Search guide |
  | Go SDK | `Scores` | `client/milvusclient/results.go:42` |
  | Rust SDK | `score` | `src/v1/collection.rs:1163`; `get_scores()` in v2 |
  | C# SDK | `Scores`; key `"score"` in row form | `Milvus.Client.V2/Response/Dql/SingleResult.cs:37`, `:138` |

  The Basic Vector Search guide states "The similarity score is also termed
  the distance to the query vector". With a rerank or in hybrid search the
  value is the reranked or fused score, not a vector distance, and it is still
  returned under the name `distance` in pymilvus and REST. The raw distance is
  not retrievable in that case. `$score` and `$distance` give the two values
  separate names with fixed definitions.
- #50923 phase 0 requires `output_fields` to go through the parser and a binder
  before any expression in `output_fields` can exist.
- Users debugging data placement, duplicates across segments, or recall issues
  have no way to see which segment a returned row came from. The values exist
  inside QueryNode (`$seg_offset` is a column of every per-segment search
  result; the segment is known for every per-segment result) but are dropped
  before the response is built.

## Scope

In scope:

- Grammar entry rule for one `output_fields` item, with mandatory `AS` for
  aliases.
- Alias table and two name-resolution scopes.
- Four virtual columns, in Query and Search, and three per-sub-request
  columns in hybrid search: `$score(x)`, `$distance(x)`, `$rank(x)`.
- A projection list in the query plan, request-local IDs for virtual
  columns, and their output from segcore to the client.

Non-goals:

- Arithmetic / function expressions in `output_fields` (#50923).
- A typed-value expression executor in segcore (#50923). The segcore change
  in this step is limited to emitting predefined virtual columns.
- `post_filter`, `ranker` on Query, and exact global ordering by score (#55091 /
  #55119).
- Ordering or filtering by `$seg_id`, `$seg_offset`, `$distance`, or the
  per-sub-request columns.
- Aliases on dynamic-field keys (see "Names and scopes").
- Removing or renaming the existing built-in `distance` / `score` key of a
  Search hit. This step only adds columns.
- SQL statements, a SQL endpoint, or any statement-level grammar.
- GROUP BY / HAVING name resolution (#50923 phase 3).

## Public API

```python
# Query: where did these rows come from?
client.query(
    collection_name="docs",
    filter="category == 'tech'",
    output_fields=["id", "title AS t", "$seg_id", "$seg_offset AS off"],
    limit=10,
)
# -> [{"id": 1, "t": "...", "$seg_id": 4611, "off": 17}, ...]

# Search: raw distance next to the reranked score
client.search(
    collection_name="docs",
    data=[query_vector],
    anns_field="embedding",
    ranker=some_rerank_function,
    output_fields=["title", "$distance", "$score", "$seg_id"],
    limit=10,
)
# -> hit = {"id": 1, "distance": 0.93,
#           "entity": {"title": "...", "$distance": 0.71, "$score": 0.93,
#                      "$seg_id": 4611}}
```

Existing requests are unchanged: a bare field name is the identity projection
and produces the same output as today. The existing `distance` key of a Search
hit is unchanged.

Recommended usage: request virtual columns under their own names, without an
alias. A name starting with `$` cannot equal a stored field, an alias, or a
built-in result key of any client, and `$` is valid in JSON and dictionary
keys. `AS` exists for renaming stored fields now and for naming computed
columns (#50923) later.

One alias form has a defined extra effect in Search: `$distance AS distance`
makes the built-in similarity value of each hit the raw distance instead of
the final score (see "Built-in similarity value"). It is for users who want
the existing `distance` key to hold a vector distance when a rerank is
present.

## Grammar

`Plan.g4` gets a second entry rule. Each `output_fields` element is parsed
separately with it:

```antlr
selectItem
    : '*'
    | aggregateCall (AS alias=Identifier)?
    | expr (AS alias=Identifier)?
    ;
```

- **`AS` is mandatory when an alias is given.** `price discount` is a syntax
  error, not "`price` aliased as `discount`". This differs from the
  `expr (AS? Identifier)?` sketch in #50923. Reasons: (1) a missing operator
  (`price * discount`) would otherwise be accepted silently; (2) unknown names
  resolve to dynamic-field keys, so the second word never triggers "field not
  found"; (3) keywords added later would change the meaning of existing
  requests; (4) `output_fields` has no alias support today, so nothing breaks,
  and relaxing "required" to "optional" later is compatible while the reverse
  is not. PostgreSQL documents the same recommendation
  (<https://www.postgresql.org/docs/current/sql-select.html>, "Omitting the AS
  Key Word").
- **`AS` is a contextual keyword, not a reserved word.** The lexer defines
  keyword tokens before `Identifier`, so a reserved word can no longer be used
  as a field name or dynamic key anywhere. `AS` is added to the `fieldName`
  rule introduced by #53968, the same treatment as `iso`, `interval`, and the
  other contextual keywords. `a AS as` must parse.
- **Virtual columns are new tokens.** `Identifier` is `[a-zA-Z_][a-zA-Z_0-9]*`
  and does not include `$`; only `$meta` exists as a token today. Add one token
  per virtual column. The per-sub-request forms add a parenthesized argument
  after `$score`, `$distance`, or the new `$rank` token: an identifier (field
  name) or a non-negative integer (position). Schema field names cannot start with `$`
  (`internal/proxy/ddl/util_ddl.go:218`), so there is no collision with stored
  fields.
- In this step the visitor accepts only two shapes for `expr` inside
  `selectItem`: a field reference (including the existing dynamic, JSON-path,
  and struct forms) or a virtual column. Any other expression is rejected with
  "expressions in output fields are not supported yet".
- **Aggregate calls move into the grammar in this step.**
  Every `output_fields` item goes through the parser; there is no regex
  pre-match. Today `count`, `sum`, `avg`, `min`, `max` are recognized by
  `aggregationPattern` (`internal/agg/aggregate.go:25`,
  `(?i)^(sum|count|avg|min|max)\s*\(\s*([\w\*]*)\s*\)$`), called from
  `util_dql.go:221`, `task_query.go:333`, and `feature_usage_hooks.go:557`.
  `Plan.g4` has no generic call rule, only fixed forms such as
  `TEXTMATCH(...)`. Add an aggregate-call alternative whose argument is a
  field name or `*`; the function name is matched case-insensitively and is a
  contextual keyword, so fields named `count`, `sum`, … stay usable. The
  three call sites take the parsed form instead of calling the regex. Every
  string the regex accepts today must parse to the same (operator, argument)
  pair and keep the same output column name; this includes the empty argument
  (`count()`), which `[\w\*]*` matches.
- The parser caches the parse tree by expression text
  (`plan_parser_v2.go:99`). The cache key must include the entry-rule name so
  that a filter string and an identical `output_fields` string do not share an
  entry.

## Names and scopes

Two scopes, the model already written in #55119:

| Scope | Clauses | Visible names |
|---|---|---|
| Input | `filter`, Boost rule filters, the expression inside each `output_fields` item | Stored fields and dynamic-field keys. No aliases. |
| Output | `order_by`, `post_filter` (#55119) | Aliases first, then virtual columns, then stored fields |

Rules:

- Aliases defined in `output_fields` are not visible to other `output_fields`
  items.
- An alias may equal the name of a stored field that is not itself returned
  under that name. In the output scope the alias shadows the field; in the
  input scope the name still means the field.
- Final output column names must be unique. `["a AS x", "b AS x"]` and
  `["a", "b AS a"]` are errors. A dynamic key written explicitly in
  `output_fields` counts as an output name: with `c` not a schema field,
  `["title AS c", "c"]` is an error.
- An alias cannot start with `$`.
- A dynamic-field key cannot take an alias in this step. Both `color AS c`
  and `$meta["color"] AS c` are errors, including the case where the alias
  equals the key. A dynamic key is not a column of its own today: the server
  returns the `$meta` JSON column restricted to the requested keys
  (`internal/proxy/dql/util_dql.go:262`), and the client picks values out of
  it by key name, so a renamed key has nothing to match.
- A bare dynamic key never takes an alias, in this step or later. A name that
  is not a schema field resolves to a dynamic key, so a misspelled field in
  `colr AS c` would return a column of NULLs instead of an error. The form
  that will accept an alias is the explicit path, `$meta["color"] AS c`.
- The output name of the explicit form `$meta["color"]` is the key, `color`,
  the same as for the bare form. Today the proxy records the raw string
  `$meta["color"]` as the output name (`internal/proxy/dql/util_dql.go:293`)
  while the returned `$meta` JSON holds the key `color`; pymilvus looks the
  listed name up in the JSON and returns no value for it (reproduced, see
  Compatibility).
- When a request returns the whole dynamic field (`*` or `$meta`) and also
  contains an alias or a virtual column, a row's dynamic field can hold a key
  equal to one of those output names. The keys are per-row data, so the
  binder cannot detect this. The explicit output column takes precedence:
  before returning, the proxy removes from each row's `$meta` JSON every
  top-level key equal to an alias or a virtual-column output name of the
  request. Example: `["title AS c", "*"]` on a row whose dynamic field is
  `{"c": 1, "color": "red"}` returns `c` = the value of `title` and
  `color` = `"red"`; the dynamic key `c` is not returned. A request that
  needs both names a different alias. Requests without aliases and virtual
  columns skip this step, so their results are unchanged.
- In Search, the alias names `distance` and `score` are restricted. These are
  the names under which official clients already return the built-in
  similarity value (see Motivation). Allowed forms:

  | Item | Effect |
  |---|---|
  | `$distance AS distance` | Allowed. Selects the raw distance as the built-in similarity value; see "Built-in similarity value". |
  | `$score AS distance`, `$score AS score` | Allowed. Same value as the built-in one; no observable change. |
  | Any other item with alias `distance` or `score` (`title AS distance`, `$distance AS score`, …) | Error at the proxy |

  The check is by name, at the proxy, for every protocol. The restriction
  lasts as long as the built-in keys exist. An alias equal to the primary-key
  name needs no extra rule in Search: the primary key is always returned, so
  the uniqueness rule rejects it.
- Aliases are case-sensitive, like schema field names.
  `validateOrderByFieldsWithGroupBy` (`internal/proxy/dql/task_query.go:296`)
  lowercases names today and must stop doing so for alias lookup.
- `*` expands to declared collection fields only. It never includes a virtual
  column.
- Each alias is evaluated once per row. With identity projections this has no
  observable effect; it becomes a requirement when #50923 adds expressions.

### Why `filter` does not see aliases

The alternative is one request-wide alias table that every clause can use.
It is not adopted. The reasons differ for the two kinds of alias.

**An alias of a virtual column cannot be evaluated in `filter`.** `filter`
runs while segcore scans rows; `$score` and `$distance` do not exist yet. A
request-wide table would make `filter="s > 0"` legal for `$score AS s` and
then unable to execute. Filtering on such a value is the job of
`post_filter`, which runs after the value exists and does see aliases.

**An alias of a stored field could be substituted, but the name becomes
ambiguous.** For `title AS t`, the proxy could rewrite `filter="t == 'x'"` to
`title == 'x'`. Three cases then have no single correct reading:

1. *Dynamic keys.* A name in `filter` that is not a schema field is a
   dynamic-field key today. With `["title AS c"]`, `filter="c > 1"` would mean
   either the alias (the field `title`) or the dynamic key `c`. Either
   precedence returns rows the other reading would not, with no error.
2. *An alias equal to a stored field name.* The rules above allow it, and
   with expressions (#50923) `price * 0.8 AS price` is a natural request.
   `filter="price > 100"` must mean the stored price; that holds only if
   `filter` does not see the alias.
3. *Adding a field.* If aliases took precedence in `filter`, a field added
   later under the same name could not be filtered on in that request. If
   fields took precedence, adding the field would change what an existing
   request filters on, with no error.

With aliases invisible in `filter`, a name there always refers to stored data
and none of the three cases arises.

The restriction costs no expressiveness: every filter can be written with the
original field name. An alias is only the name of an output column.

Other systems. PostgreSQL has the same rule: "An output column's name can be
used to refer to the column's value in ORDER BY and GROUP BY clauses, but not
in the WHERE or HAVING clauses; there you must write out the expression
instead"
(<https://www.postgresql.org/docs/current/sql-select.html>). ClickHouse makes
aliases visible in every clause and needs a setting,
`prefer_column_name_to_alias`, to choose between an alias and a column of the
same name.

## Virtual columns

| Name | Type | Meaning |
|---|---|---|
| `$score` | Float | Final score of the row: after rerank / Boost when present |
| `$distance` | Float | Raw distance or similarity returned by vector search, before any rerank |
| `$seg_id` | Int64 | ID of the segment that held the returned row version |
| `$seg_offset` | Int64 | Offset of the row inside that segment |

Without a rerank, `$distance` equals `$score`.

The two names are separate concepts and are not interchangeable:

- **`$score`** is the value the result is ranked by. It exists for every
  Search. Its source depends on the request: the vector metric value when
  there is no rerank, the BM25 relevance value for full-text search, the
  rerank / fusion / Boost output when one is present.
- **`$distance`** is the value of a vector metric between the query vector and
  the row's vector (`L2`, `IP`, `COSINE`, `HAMMING`, `JACCARD`, and the other
  vector metrics). It exists only when the `anns_field` is searched with a
  vector metric.

Full-text search is a Search on a sparse vector field with metric `BM25`.
BM25 is a relevance score, not a vector metric, so `$distance` is an error
there and the value is available as `$score`. Retrieving the BM25 value when
a rerank has replaced `$score` is not covered by this step.

### Built-in similarity value

Every Search hit carries one built-in similarity value, taken by all clients
from `SearchResultData.scores` and shown as `distance` (pymilvus, REST),
`score` (Java, Node SDKs), or `Scores` (Go SDK).

- **Default.** The built-in value is `$score`. A request that does not contain
  `$distance AS distance` gets exactly today's response.
- **`$distance AS distance`.** The user explicitly selects the raw distance as
  the built-in value. The proxy writes the raw distance into
  `SearchResultData.scores` as the last step of response assembly, and also
  returns the `distance` column in `fields_data`. The result is the same for
  every client and every SDK version, because no client-side change is
  involved:

  | Client | Result |
  |---|---|
  | pymilvus | `hit["distance"]` and `hit["entity"]["distance"]` are both the raw distance |
  | REST | `row["distance"]` is the raw distance |
  | Java, Node, Go SDKs | The built-in `score` / `Scores` and the `distance` column are both the raw distance |

  Rows are still ordered by the final score. With a rerank, the built-in value
  is therefore not monotonic in row order. A user who also needs the final
  score adds `$score` to `output_fields`.
- The replacement changes the content of `scores` only for a non-iterator
  Search with a rerank. Without a rerank the two values are equal; iterator
  with function rerank is rejected (`task_search.go:1113`); in hybrid search
  `$distance` is an error.

Why replace `scores` on the server instead of changing SDKs: pymilvus copies
`scores` to the top-level `distance` key on the client. A client-side
replacement would make `hit["distance"]` depend on the SDK version for the
same request, with no error on old versions, and the server cannot detect the
SDK version.

Client code that depends on the order of `scores` (checked 2026-11-17):

| Client | Checked at | Sorting of results by score | Other dependence on order |
|---|---|---|---|
| pymilvus | `7036e78c` | None (`client/search_result.py`) | V1 search iterator derives range bounds from the first and last hit (`orm/iterator.py:643`, `:731`) |
| Java SDK | `fa1c32d3` | None (`SearchResultsWrapper.java`, `ConvertUtils.java`) | V1 search iterator, same logic (`orm/iterator/SearchIterator.java:298`, `:314`, `:440`); V2 uses the server's `last_bound` |
| Node SDK | `d3fc4c8` | None (`milvus/utils/Search.ts:640`) | Iterator uses the server's `last_bound` (`milvus/grpc/Data.ts:1003`) |
| Go SDK | baseline | None (`client/milvusclient/results.go:90`, `read.go:139`) | Iterator uses the server's `last_bound` (`iterator.go:105`) |
| Rust SDK | `90336e5` | None (`src/v1/query.rs:1552`) | V1 iterator uses the server's `last_bound` (`src/v1/iterator.rs:1493`); V2 iterator filters on the last score of a page (`src/v2/client/iterator.rs:570`, `:1368`) |
| C# SDK | `2c89871` | None (`SearchResp.cs`, `SingleResult.cs`) | Iterator uses the server's `last_bound` (`MilvusClientV2.Dql.cs:1163`) |

The client-side iterators (pymilvus V1, Java V1, Rust V2) are unaffected because `scores` is unchanged in iterator
requests (see above). The proxy computes the iterator `last_bound`
(`task_search.go:1736`) before the replacement step.

Availability. **A virtual column that has no value for the request is an error
at the proxy. It is never returned as 0 or NULL.** This matches #55119, which
requires a ranker for `post_filter`, rejects ranker with count / aggregation,
and leaves unranked Query unchanged (where `$score` in `output_fields` already
fails today). It is a different case from "a ranker is present and no rule
matched the row": there the score exists and is 0.

| | `$seg_id`, `$seg_offset` | `$score` | `$distance` |
|---|---|---|---|
| Query | yes | only with a ranker (#55091); error until then | error |
| Query with aggregation / count / GROUP BY | error | error | error |
| Search (single vector field, vector metric) | yes | yes | yes |
| Search (full-text, metric `BM25`) | yes | yes | error: BM25 is a score, not a distance |
| Hybrid search | yes | yes (fused score) | error without argument; `$distance(x)`, `$score(x)`, `$rank(x)` select a sub-request |
| Search aggregation | error | error | error |

Semantics to document for users:

- `$seg_id` and `$seg_offset` describe physical placement at execution
  time. They change after compaction, are not stable across requests, and are
  not a row identity.
- When several versions of one primary key exist, the reduce keeps one row;
  the virtual columns describe that row.
- In hybrid search a primary key can be returned by several sub-searches; they
  refer to the same row, so placement columns are unambiguous.

### Hybrid search: per-sub-request columns

In hybrid search the final `$score` is the output of the fusion function and
has no fixed relation to any vector distance. A distance belongs to one
(vector field, query vector) pair, and a hybrid search has one pair per
sub-request, so a bare `$distance` has no single meaning.

**How other systems expose per-sub-query values.** Checked 2026-11-17
against the raw documentation source or the source code of each project, not
against rendered-page summaries; links in References.

| System | Mechanism | How a sub-query is identified | Row not returned by that sub-query |
|---|---|---|---|
| Azure AI Search | Request parameter `debug` ("Enables a debugging tool"); response `@search.documentDebugInfo.vectors.subscores`: "The breakdown of subscores of the document prior to the chosen result set fusion/combination method such as RRF". Contains `text.searchScore` and, per vector field, `searchScore` and `vectorSimilarity` ("the canonical definition of similarity metric, not the 'distance' version") | Vector field name (map key) | Entry absent: `"vectors": [{}]` in the reference example |
| MongoDB `$rankFusion` | `scoreDetails: true`; `details[]` with `inputPipelineName`, `rank`, `weight`, `value` | User-given pipeline name | "Rank is `N/A`" |
| Elasticsearch RRF | `explain=true`; per-query line "rrf score: [..], for rank [2] in query at index [0] ... for matching query with score:" | Query index, or `_name` | A no-match detail: "rrf score: [0], result not found in query ..." (`RRFRankDoc.java:85`) |
| OpenSearch | `hybrid_score_explanation` response processor (2.19): "You can use it as a debugging tool"; nested explanation with the original score under "min_max normalization of:" under "arithmetic_mean combination of:" | Position in the explanation | No node: "a leg that did not match contributes nothing rather than a zero node" (`neural-search`, `FusedDocExplanations.java`) |
| Vespa | Rank features `distance(dimension,name)`, `closeness(dimension,name)`, `itemRawScore(label)`, `bm25(field)` | `dimension` is `field` or `label`; a label is set on the query item: "useful when having multiple vector fields" | The page lists a default per feature (`distance`: max double value; `closeness`: 0; `itemRawScore`: 0) and states "closeness() is calculated only based on the vectors matched with nearestNeighbor operator"; it does not spell out the unmatched-hit case |
| LanceDB | Reranker option `return_score="all"` keeps `_distance` and `_score` next to `_relevance_score`; default `"relevance"` drops them. The base, RRF, Cohere, and VoyageAI rerankers on `main` all call the keep-scores merge for `"all"` in hybrid. Source comment: "slower than just keeping relevance score but can be useful for debugging" (`rerankers/base.py`, `_merge_and_keep_scores`) | Fixed columns (one vector query, one full-text query) | NULL: the source appends `pa.array([None] * n)` for the missing column |
| Weaviate | `explainScore` string, one line per result set that contains the object: "Hybrid (Result Set %v) Document %v: original score %v, normalized score: %v" (relative score fusion) or "... contributed %v to the score" (ranked fusion) (`usecases/traverser/hybrid/hybrid_fusion.go`) | Result-set name inside the string | No line for that result set |
| Qdrant | Response carries the final score only; returning per-prefetch scores is an open feature request (qdrant#10282, opened 2026-08-20). Inside a formula query, prefetch scores are addressable by position: "`$score[0]`, `$score[1]`, ... you can reference specific prefetch with the index within the array of prefetches" | Position in the prefetch array | "If a point is missing from a prefetch's results, its score for that prefetch is `0.0`"; overridable with `defaults` |
| pgvector | Distance is an explicit expression over a column and a query vector (`embedding <-> '[3,1,2]' AS distance`), computed for every selected row | The expression itself | Always defined |

Observations:

1. No surveyed system has an unqualified distance in hybrid results. Each
   identifies the sub-query by field name (Azure, Vespa), user label (Vespa,
   MongoDB, Elasticsearch `_name`), or position (Qdrant, Elasticsearch).
2. Systems that carry the value from retrieval leave it absent (Azure,
   Weaviate), NULL (LanceDB), `N/A` (MongoDB), or at a default (Qdrant 0.0,
   Vespa) for rows the sub-query did not return. A default that is a valid
   score needs an override mechanism (Qdrant `defaults`); NULL does not.
   Four of the systems describe the feature as a debugging tool (Azure,
   OpenSearch, LanceDB) or expose it through `explain` (Elasticsearch). Only the expression form (pgvector)
   defines it for every row, by computing it.
3. Azure (`vectorSimilarity` vs `searchScore`), LanceDB (`_distance` vs
   full-text score vs `_relevance_score`), and Vespa (`distance` vs `bm25`)
   keep vector distance and score under separate names.

**Design.**

- **One rule.** Each sub-request of a hybrid search is itself a Search and has
  its own `$score` and `$distance` with the single-search definitions. An
  argument selects the sub-request:

  | Item | Meaning |
  |---|---|
  | `$score` | Final score after fusion (unchanged) |
  | `$score(x)` | The value sub-request `x` ranked its results by: vector metric value, or BM25 value for a full-text sub-request |
  | `$distance(x)` | Vector metric value of sub-request `x`; error if `x` is a full-text (`BM25`) sub-request |
  | `$distance` without argument | Error in hybrid search: "specify the sub-request" |

- **`$rank(x)`** (Int64, nullable): the 1-based position of the row in
  sub-request `x`'s result, NULL when the row is not in it. It is needed to
  reproduce a rank-based fusion on the client. Milvus RRF computes
  `weight / (k + rank)` per sub-request with a 1-based rank and sums over the
  sub-requests that returned the row
  (`internal/util/function/chain/operator_merge.go:708`); it does not read the
  sub-request scores. The rank cannot be derived from the returned rows,
  because rows that rank higher inside a sub-request may be absent from the
  final top-K. With the column: `$score = Σ weight_x / (k + $rank(x))` over
  the sub-requests where `$rank(x)` is not NULL. A weighted (score-based)
  fusion is `Σ weight_x · norm_x($score(x))` over the sub-requests that
  returned the row (`operator_merge.go`, `collectWeightedScores`). With
  `norm_score` enabled, `norm_x` depends on the metric (`getNormalizeFunc`):

  | Metric of sub-request `x` | `norm(s)` |
  |---|---|
  | `COSINE` | `(1 + s) / 2` |
  | `IP` | `0.5 + atan(s) / π` |
  | `BM25` | `2 · atan(s) / π` |
  | Other larger-is-better metrics (`MHJACCARD`, `MaxSim`, …) | `0.5 + atan(s) / π` |
  | Smaller-is-better metrics (`L2`, `HAMMING`, `JACCARD`, …) | `1 − 2 · atan(s) / π` |

  Without `norm_score`, scores are used as returned when all sub-requests
  have the same direction; when directions are mixed, only the
  smaller-is-better ones are converted with `1 − 2 · atan(s) / π`
  (`resolveMergeScoreBehavior`). A sub-request whose scores were rewritten on
  the QueryNode is treated as `IP`.
  Elasticsearch `explain` and MongoDB `scoreDetails` return the per-input
  rank for the same reason.
- **Argument.** `x` is the `anns_field` name of the sub-request, or its
  zero-based position in the request (`$distance(0)`). A field name used by
  more than one sub-request is an error; the position form covers that case.
  Field names cannot start with a digit, so the two forms do not collide. In a
  single-vector Search, `$distance(x)` is accepted when `x` is the request's
  `anns_field` or `0`, and equals `$distance`.
- **Rows not returned by sub-request `x`: NULL.** The columns are nullable
  Float. The value is the one the sub-search produced and the fusion consumed;
  NULL states that the row was not in that sub-request's result. This is
  row-level absence inside an existing column and is separate from the
  request-level rule above (a column with no value for the request kind is an
  error). An exact distance for every returned row is a different feature: an
  expression over the stored vector and a query vector, evaluated per row
  (#50923 phase 1).
- **Output name.** The canonical text, for example `$distance(dense)`. `AS`
  applies as usual. The aliases `distance` and `score` are not allowed on the
  argument forms.
- **Not in this step.** Ordering or filtering by these columns.

**Implementation (proxy only when sub-request scores are not rewritten).**
The hybrid pipelines already hold every sub-request's reduced result next to
the fused result (`collapsed` and `rank_result`, `search_pipeline.go:3416`).
The per-sub-request columns are produced in the **last step of the proxy
pipeline**, in the end operator, after requery and any reordering: for each
requested `(column, x)`, index sub-result `x` by (query index, primary key) —
and element index for element-level hybrid, as `hybridAssembleOperator` does
(`:1688`) — and look up each row of the final result. The key must include
the query index: with `nq > 1` the same primary key can appear under several
queries with different values. The lookup does not depend on row order, so
running it last needs no carry step through requery or `order_by`. The end
operator gets `collapsed` as an additional input.

The source value is the sub-result's `Scores` when
`searchScoreInfo.Rewritten` is false (`:176`); when a sub-request's scores
were rewritten on the QueryNode, `$distance(x)` is taken from the `$distance`
column carried by the worker, the same mechanism as single-vector Search.

These columns get request-local IDs like every other virtual column (see
"Protocol"). They are evaluated at the proxy and are not sent to QueryNodes.

## Design

One model for every virtual column. Each virtual column in `output_fields` is
a projection item with a request-local ID. The proxy assigns the ID, the query
plan carries it, and whichever component evaluates the item emits a column
under that ID. From there the column is handled like a stored field: it is
part of `fields_data`, it is reduced by position and field ID, and the proxy
selects and names it from the projection list.

### Proxy: binder

`translateOutputFields` becomes a binder:

1. Parse each item with `selectItem`.
2. Resolve the item in the input scope to a stored field (existing logic), a
   dynamic key (existing logic), or a virtual column.
3. Build the alias table: output name → projection item. Check uniqueness and
   the alias rules. The set of output names includes every dynamic key that
   the request names explicitly, bare or as `$meta["key"]`.
4. Check virtual-column availability against the request kind.
5. Assign a request-local ID to every virtual-column item, in the order the
   items appear in `output_fields`.
6. Produce three results:

   | Result | Example for `["$seg_id", "title", "$score"]` in a Search | Consumer |
   |---|---|---|
   | Stored-field IDs | `[101]` | Plan, as today |
   | Projection list | `-1000 → $seg_id` (QueryNode), `-1001 → $score` (proxy) | Plan; proxy end step |
   | Final output IDs and names, in user order | `[-1000, 101, -1001]` | Proxy output selection |

7. Resolve `order_by` names in the output scope. In this step an alias of a
   stored field resolves to that field; ordering by a virtual column is
   rejected unless another design (#55119 for `$score`) enables it.

Today a single list, `OutputFieldsId`, serves both as "what segcore reads" and
as "what the proxy keeps" (`task_query.go:1256`). The binder separates them.

The binder stays in the proxy. The alias table is request-scoped and is passed
to the visitor through `ParserVisitorArgs` when a later clause needs it.

### Protocol

**Projection list.** `planpb.PlanNode` gets a repeated projection item next to
`output_field_ids`. An item holds the output ID, which virtual column it is
(an enum; plus the sub-request index for the argument forms), and the result
type. The plan sent to QueryNodes contains the items evaluated there; items
evaluated at the proxy stay in the proxy. In hybrid search every sub-request
plan carries the same items, so a column has the same ID in every sub-result.

Where each virtual column is evaluated:

| Virtual column | Evaluated by |
|---|---|
| `$seg_id`, `$seg_offset` | segcore |
| `$distance` | segcore (captured before any score rewrite) |
| `$score` in Query with a ranker | segcore (#55119) |
| `$score` in Search | Proxy, last step, from the final `Scores` |
| `$score(x)`, `$distance(x)`, `$rank(x)` | Proxy, last step, from the sub-request results |

**Request-local IDs.** Stored fields keep their schema field ID. Virtual-column
items are numbered from -1000 downward in `output_fields` order: the first is
-1000, the second -1001. The number identifies an output item, not a kind of
virtual column: `["$seg_id AS a", "$seg_id AS b"]` yields two items with two
IDs. Components look an ID up in the projection list; none of them interprets
the numeric value. The IDs are not persisted.

Field ID ranges after this change:

| Range | Use |
|---|---|
| 100 and above | Schema fields |
| 0, 1 | System fields (RowID, Timestamp) |
| -1 | "No field" sentinel in several places (for example `internal/proxy/search_agg/types.go:9`) |
| -100 to -999 | segcore-internal hidden columns; -100 and -101 are in use (`internal/core/src/common/Consts.h:42`) |
| -1000 and below | Request-local IDs of projection items |

Positive IDs are not usable: schema field IDs grow as fields are added, and a
QueryNode may briefly hold a newer schema than the proxy that built the plan.
The start constant exists in one place, the binder; `Consts.h` gets a comment
stating the range split. PostgreSQL identifies SELECT output items the same
way, by a per-statement ordinal stored in the plan ("resno should always be
equal to the item's ordinal position", `src/include/nodes/primnodes.h`).

`common.IsSystemField` (`fieldID < 100`) is true for these IDs. Its three uses
on the request path act on schema fields or on the stored-field ID list
(`task_query.go:454`, `typeutil/schema.go:556`, `:567`), and no code on the
result path branches on the sign or range of `FieldData.FieldId`.

**Names.** Between components a column is identified by its ID; `FieldName` is
filled at the proxy from the projection list and the alias table.

**Messages.** No change to `milvus-proto`. Query uses
`RetrieveResults.fields_data`; Search uses `SearchResultData.fields_data`.

### segcore

segcore reads the projection list from the plan and appends the item IDs to
its output list. A lookup function answers "is this output ID a projection
item, and which virtual column". It is separate from `SystemProperty::IsSystem`,
which has about 45 call sites including load and storage paths and means "a
stored system column".

Every output point already has a branch for system fields that builds an
Int64 column from row offsets instead of reading a schema field. The virtual
column branch sits next to it: `$seg_offset` is the offset array itself,
`$seg_id` is `get_segment_id()` repeated.

| Output point | Location |
|---|---|
| Plan parsing: output IDs are looked up in the schema | `query/PlanProto.cpp:571` |
| Retrieve output assembly | `FillTargetEntry`, `segcore/SegmentInterface.cpp:657`-`:717` |
| Retrieve with ORDER BY | `FillOrderByResult`, `segcore/SegmentInterface.cpp:497` (`PhyProjectNode` already emits the offset column for ID -100, `exec/operator/ProjectNode.cpp:179`) |
| Empty results | `FillEmptyRetrieveFields` (`SegmentInterface.cpp:73`); `BuildEmptyRetrieveBatch` (`retrieve_result_export_c.cpp:108`) |
| Output size estimate | `get_field_avg_size` (`SegmentInterface.cpp:902`; it asserts a non-negative field ID today) |
| Arrow transport of a Retrieve result | `PartitionRetrieveResult` (`segcore/segment_c.cpp:1036`): keep projection-item columns in the protobuf header like system fields; the Arrow conversion looks field metadata up in the schema (`:1113`) |
| Fetch by (segment, offset) for the two-phase Query | `FillRetrieveFieldsOrdered` (`retrieve_result_export_c.cpp:193`): per-segment fetch (`:296`), merge (`:398`), batch schema (`:160`), size estimate (`:271`) |
| Search late materialization | `FillTargetEntry(plan, SearchResult&)` (`SegmentInterface.cpp:160`, called from `search_result_export_c.cpp:1301`) |

`$distance` needs one more change, because the per-segment score is rewritten
before it leaves segcore:

1. `PhyRescoresNode::GetOutput` (`exec/operator/RescoresNode.cpp:133`)
   multiplies or adds the boost score into `SearchResult::distances_` in place
   and then calls `sort_search_result` (`exec/operator/Utils.h:198`), which
   permutes `distances_`, `seg_offsets_`, and `element_indices_`. The raw
   distance is not kept.
2. `AsyncSearch` (`segcore/segment_c.cpp:691`) multiplies every distance by -1
   for metrics where smaller is better, after the plan has run. The exported
   `$score` column (`search_result_export_c.cpp:409`) has that sign.

`SearchResult` gets a second vector holding the distance as returned by the
vector search. `PhyRescoresNode` fills it before applying the boost;
`sort_search_result` permutes it with the other vectors; the sign change in
`AsyncSearch` does not touch it. The Arrow export writes it as a `$distance`
column in the metric's natural direction, only when the plan has a `$distance`
item.

### Query path

No QueryNode Go change for virtual columns. A segcore result contains the
projection-item columns in `fields_data` under their IDs, in all three
sub-paths (protobuf, Arrow transport, two-phase fetch), and the reduce
operators treat them like stored fields. On the Arrow path, Go derives the
Arrow column set as "field order minus the columns present in the header"
(`queryutil/arrow_materialize.go:114`), so columns that segcore keeps in the
header need no handling there.

Proxy changes:

| Place | Today | Change |
|---|---|---|
| Output selection (`queryutil/remap_op.go:96`, list built at `task_query.go:1256`) | Keeps only columns whose ID is in `filterSystemFields(OutputFieldsId)`; an ID missing from the result is skipped silently | Use the binder's final output ID list. A missing projection-item ID is an error; a missing stored-field ID keeps today's behavior |
| `complement_fields` (`query_pipeline.go:243`) | Fills `FieldName` and `Type` from the schema by field ID | For projection-item IDs, fill them from the projection list; apply aliases after this step |
| Dynamic-field key removal | None | Last step, after aliases are applied. Runs only when the request has an alias or a virtual column and returns the whole dynamic field; see below |

**Dynamic-field key removal.** Input: the `$meta` JSON column and the set of
alias and virtual-column output names. For each row, delete the top-level
keys in that set and re-serialize. A row is parsed only if its bytes contain
`"<name>"` for some name in the set or contain a backslash (a key can be
stored with JSON escapes); other rows are passed through unchanged. Search
runs the same function in the end operator.

### Search path

Current flow (QueryNode worker, `internal/querynodev2/tasks/`):

1. Each segment result is exported as an Arrow table with `$id`, `$score`,
   `$seg_offset` (`search_task.go:345`).
2. The L0 function chain, if any, runs on the per-segment tables (`:357`).
3. `heapMergeReduce` merges across segments. The merged table keeps `$id` and
   `$score`; the origin of each row is kept in `Sources` as
   `(InputIdx, SegOffset, OriginalIdx)` (`heap_merge_reduce.go:54`). The L1
   chain runs after the merge (`search_task.go:403`).
4. `marshalReduceResult` writes `Ids` and `Scores` to `SearchResultData`;
   `Sources` is not exported (`marshal_reduce_result.go:35`).
5. `lateMaterializeOutputFields` uses `Sources` to read output fields from
   segcore into `FieldsData` (`search_task_go_reduce.go:444`).

Delegator and proxy reduce copy `FieldsData` column by column without looking
at the schema (`segments/search_reduce.go:42`,
`proxy/dql/search_reduce_util.go`).

Changes:

- **Worker.** `$seg_id` and `$seg_offset` come back from step 5 like stored
  output fields. `$distance` is a column of the per-segment Arrow tables from
  step 1; function chains do not write it; after the merge the worker gathers
  it with `Sources.(InputIdx, OriginalIdx)` and appends it to `FieldsData`
  under its ID. `round_decimal` is applied to it as to `Scores`.
- **Delegator / proxy reduce.** No change; the columns travel with the row.
- **Proxy rerank and hybrid assemble without requery.** No change.
  `PickFieldData` (`search_pipeline.go:1577`) and `hybridAssembleOperator`
  (`:1688`) re-select rows from the reduced `FieldsData` generically.
  `NewFieldDataIdxComputerWithSchema` treats a column whose field ID is not in
  the schema as a plain scalar column (`pkg/util/typeutil/schema.go:1078`).
- **Sign.** The proxy's sign restoration (`search_reduce_util.go:810`) acts on
  `Scores` only. `$distance` leaves segcore in its natural direction and is
  not restored.
- **Proxy requery pipelines.** Requery replaces `FieldsData` with the result of
  a second Query by primary key (`search_pipeline.go:1375`). Virtual columns
  must not come from that Query: the row may be in a different segment by
  then, and a Query has no distance. The Search plan carries the projection
  items even when stored output fields are left to the requery. Add a step
  that selects the projection-item columns from the reduced result in the
  final ID order and appends them to the requery fields. Element-level results
  key on (primary key, element index), as `hybridAssembleOperator` does.
- **Search with ORDER BY** (`searchWithOrderByPipe`, `search_pipeline.go:3619`).
  This pipeline always requeries and then reorders every `FieldsData` column
  generically (`reorderResults`, `:2573`). The carry step above goes before
  the `order_by` node.
- **Search iterator.** An iterator request is an ordinary Search whose
  `QueryInfo` carries iterator state; the proxy derives the continuation bound
  from the final result (`task_search.go:1736`). It uses the same worker path
  and pipelines. Iterator with function rerank is already rejected
  (`task_search.go:1113`), so `$distance` equals `$score` there.
- **Search aggregation.** The aggregation pipeline does not run the end
  operator and serializes hit fields through a field-ID-to-schema-name map
  (`search_pipeline.go:914`). Virtual columns are rejected for aggregation
  requests, consistent with the availability table.
- **Proxy end operator** (`search_pipeline.go:1862`). Today it names columns
  from the schema by field ID and drops every column not in the output list.
  It becomes the place where the projection list is completed:
  1. Evaluate the proxy-stage items: `$score` from the final `Scores`; the
     per-sub-request columns by lookup in the sub-request results.
  2. Check that every projection-item ID of the final output list is present;
     fail the request otherwise.
  3. Select and order columns by the final output ID list; fill names from
     the schema or the projection list; apply aliases.
  4. If the request has an alias or a virtual column and returns the whole
     dynamic field, remove the same-named keys from each row's `$meta` JSON
     ("Dynamic-field key removal" in "Query path").
  5. If the request contains `$distance AS distance`, overwrite `Scores` with
     that column. This runs after step 1 and after the iterator `last_bound`
     is computed.

  `SearchResultData.output_fields` must list the final output names; pymilvus
  treats any listed name missing from `fields_data` as a dynamic-field key.
- **Hybrid sub-requests with rewritten scores.** `$distance(x)` reads the
  `$distance` column of sub-result `x` when that sub-request's scores were
  rewritten on the QueryNode. The binder adds a `$distance` item to the plan
  for this purpose when the user did not request one; it is numbered after the
  user's items and is not part of the final output list.

`SearchResultData.distances` (field 10) exists and the proxy has readers for
it, but no writer was found in this repository. This design does not use it.

### Compatibility

- **Old requests.** Unchanged. Items without `AS` and without `$` resolve
  exactly as before.
- **Explicit `$meta["key"]` items.** The output name changes from the raw
  string to the key. Measured on 2026-11-17 against a local build of master with
  pymilvus 3.1.0rc100, collection with a dynamic field
  holding `color`:

  | `output_fields` | Server response (Query) | pymilvus row |
  |---|---|---|
  | `["color"]` | `output_fields=["color"]`, `$meta` = `{"color":"c1"}` | `{"id": 1, "color": "c1"}` |
  | `['$meta["color"]']` | `output_fields=['$meta["color"]']`, `$meta` = `{"color":"c1"}` | `{"id": 1}` |

  Search behaves the same (`entity` is `{}` for the second form). After the
  change both forms return `color`. A client that reads the raw `$meta`
  column itself sees the same JSON as before.
- **Mixed versions.** An old QueryNode ignores the projection list and
  returns no virtual columns. Two checks make the request fail instead of
  returning rows with a column missing: the reduce input validation requires
  every input to have the same columns in the same positions
  (`queryutil/helpers.go:118`), which catches a mix of old and new nodes; and
  the proxy's presence check on projection-item IDs catches the case where
  every node is old.
- **Old proxy, new SDK.** `$seg_id` is not a valid identifier for the old
  parser; the request fails with a parse error.
- **Server-returned output name list.** `QueryResults.output_fields` and
  `SearchResultData.output_fields` must list the final output names (aliases
  applied, virtual columns included). Search sets it from the user's raw list
  today (`internal/proxy/dql/task_search.go:1703`). pymilvus, the Java SDK, and
  the REST gateway all treat a listed name that has no matching column as a
  dynamic-field key.
- **SDKs and REST** (decoding of a column that is not in the collection
  schema):

  | Client | Checked at | Behavior | Change needed |
  |---|---|---|---|
  | pymilvus | `1988124c` (2026-11-11) | Query rows keyed by `FieldData.field_name`, decoded by `FieldData.type` (`grpc_handler.py:2428`); Search entities likewise (`search_result.py:79`). `output_fields` strings passed through. | None expected |
  | Java SDK | `fa1c32d3` (2026-11-06) | Search rows built by iterating the server's `output_fields` and matching `FieldData.fieldName` (`SearchResultsWrapper.java:222`); Query rows by field name. | None expected |
  | REST gateway | baseline | Rows keyed by `FieldData.FieldName`, typed by `FieldData.Type`; schema used only to find the primary-key name (`internal/distributed/proxy/httpserver/utils.go:4451`). | None expected |
  | Go SDK (`client/`) | baseline | Columns are built by name and type, but the *request* strings minus schema field names are treated as dynamic keys, and a returned column removes only an entry equal to its own name (`client/milvusclient/read.go:212`, `:271`). `"$seg_id"` works; `"title AS t"` leaves `title AS t` unresolved and fails with "extra output fields" when the result has no dynamic column. | Yes: resolve against the server's `output_fields` or parse the alias |
  | Node SDK | `d3fc4c8` (2026-11-17) | Rows are flat: built-in `score` first, then one key per name in the server's `results.output_fields` (falling back to the `fields_data` names when that list is empty); a name with no matching column is read from the dynamic column (`milvus/utils/Search.ts:624`, `:655`, `:673`). | None expected |
  | Rust SDK | `90336e5` (2026-11-17) | Columnar: each `FieldData` becomes a `FieldColumn` named by `field_name` and typed by `type`, with no schema lookup (`src/v1/data.rs:68`, `src/v1/query.rs:1537`). | None expected |
  | C# SDK | `2c89871` (2026-11-17) | Columns keyed by `FieldData.FieldName`; row form writes one key per output column, then `"score"` only "when no output field is literally named \"score\"" (`SingleResult.cs:129`-`:138`). | None expected |

- **REST Search rows are flat.** The gateway writes the primary key, `distance`,
  and every output column into one map; the built-in `distance` is written
  last and overwrites a user column of the same name. A stored field named
  `distance` has this collision today and is not changed by this design. For
  aliases the collision is closed by the rules in "Names and scopes": the
  aliases `distance` and `score` are allowed only on items whose value equals
  the built-in value after the `scores` replacement, so an overwrite never
  changes the value; the primary-key name is rejected by output-name
  uniqueness. The same holds for the two SDK row formats that are flat and
  write the built-in `score` before the output columns: Java V1
  `getRowRecords` (`SearchResultsWrapper.java:114`) and the Node SDK
  (`milvus/utils/Search.ts:655`). The C# row form resolves the same collision
  the other way: an output column named `score` is kept and the built-in
  value is not written (`SingleResult.cs:136`).
- **REST Query rows have no `distance` key.** Both Query handlers pass `nil`
  for `scores` (`handler_v2.go:1668`, `:1745`; `handler_v1.go:557`, `:637`),
  and the key is written only when `scores` is non-nil (`utils.go:4706`). The
  restriction on the aliases `distance` and `score` therefore applies to
  Search only.

## Relationship to other work

- **#50923.** This step is the access-layer part of phase 0 (grammar entry
  rule, binder, alias table) plus the structure expressions need: the
  projection list in the plan, request-local output IDs, and a branch for
  projection items at every segcore output point. An expression such as
  `price * 0.8 AS d` becomes one more kind of projection item; the binder
  assigns its ID the same way, and segcore evaluates the expression in the
  branch that today copies offsets. A request-level "evaluation stage" per
  item (segcore or proxy) already exists in this step; an expression takes the
  latest stage among its inputs. `$meta["key"] AS name` is such an expression:
  a path extraction over a stored field, emitted by segcore as a separate
  column of type JSON, NULL where the key is absent. `post_filter` without a ranker is not allowed
  by #55119; when expressions exist, a `post_filter` that depends only on
  stored fields can be rewritten by the binder into the `filter` conjunction.
- **#55091 / #55119.** `$score` as a Query output column and the scope table
  come from here. Boost does not need an alias: `post_filter="$score > 0.5"`
  and `order_by` on `$score` reference the virtual column directly, and
  `$score` is not visible in `filter` because the score does not exist when
  the filter runs. The `$score AS ...` examples in #55119 can be replaced by
  plain `$score`. `ranker`, `post_filter`, and exact global ordering stay in
  #55119. This document adds Search and three virtual columns
  that #55119 does not mention; #55119 excludes Search behavior changes.

## Implementation sequence

1. Grammar: `selectItem`, `AS`, aggregate calls, virtual-column tokens and
   argument forms, cache key. Parser tests including contextual-keyword cases.
2. Proxy binder and alias table; stored-field aliases end to end in Query and
   Search (no QueryNode change).
3. Projection list in the plan and request-local IDs; proxy output selection
   by the final output ID list with the presence check; Search `$score`
   (proxy only).
4. segcore: projection-item branch at the Retrieve output points; Query
   `$seg_id` / `$seg_offset`.
5. segcore: Search late materialization branch; Search `$seg_id` /
   `$seg_offset`; proxy requery carry step.
6. segcore: raw-distance vector and export; Search `$distance`;
   `$distance AS distance`.
7. Hybrid search `$score(x)`, `$distance(x)`, `$rank(x)`.

Steps 2–7 are independently shippable.

## Test plan

Design only; nothing below has been run.

- **Aggregates in the grammar.** Every form accepted by the current regex:
  each of the five functions, mixed case, spaces inside and around the
  parentheses, `count(*)`, empty argument; identical operator, argument, and
  output column name before and after; a field named `count` or `sum` as a
  plain output field and as a filter operand.
- **Grammar.** `a AS b`; `a b` rejected; field named `as`; `a AS as`; `AS` in
  string literals and JSON keys; each virtual column with and without alias;
  arithmetic in `output_fields` rejected with the dedicated message; filter
  and output item with identical text do not share a cached tree.
- **Scopes.** Alias not visible in `filter`; alias equal to a non-returned
  stored field; duplicate output names; alias starting with `$`; alias on a
  dynamic key rejected in both the bare and the `$meta[...]` form, with and
  without the alias equal to the key; `color` without an alias unchanged;
  `$meta["color"]` without an alias returns the key `color` in pymilvus, in
  Query and Search; an alias equal to an explicitly requested dynamic key
  rejected (`["title AS c", "c"]`, `["title AS c", '$meta["c"]']`); whole
  dynamic field with a same-named key (`["title AS c", "*"]`,
  `["title AS c", "$meta"]`, `["$seg_id", "*"]` with a row holding the key
  `$seg_id`): the explicit column is returned, the dynamic key is removed,
  other keys are kept, rows without the key are byte-identical, a key stored
  with JSON escapes is removed, a nested key of the same name is kept; the
  same request without alias and virtual column returns `$meta` unchanged;
  aliases
  `distance` / `score` in Search: the three allowed forms accepted, every
  other form rejected; both accepted in Query; alias equal to the primary-key
  name rejected in Search; case
  sensitivity; `order_by` on an alias of a stored field; `*` excludes virtual
  columns.
- **Projection list.** IDs follow `output_fields` order; the same virtual
  column under two aliases returns two columns; virtual columns before,
  between, and after stored fields keep the user's order; a result that lacks
  a projection-item column fails the request.
- **Query.** Each sub-path (protobuf, Arrow transport, two-phase); ORDER BY;
  pagination; iterator; growing and sealed segments; duplicate primary keys
  across segments; only-virtual-column output; aggregation rejected. Returned
  `$seg_id` / `$seg_offset` verified by reading the row back from that
  segment.
- **Search.** With and without requery; L0 / L1 / L2 rerank; group-by;
  element-level; hybrid (placement columns allowed, bare `$distance` rejected);
  `$distance` equals `distance` without rerank and differs with it; `nq > 1`.
  `$distance` with a segcore boost ranker and with an L0 / L1 function chain;
  positive value for `L2` / `HAMMING` / `JACCARD`; growing and sealed segments.
  `$distance AS distance` with rerank: `scores` holds the raw distance, row
  order is by final score, `$score` in the same request holds the final
  score; without the alias `scores` is byte-identical to today; iterator
  `last_bound` unchanged.
- **Hybrid per-sub-request columns.** Argument by field name and by position;
  field used by two sub-requests rejected by name and accepted by position;
  row returned by one sub-request only (NULL in the other's columns);
  `$distance(x)` on a `BM25` sub-request rejected and `$score(x)` accepted;
  RRF: `$score` equals `Σ weight / (k + $rank(x))` computed from the returned
  columns; `nq > 1` (rank and value per query); group-by; element-level;
  with and without requery; aliases `distance` / `score` rejected on the
  argument forms; single-vector Search with `$distance(anns_field)` and
  `$distance(0)`.
- **Compatibility.** Requests without the new syntax are byte-identical;
  mixed-version cluster fails closed; pymilvus row and hit decoding.

## References

- [#50923 Unified expression engine and projection operator](https://github.com/milvus-io/milvus/issues/50923)
- [#55091 Boost scoring, score filtering, and global ORDER BY in Query](https://github.com/milvus-io/milvus/issues/55091)
- [#55119 Design: Boost scoring and global ordering in Query](https://github.com/milvus-io/milvus/pull/55119)
- [#53968 Contextual keywords as field names](https://github.com/milvus-io/milvus/pull/53968)
- [#54282 Grep / QueryScan and computed-result transport](https://github.com/milvus-io/milvus/pull/54282)
- [Azure AI Search: hybrid search scoring, "Unpack a search score into subscores"](https://learn.microsoft.com/en-us/azure/search/hybrid-search-ranking)
- [Azure AI Search: Documents - Search Post (debug info example)](https://learn.microsoft.com/en-us/rest/api/searchservice/documents/search-post?view=rest-searchservice-2026-10-01)
- [MongoDB `$rankFusion`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/rankFusion/)
- [Elasticsearch reciprocal rank fusion](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/reciprocal-rank-fusion)
- [Vespa rank features](https://docs.vespa.ai/en/reference/rank-features.html)
- [LanceDB reranker base class](https://github.com/lancedb/lancedb/blob/main/python/python/lancedb/rerankers/base.py)
- [OpenSearch hybrid score explanation processor](https://github.com/opensearch-project/documentation-website/blob/main/_search-plugins/search-pipelines/explanation-processor.md)
- [Weaviate hybrid fusion source](https://github.com/weaviate/weaviate/blob/main/usecases/traverser/hybrid/hybrid_fusion.go)
- [Qdrant search relevance (formula variables)](https://qdrant.tech/documentation/search/search-relevance/)
- [Weaviate additional properties](https://docs.weaviate.io/weaviate/api/graphql/additional-properties)
- [Qdrant hybrid queries](https://qdrant.tech/documentation/search/hybrid-queries/), [qdrant#10282](https://github.com/qdrant/qdrant/issues/10282)
- [pgvector README](https://github.com/pgvector/pgvector/blob/master/README.md)
