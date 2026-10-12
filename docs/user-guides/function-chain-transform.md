# Run Function Chain transformations with PyUDF

This guide describes the experimental implementation in the matching Milvus,
milvus-proto and PyMilvus changes. It is not available merely by choosing an
existing release number. Use a server and SDK containing the same transformation
contract. The SDK checks server capability before sending a binding or stage.

For source builds and coordinated protocol/SDK revisions, see the
[dependency delivery guide](../dev/function_chain_delivery.md).

The [runtime schema extension](../design-docs/design_docs/20261012-function-chain-runtime-schema.md)
uses runtime 0.4.0 for complete Arrow schema transport. UDFs can inspect
`context.input_schema` and return `pyarrow.RecordBatch`/`Table` to preserve logical
type metadata. Separate function input/output schema declarations are unnecessary.

The complete deterministic example is
[PyMilvus `function_chain_transform_demo.py`](https://github.com/milvus-io/pymilvus/blob/master/examples/function_chain_transform_demo.py).
Its wheel source is
[`transform_demo`](../../internal/util/function/pyudf/python/examples/transform_demo/milvus_transform_demo.py).
The example performs INSERT chunking/embedding/tagging, query translation and
embedding, query-dependent L2 ranking, and a Struct-valued POST highlight. Vectors and translations are
deterministic; it calls no paid external model API. The source links for new
files become upstream links when the corresponding changes are merged.

## 1. Prepare the server and Python runtime

Upgrade every write Proxy and the coordinators before enabling collection-bound
ingestion. A new Proxy's capability check against a coordinator does not prove
that an older Proxy behind the same endpoint will execute the stored chain.
Do not activate ingestion bindings while old write Proxies can receive INSERTs.

PyUDF is disabled by default. Install the runtime and its dependencies into
the `python3` interpreter used by each Proxy, then enable it and restart:

```yaml
common:
  fileResource:
    mode:
      proxy: sync
function:
  pyUDF:
    enabled: true
    address: 127.0.0.1:19090
    rpcTimeout: 30s
    maxMessageBytes: 67108864
    server:
      workerCount: 1
```

For a local source checkout, an installed virtual environment works:

```bash
python3 -m venv .venv-pyudf
.venv-pyudf/bin/python -m pip install ./internal/util/function/pyudf/python
.venv-pyudf/bin/python -I -c 'import milvus_pyudf_runtime, pyarrow, grpc'
```

Start the patched server using its normal deployment/startup procedure with
that environment's `bin` directory on PATH. For a locally built binary:

```bash
PATH="$PWD/.venv-pyudf/bin:$PATH" ./bin/milvus run standalone
```

The supervisor launches `python3 -I -m milvus_pyudf_runtime.supervisor`; placing
source code on PYTHONPATH is insufficient. Runtime dependencies are pinned in
[`pyproject.toml`](../../internal/util/function/pyudf/python/pyproject.toml).
Install any additional model-client libraries into the worker environment too.
The worker endpoint has no authentication/TLS and executes trusted Python
wheels; keep it on loopback or an access-controlled network.

Install the matching PyMilvus source/wheel in the client environment. Merely
installing the Python worker runtime does not update the client SDK or server
protocol.

## 2. Build and register the example wheel

From the Milvus checkout:

```bash
.venv-pyudf/bin/python -m pip wheel --no-deps \
  ./internal/util/function/pyudf/python/examples/transform_demo \
  --wheel-dir ./dist/transform-demo
```

Upload `milvus_transform_demo-0.3.0-py3-none-any.whl` to the object storage
available to Milvus FileResource, using your storage client. Register its
storage object path, not a client-local filesystem path:

```python
from pymilvus import MilvusClient

client = MilvusClient("http://127.0.0.1:19530")
client.add_file_resource(
    name="transform_demo",
    path="udf/milvus_transform_demo-0.3.0-py3-none-any.whl",
)
```

`add_file_resource` registers an already uploaded object; it does not upload
the local wheel. Proxy sync mode makes the artifact available locally. Python
loads its factory through the `milvus.pyudf` entry point. A collection binding
retains resource references, so drop that collection before removing its
referenced resource. Use a new immutable object/resource for a changed wheel;
this feature adds no in-process wheel-replacement API.

With the matching PyMilvus checkout and a fresh collection name:

```bash
python examples/function_chain_transform_demo.py \
  --uri http://127.0.0.1:19530 \
  --resource-name transform_demo \
  --collection function_chain_transform_demo
```

Run this command from the PyMilvus repository. `--wheel-path <object-key>` can
register the object as part of the script instead; omit it when the resource
already exists. The script leaves its collection available for inspection.
Its deliberate UDF failure prints an expected error while the overall script
checks the result and exits successfully.

## 3. Bind ingestion and omit generated fields

An ingestion chain is stored in the collection schema. `PRE_INSERT` and
`INGESTION` name the same stage. This example reads an undeclared temporary
`document` input, then stores generated fields:

```python
from pymilvus import DataType, FunctionChain, FunctionChainStage
from pymilvus.function_chain import col, fn

ingest = (
    FunctionChain(FunctionChainStage.PRE_INSERT, "chunk_and_embed")
    .flat_map(
        "chunk",
        fn.py_udf(
            col("document", DataType.VARCHAR),
            resource_name="transform_demo",
        ),
        position="chunk_index",
    )
    .select(
        ["chunk.text", "chunk.vector", "chunk.tags"],
        ["text", "vector", "tags"],
    )
)

schema = client.create_schema(auto_id=True, enable_dynamic_field=False)
schema.add_field("id", DataType.INT64, is_primary=True)
schema.add_field("text", DataType.VARCHAR, max_length=256)
schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
schema.add_field(
    "tags", DataType.ARRAY,
    element_type=DataType.VARCHAR, max_capacity=16, max_length=32,
)
schema.add_function_chain(ingest)
client.create_collection("chunks", schema=schema)

result = client.insert(
    "chunks", [{"document": "alpha beta"}, {"document": "gamma"}]
)
```

The SDK derives that `text`, `vector` and `tags` are generated. Supply the
original `document` values; omit generated destination values. The explicit
VARCHAR hint tells the SDK how to encode a temporary input absent from the
collection schema. Hints describe input types; they are not cast operators.
Temporary scalar/vector inputs are supported, while nested temporary inputs
require a collection-schema declaration.

An in-place map that reads and overwrites `text` still requires input `text`.
If other required stored fields such as a tenant/partition key are supplied,
keep them in the final select. Final values bind to destination schema types,
dimensions and nullability. AutoID is allocated for produced children; explicit
primary keys must be distinct after expansion.

Keep select as the final operator when its columns define the complete output.
The executor can then discard unused parent payload before FlatMap's internal
Explode repeats it for every chunk. Inputs read by later operators, including
`document` or `tenant`, remain available, and system identity/lineage is
preserved. Without a terminal select, the complete parent payload is retained;
an intermediate select alone does not enable automatic pruning.

The example returns three IDs and:

```python
{
    "input_rows": 2,
    "output_rows": 3,
    "source_offsets": [0, 1, 2, 3],
    "source_row_indexes": [0, 0, 1],
    "outcome_unknown": False,
}
```

This is `result["ingestion_result"]`. Its entry at output position `i` matches
`result["ids"][i]`; use the CSR offsets to locate its original input row.
`insert_count` counts confirmed stored children, not supplied documents. Empty
documents produce no children and offsets `[0]`; the zero-output path does not
append records to WAL.

## 4. Prepare queries and select POST payload

Build a vector index and load the collection before searching, as in the
complete script:

```python
index = client.prepare_index_params()
index.add_index("vector", index_type="FLAT", metric_type="COSINE")
client.create_index("chunks", index)
client.load_collection("chunks")
```

PRE's payload column is named `query`. This chain translates
the request inputs, then produces their query vectors:

```python
pre = (
    FunctionChain(FunctionChainStage.PRE_PROCESS, "translate_and_embed")
    .map(
        "query",
        fn.py_udf(
            col("query"), resource_name="transform_demo",
            udf_params={"mode": "translation"},
        ),
    )
    .map(
        "query",
        fn.py_udf(
            col("query"), resource_name="transform_demo",
            udf_params={"mode": "embedding"},
        ),
    )
)
post = (
    FunctionChain(FunctionChainStage.POST_PROCESS, "highlight")
    .map(
        "highlight",
        fn.py_udf(
            col("text"), resource_name="transform_demo",
            udf_params={"mode": "highlight"},
        ),
    )
    .select(["highlight"])
)
rerank = (
    FunctionChain(FunctionChainStage.L2_RERANK, "query_aware_rerank")
    .map(
        "$score",
        fn.py_udf(
            col("text"), resource_name="transform_demo",
            udf_params={"mode": "rerank"},
        ),
    )
    .sort("$score", desc=True)
    .limit(3)
)
hits = client.search(
    "chunks", data=["hello", "world"], anns_field="vector", limit=3,
    function_chains=[pre, rerank, post],
)
```

PRE batches the two query rows and preserves their count/order. Dictionary
translation produces `你好` and `世界`; embedding returns two 2D vectors.
Generated vector dimension/type must match the target field, with native
FloatVector-to-FP16/BF16 conversion supported. Final text can instead enter a
collection's native query embedding or BM25 path.

L2 receives a separate candidate group and `query_index` for each query. The
demo ranks `alpha` first for `hello` and `gamma` first for `world`.

POST calls `transform_batch` once with all query groups after ranking. Here
Python returns one Struct array per group, including `text`, `start`, the
original query, its index and total hit count across all groups. Each Struct
becomes a JSON highlight object in the corresponding hit's entity. Select
exposes only highlight payload. IDs, scores and
query/candidate boundaries remain dedicated result metadata. A final select
also controls aliases/output order; without select, original requested fields
and generated payloads are returned, while hidden model inputs remain hidden.

Generated payload is typed from its runtime values and has `field_id=0`; it
does not add stored collection fields. Scalar arrays, scalar StructArray inputs
and supported single vectors can be consumed. ArrayOfVector cannot yet be a
POST source; unreferenced original vector-array payload can pass through.
Generic Struct-to-JSON supports nested lists/structs and nulls, but embedded
timestamp/decimal/dictionary types are unsupported. Malformed text/JSON and
non-finite JSON numbers fail as function outputs.

In HybridSearch, attach PRE to each `AnnSearchRequest` and POST to top-level
`function_chains`. PRE/POST can coexist with RRF or an existing ranker. To change
scores/order, use an actual rerank chain such as L2; POST does not rerank.

## 5. Choose map, explode and flat_map

| Operator | Meaning | Available transformation stages |
|---|---|---|
| `map` | Compute/overwrite one or more columns, one outer value per input row | INGESTION, PRE, POST |
| `select` | Keep columns and optionally rename them; preserves identity/lineage | INGESTION, PRE, POST |
| `explode` | Expand an existing List column and repeat parent columns | INGESTION |
| `flat_map` | Run a List-valued expression, then expand its list | INGESTION |
| `filter` | Remove ingestion rows with a Boolean expression | INGESTION |

A chunking UDF returns one `List<Struct>` per document. FlatMap emits each
Struct as a `chunk`, and select reads `chunk.text`/`chunk.vector`. Position is
the optional second operator output: `[chunk, chunk_index]`. The UDF returns
one List array, not two arrays and not an arbitrary flat row batch. The index
starts at zero for each document. The example drops it; add a stored Int64
field and include it in final select if it should be persisted.

The equivalent two-step form is:

```python
chain.map("chunks", chunk_expression).explode(
    "chunks", "chunk", position="chunk_index"
)
```

Default expansion drops null/empty lists. `outer=True` emits one null element
and null position for them, requiring compatible destination nullability.
Null elements inside a nonempty list retain their actual index. Successive
explodes form Cartesian expansion. Multi-row `aggregate`/`reduce` is not
implemented; existing merge/group_by do not create an arbitrary summary row.

## 6. Write a model wheel

The example's [entry-point declaration](../../internal/util/function/pyudf/python/examples/transform_demo/pyproject.toml)
exports `factory` in `milvus.pyudf`. The factory receives stage context and
returns an instance with exactly one callable:

- INGESTION: `transform(params, columns)`.
- PRE/L2: `transform_query(params, columns, *, context)`.
- POST: `transform_batch(params, batches, *, context)`.

Use runtime 0.2 or newer for this contract and update POST wheels to the batch
callable. Existing two-argument INGESTION/PRE/L2 methods are accepted; declare
the `context` keyword when using request metadata. Keep the factory's
initialization context separate from this per-invocation context.

For an ordinary search, a reranker reads the corresponding original query as:

```python
query = context.original_queries[0].values[context.query_index]
```

In L2/POST, `context.queries` contains the final PRE payloads, before native embedding.
They may already be vectors if PRE performs embedding itself. VarChar values
are strings; vector values are bytes with a `data_type` name. For HybridSearch,
the outer index identifies the ANN subrequest, and each input has NQ values.
L0/L1 use the same proposed grouping/index contract but do not yet execute
PyUDF in QueryNode.
During PRE, context contains the payloads from entry to the PRE chain; current
intermediate query values are available in `columns`.

POST receives all query inputs in context and all candidate groups in `batches`:

```python
import pyarrow as pa

class PostProcess:
    def transform_batch(self, params, batches, *, context):
        outputs = []
        for query_index, columns in enumerate(batches):
            query = context.original_queries[0].values[query_index]
            outputs.append([
                pa.array(
                    [f"{query}: {text.as_py()}" for text in columns[0]],
                    type=pa.string(),
                )
            ])
        return outputs
```

`batches[i]` contains the selected input columns for query `i`; zero-hit queries
remain present as typed empty arrays. `context.query_index` is `None` for POST,
and `context.result_offsets` gives cumulative candidate counts. POST returns
the same number of groups and candidates; N-to-one aggregation remains a
separate capability.
Include `col("$id")` or `col("$score")` in the expression when the UDF needs
candidate identity or scores as input columns.

Return a sequence of typed `pyarrow.Array` objects with one outer value per
input row. Specify types for zero-row and all-null arrays. A map can return
several arrays when its output names match their count; flat_map returns one
List array. POST wraps these output arrays in one sequence per query group.
Factory instances can be called concurrently, so keep model state
thread safe. Set external API timeouts explicitly; a Milvus RPC timeout does
not terminate running Python or undo an external call.

| Use case | Hook |
|---|---|
| Document chunking with embeddings/tags | INGESTION flat_map → select |
| Gemini or another embedding API | INGESTION map; PRE map for query vectors |
| Input translation/normalization | INGESTION map for documents; PRE map for queries |
| Semantic highlight or per-hit enrichment | POST map → select |
| Model reranking | L2 rerank, using a score-producing UDF/model |
| Video keyword extraction from a URL/object reference | INGESTION map producing stored keywords |

These hooks let your wheel call a model service; the deterministic demo does
not validate any particular external provider. Install the provider dependency
in the worker environment, return the declared output shape and use the real
model dimension. Keep credentials in deployment configuration/environment,
not persistent chain parameters. A preprocessing function can normalize text;
replacing Milvus/BM25's tokenizer still uses the analyzer configuration/API.

## 7. Handle partial or unknown INSERT outcomes

Ingestion INSERT has no request-wide transaction across channels and rejects
idempotency keys in v1. A failure can contain confirmed IDs and source mapping;
an unknown outcome can mean additional rows were written. Inspect the retained
result before deciding how to reconcile writes:

```python
from pymilvus.exceptions import IngestionFunctionChainError

try:
    result = client.insert("chunks", [{"document": "alpha beta"}])
except IngestionFunctionChainError as error:
    mutation = error.mutation_result
    confirmed_ids = [] if mutation is None else mutation.primary_keys
    mapping = None if mutation is None else mutation.ingestion_result
    outcome_unknown = error.unknown_outcome
    # Record/reconcile these values; do not replay the entire document blindly.
    raise
```

The SDK disables automatic ingestion replay even if retry options were passed.
A lost RPC response can have no MutationResult. A local MutationFuture wait
timeout may be resolved by waiting on the same future, without issuing a new
INSERT. WAL recovery uses materialized rows and does not run the model again;
this does not guarantee exactly-once model execution across new requests.

## 8. Return independent POST tables

Use the matching PyMilvus checkout with its `arrow` extra and worker runtime
0.3 for this mode. Ordinary HITS searches do not require the Arrow extra.

```python
from pymilvus.function_chain import agg

post_table = (
    FunctionChain(FunctionChainStage.POST_PROCESS, result_mode="table")
    .concat()
    .group_by("text")
    .aggregate({
        "matches": agg.count(),
        "queries": agg.collect_list(col("$query_index")),
        "best_score": agg.max(col("$score")),
    })
)
table_result = client.search(
    "chunks", [[1.0, 0.0], [0.0, 1.0]], anns_field="vector",
    search_params={"metric_type": "COSINE"}, limit=3,
    function_chains=post_table,
)
print(table_result.schema)
print(table_result.rows)
arrow_table = table_result.to_arrow()
```

`concat()` combines all current groups in order. Without it, aggregates run
independently for each current query/ANN group. Group keys follow first
occurrence, including a null-key group. Aggregates include `count`, `sum`,
`avg`, `min`, `max`, `first`, `last` and `collect_list`. `count()` counts rows;
`count(col(...))` counts non-null values. FIRST/LAST and COLLECT_LIST retain
nulls and can carry supported nested values.

For Hybrid Search, use `input="ann_inputs"` to handle the separate ANN branches
before top-level fusion. Each AnnSearchRequest's limit bounds its own inputs;
the final Hybrid limit is not an input cutoff in this mode. Use `$query_index`
and `$ann_index` to preserve source identity, and optionally group by `$id` to
combine repeated documents. This mode replaces top-level fusion, so do not
also supply a top-level L2 chain or ranker. FUSED mode instead receives normal
post-rerank candidates and honors the normal search limit.

Custom many-to-one summaries use a reducer:

```python
summary = (
    FunctionChain(FunctionChainStage.POST_PROCESS, result_mode="table")
    .concat()
    .reduce("summary", fn.py_udf(
        col("text"), resource_name="transform_demo",
        udf_params={"mode": "summary"},
    ))
)
query_table = client.query(
    "chunks", filter="id >= 0", limit=100,
    function_chains=summary,
)
```

A TABLE reducer implements `transform_batch` and returns one row per supplied
group; `context.table_result` and `context.reduction` are true. MAP still
preserves each group's rows. `context.query_count` and query snapshots describe
the original search, independently of current grouping; Query has query_count
zero. Pass source index columns explicitly when the function needs them.
`.group_by(...).reduce(...)` supplies one input group per distinct key. If all
keyed groups are empty, the UDF is invoked once with typed empty columns to
obtain its output schema, and its output row is discarded. Reducers must handle
empty input without assuming a first row.

TABLE output carries its own Arrow schema, including Struct/List outputs and
typed empty results. It has no mandatory hit ID or score. A global native
aggregate of empty input emits COUNT zero; a keyed aggregate emits no rows.
Query input still follows the normal filter, limit and offset; this does not
aggregate rows outside the retrieved candidate set. The matching PyMilvus
`examples/function_chain_post_tables.py` is an executable end-to-end example.

## Current boundaries and verification

HITS mode rejects transformations on Query. All modes reject Search Iterator
and search-by-IDs; PRE
embedding-list input; POST search_aggregation; and new transformation bindings
or stages through REST. Bound collections reject Upsert/partial update/Import
and ingestion INSERT idempotency keys. Binding replacement through ALTER is
not implemented. Query POST accepts TABLE/FUSED only and rejects native Query
aggregation; aggregate inside the chain instead. REST transformation requests
are rejected explicitly. Nested/group-key type support follows the individual
operators; TABLE does not promise every Arrow type as a grouping key.

The matching local SDK/server/runtime-0.2/WAL demonstration passed with
2 inputs → 3 stored rows, aligned CSR, two PRE-prepared queries, query-aware
L2 reranking and one POST call containing both result groups. The two queries
ranked different documents first, and every POST hit carried the correct
query/index plus the total hit count across both groups. A separate Hybrid
Search probe retained both ANN query inputs. Candidate group sizes `[3, 3]`,
`[2, 0]` and `[0, 0]` each produced exactly one POST invocation, including
empty groups. Empty INSERT returned zero rows; deliberate UDF failure returned
code 2400, retained its MutationResult with known outcome, and added no rows.
The earlier ingestion demonstration also verified that a restarted server
retained the binding and the three rows/vectors/tags.
These observations do not validate external model services, CDC fault recovery
or mixed-version deployment. Graceful shutdown was not verified: existing etcd
cleanup panicked during SIGTERM.

Native race regressions verify column retention and pruning. In one controlled
Arrow test, projecting 256 tiny chunks from a 4 KiB document reduced checked
allocator peak from 1,068,800 to 16,064 bytes. This measures Arrow allocations in
that case, not total worker/process memory. The runtime wheel build passed;
Docker images were not built, and the full repository Go target did not pass.

See the [design and verification matrix](../design-docs/design_docs/20261010-function-chain-transform-stages.md)
for exact contracts, test coverage and local evidence.
