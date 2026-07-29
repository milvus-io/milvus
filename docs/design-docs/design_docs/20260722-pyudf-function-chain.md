# MEP: PyUDF FunctionChain Expression

- **Created:** 2026-07-22
- **Author(s):** @junjie.jiang
- **Status:** Draft
- **Component:** Function Chain / Embedded CPython
- **Related Issues:** TBD
- **Released:** N/A

## Summary

PyUDF adds a `py_udf` FunctionChain expression for executing a Python wheel during L2 rerank. The current runtime embeds CPython in Milvus, exchanges Arrow data with PyArrow through the Arrow C Data Interface, and lazily loads synchronized FileResource wheels through a lease-based cache.

The expression package owns a process-global Production Runtime, following the existing XGBoost expression ownership pattern. Package initialization reads the immutable configuration, initializes embedded CPython immediately when enabled, registers the final runtime as a FileResource listener, and binds every `PyUDFExpr` to it without component-specific FunctionChain injection.

## Scope

The first release is constrained to:

- expression name `py_udf`;
- `StageL2Rerank` support through the package-global expression runtime with no component-specific build-context injection;
- embedded CPython only;
- one Python wheel per FileResource;
- wheels already synchronized to the executing process's local storage;
- synchronous `transform_query` execution;
- Arrow C Data exchange without Arrow IPC;
- trusted runtime API version `1`;
- lazy resource loading, singleflight, leases, and stale eviction;
- a uniform `CStatus` C ABI with function-pipeline error code `2400`.

The following are not implemented in the current slice:

- Proxy FileResource synchronization/download and the initial authoritative snapshot delivery;
- an end-to-end Proxy Search execution using a user wheel delivered by that synchronization path;
- full-batch `transform` execution;
- dedicated executor or admission queue;
- asynchronous `CFuture` execution;
- hard interruption of Python code;
- multi-instance scheduling or reentrant execution;
- process-pool isolation;
- L0 or L1 execution;
- wheel upload, signing, permission checks, or registration-time validation;
- general LRU or cached-resource memory limits.

## Public FunctionChain contract

### Expression

The public resource parameter is named `resource_name` at every PyUDF layer:

```text
py_udf(
  feature_a,
  feature_b,
  resource_name="rank_udf",
  udf_params={"mode": "add", "factor": 0.3}
)
```

The corresponding FunctionChain expression is:

```text
expr.name = "py_udf"
expr.args = [column("feature_a"), column("feature_b")]
expr.params["resource_name"] = string("rank_udf")
expr.params["udf_params"] = object(...)
```

A client convenience API may expose positional syntax, but the compiled server-side parameter remains `resource_name`:

```python
fn.py_udf(
    resource_name="rank_udf",
    columns=[col("feature_a"), col("feature_b")],
    params={"mode": "add", "factor": 0.3},
)
```

PyUDF is a new expression and does not preserve aliases such as `name` or `udf_name`.

### Validation

- `resource_name` is required and cannot be empty.
- `udf_params` is optional and defaults to an empty object.
- Unknown parameters are rejected.
- At least one input column is required.
- Expression arguments must be column references, not literals.
- Duplicate input columns preserve their position and multiplicity.
- PyUDF is runnable only at `StageL2Rerank`.
- Output count and types are determined at execution time.

For example:

```text
py_udf(col("a"), col("a"), col("b"))
  -> MapOp.inputs = ["a", "a", "b"]
  -> Python columns = [a, a, b]
```

Dependency analysis may still deduplicate required fields so that `a` is fetched only once.

### Dynamic outputs

`PyUDFExpr.OutputDataTypes()` returns `nil`. After execution:

1. the runtime returns the actual `[]*arrow.Chunked` outputs;
2. the PyUDF expression validates supported types and chunk layout;
3. the generic `MapOp` compares the actual output count with declared map outputs;
4. outputs are assigned positionally.

Supported output Arrow types are:

- bool;
- int8, int16, int32, int64;
- float32, float64;
- string.

An output assigned to `$score` must satisfy the existing FunctionChain score type rules.

## Control messages

Small control messages reuse `pkg/proto/cgo_msg.proto`:

```proto
message PyUDFLoadRequest {
  string resource_name = 1;
  int64 resource_id = 2;
  string resource_path = 3;
  string local_path = 4;
  string stage = 5;
  int32 instance_count = 6;
}

message PyUDFRunParams {
  string resource_name = 1;
  string stage = 2;
  schema.FunctionParamObject udf_params = 3;
}
```

Arrow arrays, schemas, descriptors, and IPC payloads are not placed in these messages.

Parameter rules are:

- supported values are bool, int64, double, string, bytes, array, and object;
- `udf_params=nil` is normalized to an empty object;
- unset values and nil nested array/object nodes are rejected;
- object keys and string values must be valid UTF-8;
- bytes remain opaque and need not be UTF-8;
- maximum recursive depth is 64;
- Go clones the parameter object before serialization;
- deterministic protobuf marshal stabilizes map wire ordering for tests and diagnostics.

## Wheel contract

One PyUDF FileResource points to one `.whl` file. The wheel contains user code and any private model/configuration resources and declares exactly one `milvus.pudf` entry point:

```toml
[project.entry-points."milvus.pudf"]
main = "xgboost_rank:create_udf"
```

The entry point resolves to a factory:

```python
def create_udf(context):
    return RankUDF(context)
```

The runtime:

- does not run `pip install` for user wheels;
- opens the synchronized `ResolvedFileResource.LocalPath` directly;
- requires exactly one `milvus.pudf` entry point;
- caches the created UDF instances with the loaded resource;
- rejects conflicting top-level Python packages between loaded wheels;
- expects shared dependencies such as PyArrow 23.0.1 to be provided by the Milvus runtime image.

Private model and configuration files should be packaged inside the user wheel.

## Trusted Python runtime

The trusted package is built from:

```text
internal/core/src/pyudf/python/milvus_pyudf_runtime/
```

It is separate from user wheels and must be installed into the system site-packages of the same Python interpreter selected by CMake. Milvus imports it at runtime but never installs it during service startup.

The unreleased trusted contract remains:

```python
RUNTIME_API_VERSION = 1
```

Development iterations do not increment this value before the first released compatibility boundary.

The package provides:

- wheel metadata and entry-point loading;
- immutable `PyUDFContext` construction;
- Arrow C Data import/export helpers;
- recursive parameter freezing;
- `transform_query` execution and return validation;
- optional instance close handling.

The wrapper requires exactly `pyarrow==23.0.1`. The version is pinned in the trusted wheel metadata so installing the wheel resolves the same runtime dependency in every image.

## Python UDF interface

### Context

The factory receives initialization-time context:

```python
PyUDFContext(
    resource_name="xgboost_rank_v2",
    wheel_path="/local/path/xgboost_rank-0.2.0-py3-none-any.whl",
    stage="L2_rerank",
    logger=...,
    runtime_info=...,
)
```

The context contains stable runtime information. Request-specific values are passed to the execution method through `params` and `columns`.

### Implemented execution method

The current execution path supports `transform_query`:

```python
class ScoreAdjustUDF:
    def transform_query(self, params, columns):
        factor = float(params.get("factor", 1.0))
        values = [value.as_py() * factor for value in columns[0]]
        return [pa.array(values, type=pa.float32())]

    def close(self):
        pass
```

For each input chunk/query, the wrapper passes a sequence of `pyarrow.Array` objects. The method must return a sequence of `pyarrow.Array` objects.

Rules:

- input order matches expression argument order;
- a single output still uses a sequence of length one;
- each output array length equals the current query row count;
- output count and type remain stable across all query chunks;
- Python lists and NumPy arrays are not implicitly converted;
- request parameters are recursively frozen into immutable mappings and tuples.

The loader can identify a wheel exposing `transform`, but the native run path currently rejects it as unsupported. Full-batch `transform` is a future execution slice, not a usable current behavior.

## Arrow C Data exchange

The data path is:

```text
Go arrow.Chunked
  -> cdata.ExportArrowArray
  -> C++ invocation-owned ArrowArray / ArrowSchema
  -> PyArrow _import_from_c
  -> user transform_query
  -> PyArrow _export_to_c
  -> C++ result-owned ArrowArray / ArrowSchema
  -> cdata.ImportCArray
  -> Go arrow.Chunked
```

The control plane uses protobuf; the data plane uses Arrow C Data. No Arrow IPC encoding or decoding is performed.

### Invocation ownership

- C++ allocates stable, zero-initialized input descriptor slots.
- Go exports each input chunk into those slots.
- `DeletePyUDFInvocation` releases every descriptor not consumed by PyArrow.
- Once PyArrow imports a descriptor, its Python/Arrow references own the corresponding release path.
- Resource and invocation handles remain caller-owned across synchronous `RunPyUDFResource`.

### Result ownership

- C++ allocates stable, zero-initialized output descriptor slots.
- PyArrow exports returned arrays into those slots.
- Go imports each output descriptor and assembles `arrow.Chunked` values.
- `DeletePyUDFResult` releases any output slot not imported by Go.
- Partial export/import failures release consumed and unconsumed descriptors exactly once.

### Layout invariants

- every input column has the same chunk count;
- chunks at the same index have the same row count;
- each chunk represents one query/NQ;
- zero-row chunks, null bitmaps, and sliced offsets are supported;
- outputs preserve input chunk count and per-chunk row count;
- one logical output has one stable Arrow type across all chunks.

The zero-copy claim applies to Arrow buffers crossing Go and PyArrow. Constructing the initial Arrow DataFrame from Search results still writes data into Arrow buffers.

## Native runtime

The current C ABI is synchronous and uniform:

```c
bool PyUDFRuntimeBuildEnabled(void);
CStatus InitializePyUDFRuntime(void);
CStatus LoadPyUDFResource(..., CPyUDFResource* resource);
CStatus RunPyUDFResource(
    CPyUDFResource resource,
    CPyUDFInvocation invocation,
    const uint8_t* serialized_params,
    uint64_t serialized_params_len,
    CPyUDFResult* result);
CStatus DeletePyUDFResource(CPyUDFResource resource);
```

All operations return ordinary `CStatus`; PyUDF does not define a parallel status structure or error-origin field.

### CPython lifecycle

- `InitializePyUDFRuntime` is process-level and idempotent.
- initialization uses isolated `PyConfig`;
- ambient `PYTHONPATH` and user site-packages are ignored;
- the trusted package API version is validated as `1`;
- the main interpreter is retained for process lifetime;
- initialization releases the initial GIL with `PyEval_SaveThread`;
- subsequent Python C API calls acquire the GIL;
- normal shutdown does not call `Py_FinalizeEx`.

When `MILVUS_ENABLE_PY_UDF=OFF`, the same ABI is provided by a stub and returns an explicit unsupported error without linking libpython.

### Current concurrency behavior

The loader creates `instancesPerResource` Python objects, but current execution selects `instances_[0]`. A resource mutex serializes Run and Close. Therefore `instancesPerResource` is not yet active scheduling capacity.

The following remain future work:

- choosing among multiple instance slots;
- concurrent reentrant execution;
- a dedicated native executor;
- Go admission control and queue limits;
- asynchronous `CFuture` ownership;
- query-boundary cancellation.

## Go Production Runtime

`ProductionRuntime` composes the util-layer pieces:

```text
ProductionRuntime
  -> Runtime interface
  -> FileResource Listener interface
  -> Cache
  -> embedded ResourceLoader
  -> native CPython resource
```

When enabled, construction:

1. validates binary capability;
2. initializes the embedded native runtime;
3. builds an embedded `ResourceLoader`;
4. creates the FileResource cache with the configured load timeout.

When disabled, construction returns an unavailable runtime and does not initialize CPython.

The expression package creates Production Runtime as a process-global object:

- package initialization parses the non-refreshable configuration and constructs the final Production Runtime with a process-lifetime context, matching the XGBoost expression ownership pattern;
- when enabled, construction validates build capability and initializes embedded CPython immediately; a configuration, capability, native initialization, or cache construction failure panics and prevents normal process startup;
- native CPython initialization remains process-idempotent through the C++ runtime's `std::call_once`;
- when disabled, construction installs an unavailable Runtime without checking native capability or initializing CPython;
- package initialization registers the final Production Runtime directly as FileResource listener `pyudf`, so snapshots are delivered to the cache without manager forwarding or replay;
- request context is used for `Acquire` and `Run`, not for global runtime/cache lifetime;
- `PyUDFExpr` defaults to this global Runtime, so `FunctionBuildContext` remains free of PyUDF-specific dependencies and Proxy, QueryNode, or DataNode startup code does not need PyUDF-specific hooks.

Individual user wheels remain lazily loaded on first acquisition. Resource update/removal closes stale resources after their final lease. Process shutdown does not explicitly close the currently active resource set or finalize CPython; the operating system reclaims process resources.

### FileResource cache

The cache:

- accepts authoritative `fileresource.SyncEvent` snapshots;
- indexes only `.whl` resources;
- returns retryable `ServiceUnavailable` before the first snapshot;
- ignores snapshots whose version is not newer than the current version;
- resolves `resource_name` to `ResolvedFileResource.LocalPath`;
- lazy-loads per resource identity and stage;
- merges concurrent first loads with singleflight;
- protects loaded resources with leases/refcounts;
- removes replaced/deleted resources from future lookup immediately;
- closes a stale resource after its last lease is released;
- retires all loaded resources when the runtime closes and closes each resource after its final active lease is released.

The cache key uses `ResolvedFileResource.ID`, `Name`, remote `Path`, and stage. `LocalPath` is not part of the current identity, so a snapshot that changes only `LocalPath` does not trigger replacement; this should be revisited when the FileResource synchronization contract is integrated. The key does not use the wheel filename alone.

### Synchronous cancellation semantics

The current Go adapter checks context:

- before native load or run;
- after native load or run returns.

If a run finishes after the context expires, imported outputs are released before returning the context error. An executing Python call is not interrupted. This is cooperative pre/post-call handling, not hard cancellation.

## Error contract

The C ABI reserves the function-pipeline code:

```c
typedef enum CPyUDFErrorCode {
    PyUDFErrorCodeFunctionFailed = 2400,
} CPyUDFErrorCode;
```

The mapping is:

```text
user wheel/import/factory/transform/return-contract/close failure
  -> PyUDFFunctionError
  -> CStatus error_code 2400
  -> merr.ErrFunctionFailed

native/runtime/control/Arrow/file/handle failure
  -> native SegcoreError or system exception
  -> CStatus with native code
  -> merr.SegcoreError(code, message)
```

Code `2400` belongs to the Function pipeline family, not the segcore `2000-2099` table. Go handles `2400` before invoking the segcore classifier.

Classification examples:

- missing or empty `resource_name`: parameter error;
- resource absent from a ready snapshot: parameter error;
- snapshot not yet received: retryable service unavailable;
- resolved resource with empty/unreadable local path: system error;
- user wheel metadata/import/factory failure: function failed (`2400`);
- user `transform_query` exception: function failed (`2400`);
- invalid user output object, type, count consistency, or layout: function failed (`2400`);
- Arrow descriptor, native handle, protobuf, or trusted-wrapper invariant: system/native error;
- cancellation/deadline: original context error.

A Python `ValueError` is not automatically an input error because it can originate from user code or a dependency.

## Configuration

```yaml
function:
  pyUDF:
    enabled: false
    loadTimeout: 30s
    executorThreads: 1
    maxQueueSize: 64
    instancesPerResource: 1
```

Current behavior:

- `enabled` controls whether package initialization constructs Production Runtime with embedded CPython; enabled initialization failure panics and stops normal process startup;
- `loadTimeout` provides a deadline checked around wheel import, factory execution, and instance initialization; the synchronous native call cannot be interrupted, so work that returns after the deadline is rejected and closed rather than being hard-stopped at the deadline;
- `instancesPerResource` controls the number of objects created at load time, although only the first is currently executed;
- `executorThreads` and `maxQueueSize` are validated configuration reserved for the future executor/admission slice and do not yet create scheduling capacity.

Configuration is non-refreshable. Runtime selection does not silently fall back.

### Embedded configuration limitation

The current package-global initialization model is not compatible with enabling PyUDF from Embedded Milvus configuration:

1. importing the FunctionChain expression package constructs `globalPyUDFRuntime` during Go package initialization;
2. that construction calls `paramtable.Get()`, which initializes the global parameter table through `sync.Once`;
3. the Embedded entry point sets `MILVUSCONF=/tmp/milvus/configs/` only later, when `startEmbedded()` is invoked;
4. `MilvusRoles.Run()` subsequently calls `paramtable.InitWithBaseTable(...embedded-milvus.yaml)`, but the earlier `sync.Once` initialization prevents that Embedded configuration from replacing the parameter table.

Consequently, the package-global runtime reads the pre-Embedded/default configuration, normally leaving PyUDF disabled even if `embedded-milvus.yaml` enables it. PyUDF is therefore not supported in Embedded Milvus under the current initialization model. Standalone and cluster processes must make their configuration environment available before process/package initialization. Supporting Embedded Milvus requires moving its configuration/environment setup ahead of Go package initialization or replacing the package-global configuration read with an explicit post-configuration bootstrap.

## Build and packaging

The trusted runtime wheel can be built and verified with:

```bash
make build-pyudf-runtime-wheel
make test-pyudf-runtime-wheel
make PYTHON=/path/to/cmake-selected-python install-pyudf-runtime-wheel
```

The build and test targets do not modify system site-packages. The explicit install target does, and must use the same interpreter selected by CMake for embedded runtime tests.

`MILVUS_ENABLE_PY_UDF=ON` requires Python 3.10+ with `Interpreter` and `Development.Embed`. Official builder and runtime Dockerfiles use a named uv 0.11.23 stage and independently run `uv python install 3.12.13`. Builders select `python3.12` for CMake and trusted-wheel construction; runtime images install the trusted wheel with uv, which resolves the metadata-pinned `pyarrow==23.0.1`, and register that Python distribution's `libpython` directory with the dynamic linker. The Python installations are recreated from the same pinned uv and Python versions rather than copied between images.

## Verification completed for the current slice

The implemented Production Runtime slice has focused coverage for:

- trusted Python runtime and wheel packaging;
- C ABI invocation/result ownership;
- embedded CPython initialization and resource lifecycle;
- synchronous `transform_query` execution;
- Go Arrow C Data import/export;
- cache snapshot readiness, versioning, lazy load, leases, and eviction;
- disabled runtime behavior;
- real-wheel `ProductionRuntime` execution;
- function error code `2400` and native/system error preservation;
- enabled and disabled native build paths;
- concurrency coverage for cache/runtime paths;
- package-global runtime construction, panic-on-initialization-failure behavior, direct listener delivery, and request-context separation.

These tests verify package-global initialization and the expression/util/native runtime. They do not verify user-wheel Search E2E because Proxy FileResource synchronization is not implemented yet.

## Remaining integration work

The next integration slice should:

1. add Proxy FileResource synchronization/download and initial snapshot delivery;
2. extend coordinator distribution and readiness tracking to Proxy nodes;
3. test resource add/update/remove and in-flight lease behavior through Proxy;
4. run a real user-wheel Search E2E that rewrites `$score`;
5. trace `ErrFunctionFailed(2400)` and native/system errors through the complete Search response path.

## Future work

- full-batch `transform`;
- dedicated executor and admission queue;
- active multi-instance scheduling;
- reentrant capability;
- asynchronous CFuture execution;
- finer cooperative cancellation;
- process-pool crash isolation and hard timeout;
- general cache memory accounting and LRU;
- registration-time wheel validation, signing, and permissions;
- L0/L1 execution.
