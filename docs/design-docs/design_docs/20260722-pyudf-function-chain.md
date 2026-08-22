# MEP: PyUDF FunctionChain Expression

- **Created:** 2026-07-22
- **Author(s):** @junjie.jiang
- **Status:** Draft
- **Component:** Function Chain / Embedded CPython
- **Related Issues:** TBD
- **Released:** N/A

## Summary

PyUDF adds a `py_udf` FunctionChain expression for executing a Python wheel during L2 rerank. The current runtime embeds CPython in Milvus, exchanges Arrow data with PyArrow through the Arrow C Data Interface, and lazily loads synchronized FileResource wheels through a lease-based cache.

The expression package owns a process-global Production Runtime, following the existing XGBoost expression ownership pattern. Package initialization reads the immutable configuration, validates the enabled build capability, constructs and registers the final runtime as a FileResource listener, and binds every `PyUDFExpr` to it without component-specific FunctionChain injection. Embedded CPython is initialized lazily and exactly once when the enabled runtime handles its first `Acquire`.

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
- a uniform `CStatus` C ABI with function-pipeline error code `2400`;
- Proxy FileResource synchronization/download with an initial authoritative snapshot;
- user-wheel Proxy Search E2E coverage through the synchronization path.

The following are not implemented in the current slice:

- full-batch `transform` execution;
- dedicated executor or admission queue;
- asynchronous `CFuture` execution;
- hard interruption of Python code;
- runtime-managed instance scheduling or UDF concurrency policy;
- process-pool isolation;
- L0 or L1 execution;
- wheel upload, signing, permission checks, or registration-time validation;
- dynamic wheel removal, update, replacement, or hot reload during active use
  or without subsequently restarting every process that loaded the wheel;
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

An output assigned to `$score` is normalized to Float32 at the map boundary.
Float32 is retained as-is; int8, int16, int32, int64, and Float64 are converted
with Arrow numeric cast semantics, including possible precision loss. Other
types, including bool and string, violate the UDF output contract and return
`FunctionFailed` (`2400`). A `$score` output must not contain nulls; null scores
also return `FunctionFailed` instead of being interpreted from the Arrow value
buffer. Outputs assigned to ordinary columns retain their runtime Arrow types
and may contain nulls.

## Control messages

Small control messages reuse `pkg/proto/cgo_msg.proto`:

```proto
message PyUDFLoadRequest {
  string resource_name = 1;
  int64 resource_id = 2;
  string resource_path = 3;
  string local_path = 4;
  string stage = 5;
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

One PyUDF FileResource points to one `.whl` file. The extension is matched
case-insensitively across the FileResource index and native/Python loaders. A
wheel contains exactly one top-level UDF import root (normally a package), may
contain private model and configuration resources below that package, and
declares exactly one `milvus.pyudf` entry point whose module belongs to the same
top-level import root.

### Standard PyUDF project example

A minimal source project uses the standard `src` layout:

```text
rank-udf/
├── pyproject.toml
└── src/
    └── rank_udf/
        ├── __init__.py
        └── assets/
            └── config.json
```

`pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=61", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "rank-udf"
version = "0.1.0"
requires-python = ">=3.10"
# Dependency metadata is descriptive only for PyUDF. Milvus never runs pip for
# a user wheel; operators must preinstall every dependency in site-packages.
dependencies = ["pyarrow==23.0.1"]

[project.entry-points."milvus.pyudf"]
main = "rank_udf:create_udf"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
rank_udf = ["assets/*.json"]
```

`src/rank_udf/assets/config.json`:

```json
{"default_factor": 1.0}
```

`src/rank_udf/__init__.py`:

```python
import json
from importlib.resources import files

import pyarrow as pa


class RankUDF:
    def __init__(self, context, default_factor):
        self.context = context
        self.default_factor = default_factor

    def transform_query(self, params, columns):
        factor = float(params.get("factor", self.default_factor))
        values = [float(value.as_py()) * factor for value in columns[0]]
        return [pa.array(values, type=pa.float32())]

    def close(self):
        pass


def create_udf(context):
    config_text = files("rank_udf").joinpath("assets/config.json").read_text()
    config = json.loads(config_text)
    return RankUDF(context, config["default_factor"])
```

Build it with the standard Python build frontend:

```bash
python -m build --wheel
```

The resulting wheel has one importable top-level package:

```text
rank_udf-0.1.0-py3-none-any.whl
├── rank_udf/
│   ├── __init__.py
│   └── assets/config.json
└── rank_udf-0.1.0.dist-info/
    ├── METADATA
    ├── WHEEL
    ├── entry_points.txt
    └── RECORD
```

The runtime:

- does not run `pip install` for user wheels;
- opens the synchronized `ResolvedFileResource.LocalPath` directly;
- validates the ZIP directory and reads required metadata/modules on demand,
  without a full CRC scan of every wheel member during lazy load;
- requires exactly one `milvus.pyudf` entry point;
- requires exactly one importable top-level Python root matching the entry
  point's root module or package;
- caches the created UDF instance with the loaded resource;
- rejects another PyUDF resource that uses the same top-level UDF import root;
- does not track modules imported by the UDF from system `site-packages`;
- expects shared dependencies such as PyArrow 23.0.1 to be provided by the Milvus runtime image.

PyUDF wheel authors and operators must observe these rules:

- use a pure-Python wheel, normally tagged `py3-none-any`; native extension
  modules cannot be imported directly from a wheel archive;
- give every PyUDF resource a globally unique top-level import root for the
  lifetime of the Milvus process;
- place all UDF-owned modules, models, and configuration below that package and
  use relative imports for its internal code;
- do not vendor third-party dependencies as separate top-level packages in the
  wheel; operators install all dependencies into the embedded interpreter's
  system `site-packages` before Milvus starts;
- treat `project.dependencies` / `Requires-Dist` as documentation for operators;
  the PyUDF loader does not resolve or install it;
- implement `transform_query` for the current execution path, return a sequence
  of `pyarrow.Array` values, and preserve the input row count;
- treat `params` as immutable and remember that arrays arrive as tuples;
- synchronize mutable instance state inside the UDF when it is not thread-safe,
  because concurrent requests may call the same instance;
- use `importlib.resources` for packaged data instead of assuming the wheel has
  been extracted to a directory;
- follow the quiesce-and-restart procedure below before removing or replacing a
  loaded wheel.

### Wheel lifecycle limitation

Dynamic PyUDF wheel removal, update, and replacement are not supported while
active requests may use the wheel, or without subsequently restarting every
process that loaded it. After a wheel has been loaded, its imported modules,
`sys.path` entry, and top-level package claims remain process-wide even after
the corresponding resource instance is closed. Removing or replacing the
FileResource ID, remote path, local path, or wheel contents does not unload or
reload those Python modules safely.

Before removing or replacing a PyUDF FileResource, operators must stop new
requests that can acquire the UDF and wait for all requests using it to finish.
After the resource change has been synchronized, restart every Milvus process
that may have loaded the wheel. Deleting and then re-creating a resource without
quiescing requests and restarting those processes is not a supported update
mechanism.

The PyUDF cache lease protects the loaded native/Python instance from being
closed during an active call. It does not lease, copy, or otherwise retain the
backing `ResolvedFileResource.LocalPath`. Consequently, removing or replacing a
wheel while it is in use may delete the backing archive and cause lazy imports,
`importlib.resources` access, or wheel-internal model/configuration reads to
fail. Callers must follow the quiesce-and-restart procedure above.

TODO: design safe dynamic replacement around a content-derived, unique Python
namespace for every wheel version. The corresponding wheel contract should be
validated before loading and should initially require one UDF package, relative
imports for wheel-internal code, runtime-provided dependencies, and no native
extension modules. Old version namespaces must remain usable until their final
leases drain; the design must also bound retained module memory or recycle the
Python execution process. Until that contract and loader exist, arbitrary wheel
replacement remains unsupported.

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

The embedded loader creates one Python object per resource. The UDF contract
does not define a concurrency capability declaration, and the runtime does not
serialize calls to that object. Concurrent requests call the same UDF instance
directly; a UDF that is not thread-safe must synchronize its own mutable state.
The Go cache lease guarantees that resource close starts only after active runs
release their leases, so Run/Close lifecycle safety does not require a native
execution mutex.

The following remain future work:

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
2. builds an embedded `ResourceLoader`;
3. creates the FileResource cache;
4. records the embedded native initializer for one-time execution by the first `Acquire`.

When disabled, construction returns an unavailable runtime and never initializes CPython.

The expression package creates Production Runtime as a process-global object:

- package initialization parses the non-refreshable configuration and constructs the final Production Runtime with a process-lifetime context, matching the XGBoost expression ownership pattern;
- when enabled, construction validates build capability and prepares the cache, but does not initialize CPython; configuration, capability, or cache construction failure panics and prevents normal process startup;
- the first `Acquire` initializes embedded CPython before consulting the FileResource cache, and initialization failure is cached for all subsequent acquisitions;
- concurrent first acquisitions initialize CPython only once through the Go runtime's `sync.Once`; the C++ runtime additionally remains process-idempotent through `std::call_once`;
- when disabled, construction installs an unavailable Runtime without checking native capability or initializing CPython;
- package initialization registers the final Production Runtime directly as FileResource listener `pyudf`, so snapshots are delivered to the cache without manager forwarding or replay;
- request context is used for `Acquire` and `Run`, not for global runtime/cache lifetime;
- `PyUDFExpr` defaults to this global Runtime, so `FunctionBuildContext` remains free of PyUDF-specific dependencies and Proxy, QueryNode, or DataNode startup code does not need PyUDF-specific hooks.

Individual user wheels remain lazily loaded on first acquisition. At the cache
layer, a newer FileResource snapshot removes stale instances from future lookup
and closes each stale native/Python instance after its final cache lease. The
lease does not retain the backing wheel file, so FileResource removal or
replacement must not race with active requests. Resource changes require the
quiesce-and-restart procedure described in
[Wheel lifecycle limitation](#wheel-lifecycle-limitation). Process shutdown
does not explicitly close the currently active resource set or finalize
CPython; the operating system reclaims process resources.

### FileResource cache

The cache:

- accepts authoritative `fileresource.SyncEvent` snapshots;
- indexes only `.whl` resources;
- returns retryable `ServiceUnavailable` before the first snapshot;
- ignores snapshots whose version is not newer than the current version;
- resolves `resource_name` to `ResolvedFileResource.LocalPath`;
- lazy-loads per resource identity and stage;
- merges concurrent first loads with singleflight;
- protects loaded native/Python instances with leases/refcounts, but does not
  lease or copy their backing FileResource files;
- removes replaced/deleted resources from future lookup immediately;
- schedules a stale resource for asynchronous close after its last lease is released, so user close logic and GIL acquisition never block FileResource listeners or query release paths;
- retires all loaded resources when the runtime closes and asynchronously closes each resource after its final active lease is released.

The cache key uses `ResolvedFileResource.ID`, `Name`, remote `Path`, and stage. `LocalPath` is not part of the current identity, so a snapshot that changes only `LocalPath` does not trigger replacement; this should be revisited when the FileResource synchronization contract is integrated. The key does not use the wheel filename alone.

### Synchronous cancellation semantics

The current Go adapter checks context:

- before native load or run;
- after native load or run returns.

Resource loads use the cache lifecycle context rather than an individual request context. Canceling a request stops that caller from waiting but does not cancel a shared load; a successful load can still be published to the cache for subsequent requests. If a run finishes after its request context expires, imported outputs are released before returning the context error. An executing Python call is not interrupted. This is cooperative pre/post-call handling, not hard cancellation.

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
```

Current behavior:

- `enabled` controls whether package initialization constructs an available Production Runtime. Construction validates native build capability and creates the cache, but embedded CPython initialization is deferred until the first `Acquire`;
- a deferred CPython initialization failure is returned by the first `Acquire` and cached for subsequent acquisitions; it does not panic during package initialization.

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

The install target disables user-site fallback and verifies under isolated Python mode that both `milvus_pyudf_runtime` and `pyarrow` are importable. This guard checks `pyarrow` availability only; dependency metadata remains responsible for version selection.

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
- package-global runtime construction, panic-on-initialization-failure behavior, direct listener delivery, and request-context separation;
- coordinator-to-Proxy FileResource distribution and Proxy snapshot/download behavior;
- Proxy Search API preservation of `ErrFunctionFailed(2400)` returned by task execution.

Python-client E2E coverage is included for user-wheel Search score rewriting, chaining with built-in rerankers, multiple resources, resource removal, recursive parameter values, user exceptions, and output-contract failures such as non-Arrow values, row/count/type mismatches, unsupported Arrow types, and null scores. The failure cases verify `ErrFunctionFailed(2400)` and that a subsequent PyUDF Search still succeeds. These cases require a PyUDF-enabled Milvus deployment; focused local tests do not replace a recorded green CI execution.

## Remaining integration work

The next verification and hardening slice should:

1. include the PyUDF Search E2E cases in a PyUDF-enabled CI job and record the first green run;
2. fault-inject a representative native/system failure through the complete Search stack;
3. resolve the Embedded Milvus configuration initialization limitation described above.

## Future work

- full-batch `transform`;
- dedicated executor and admission queue;
- asynchronous CFuture execution;
- finer cooperative cancellation;
- process-pool crash isolation and hard timeout;
- safe dynamic wheel removal and replacement using the versioned namespace and
  restricted wheel contract described in
  [Wheel lifecycle limitation](#wheel-lifecycle-limitation),
  with process isolation as the fallback;
- general cache memory accounting and LRU;
- registration-time wheel validation, signing, and permissions;
- L0/L1 execution.
