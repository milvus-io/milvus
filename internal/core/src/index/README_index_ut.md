# index_tests test framework

`index_tests` expands index read, filter, and Artifact lifecycle tests from one centrally defined
set of datasets and backend configurations. The core principle is that registration only combines
lightweight descriptors; data is generated, the index is built, and the Reader is opened only when
each GTest runs. This gives every "case × backend" combination its own name, its own failure
result, and its own resource lifecycle, while avoiding storing large data or built indexes in the
parameter table.

## Two central catalogs

Datasets have their descriptors and ownership model defined in
[ScalarTestData.h](test_utils/ScalarTestData.h) and are all registered in `ScalarDataSets()` in
[ScalarDataSets.cpp](test_utils/ScalarDataSets.cpp). Text, Ngram, Spatial, and JSON data can be
split into separate `.cpp` files, but they still join the same `DataCatalog` through this single
entry point.

A `ScalarDataSet<T>` stores only:

- its name and input C++ type `T`;
- the input shape `BackendInputShape`, such as plain scalars, array rows, nested elements, WKB, or
  JSON;
- the coordinate domain `Domain` and an optional logical value type;
- whether the data contains nulls;
- a parameterless `make_data` generator.

The generator returns `ScalarTestData<T>`, which owns the values, the validity bitmap, the batch
boundaries, and the metadata each dataset needs. It also owns the underlying bytes of strings,
arrays, and JSON projections, so no dangling `string_view` or `ArrayView` is stored in the global
catalog. `batch_sizes` only describes the split; `ScalarTestInput<T>` creates borrowed views at
runtime and guarantees that validity bitmap subviews keep their original bit offsets.

The minimal form of a data registration is:

```cpp
catalog.Add<int64_t>({
    .name = "SmallNullable",
    .requires_nullable = true,
    .make_data = [] {
        ScalarTestData<int64_t> data({1, 2, 3});
        data.validity.reset(1);
        return data;
    },
});
```

Backends are defined in [ScalarReaderFactory.h](test_utils/ScalarReaderFactory.h) and are all
registered in `ScalarReaderBackends()` in
[ScalarReaderBackends.cpp](test_utils/ScalarReaderBackends.cpp). A `BackendSpec` represents one
independently runnable build/open configuration, including the index family, input shape, physical
field type, logical value type, coordinate domain, nullability, heap/mmap request, build/load
parameters, and open mode. A Hybrid or JSON wrapper can declare several possible loader index
families and use dataset metadata to fill in runtime parameters such as paths.

Cases do not copy backend configuration and do not implement query results per backend name. The
backend catalog describes "how to create the Reader" and "which capabilities the Reader claims";
the case provides the input, operation arguments, and correct results.

## From a case to independent GTests

[CaseTestDriver.h](test_utils/CaseTestDriver.h) provides the common `IndexTestCase<T>`. `T` is the
build input type; the other fields record the dataset, input shape, coordinate domain, logical value
type, input lifetime, and backend selection conditions. `body` states which stage this test stops
at:

- `Query<Op>` runs one query after opening the Reader, with the capability given by `Op`;
- `QueryBatch<T>` runs several named `Query<Op>` in order on the same Reader;
- `Observe<T>` runs an observation callback after opening the Reader and can specify the
  capabilities it needs;
- `BuildFails` only calls the builder and checks the error, without entering wrapping or `Open`.

`Query<Op>` uses `Op::ValueType` as the build input type, takes the query arguments from
`Op::Args`, and calls the corresponding Reader interface through `Op::Reader`.
The `Op`s in a `QueryBatch<T>` may differ, but every `Op::ValueType` must be `T`. Names within a
batch must be non-empty and unique.

After a case file defines small operation adapters, it can register common descriptors directly:

```cpp
cases.Add(IndexTestCase<int64_t>{
    .name = "FindOneAndThree",
    .dataset = "SmallNullable",
    .body = Query<In<int64_t>>{
        .args = {.keys = {1, 3}},
    },
});
```

When several queries need to be checked on the same dataset, they can be combined into one case:

```cpp
cases.Add(IndexTestCase<int64_t>{
    .name = "Membership",
    .dataset = "HundredThousandRows",
    .body = QueryBatch<int64_t>{
        {"In", Query<In<int64_t>>{.args = {.keys = {7, 31}}}},
        {"NotIn", Query<NotIn<int64_t>>{.args = {.keys = {7, 31}}}},
    },
});
```

During registration, `IndexTestCases::Add` first checks the dataset descriptor and then intersects
the following conditions:

1. the C++ input type, input shape, coordinate domain, and logical value type;
2. whether the data contains nulls, whether the backend allows nulls, and the capabilities declared
   by the query or observation stage;
3. the optional index family, exact backend name, and `select_backend` condition.

A batch query intersects the Reader capabilities declared by all of its subqueries; only backends
that can run the complete batch generate a GTest. An `Unsupported` caused by operation arguments is
still checked by the policy and error expectation of the corresponding `Query<Op>`, and is not
skipped as a missing capability.

Each matching backend generates one `FilterParam` named
`backend_dataset_case`. The parameterized test body only calls `GetParam().run()`, so each entry in
the GTest report is one fixed combination, and no single test body loops over multiple backends.
The registration closure keeps only the case configuration, the dataset descriptor, and one backend
configuration, and does not call `make_data`; the runtime metadata generated by the dataset is
passed to the build/load parameter completion logic at execution time.

`InputLifetime::KeepUntilBodyCompletes` keeps the input data and borrowed views alive until the
query or observation finishes; `ReleaseBeforeBody` uses mutually independent expected data and build
input, and destroys the input owner before the callback.
`BuildFails` borrows its input synchronously and allows only the former lifetime.
`QueryBatch<T>` follows the same rule: with the former lifetime, data is generated once and
Build/Open runs once; with the latter, independent expected data and a temporary build input are
still generated, but the whole batch also runs Build/Open only once.

A dataset's `requires_nullable` means the generated data actually contains nulls; a backend's
`nullable` means that configuration accepts nullable input. Ordinary selection never sends the
former to a non-nullable backend. Only a `BuildFails` that explicitly verifies this contradictory
input may explicitly allow the mismatch; the data must contain nulls, and every backend left after
intersecting index family, name, and predicate must be non-nullable, otherwise registration fails.
This does not mean that every index family promises the same error.

## Execution and result rules

The common driver generates the data and `ScalarTestInput` inside the test body. The query and
observation stages call `ReaderBackend::Create` and both check Count and the coordinate domain; the
Observe stage additionally checks ValueType, the basic Caps relations, and resource statistics. The
Query adapter and the Observe callback then each check the capabilities, dynamic interfaces, and
results they need.
For simple predicates, `Op::Oracle` computes the expectation from the raw data; complex LIKE, regex,
or edge-case data list the offsets explicitly with `ManualHits`. The actual and expected bitmaps are
compared directly as a whole. The query error check wraps only `Op::Run` and verifies the exact
`ErrorCode`, so a build/load failure is never miscounted as a correct query rejection.
A batch adds a `SCOPED_TRACE` with each subquery's stable name when that subquery runs; after an
ordinary `EXPECT` failure the remaining queries keep running, while a fatal failure stops the
current batch so that an invalid Reader is not used further.

`BuildFails` first generates the data and input outside the error assertion and performs the
registry lookup and configuration parsing through `CreateBuilder`; the error assertion wraps only
the production builder's `Build`. It does not call Artifact wrapping or `Open`, so fixture,
registry, and load errors cannot pass as the expected build failure.

The ground truth of pattern-match results still belongs to the case; the backend catalog only
additionally stores the routing expectation for `ShouldUseForOp`: usable, declines optimization but
can still be queried directly, unsupported, or selected depending on the data. Even if an operation
is not suitable for optimization, its result is still verified as long as the contract allows a
direct query.

Ngram and Spatial return candidate sets and do not promise that they equal the final ground truth.
The common contract cases verify only the candidate superset that each interface promises; only for
interfaces that receive an initial mask do they further check that no bits are added outside the
mask and that the AND narrows the mask. The complete first-phase bitmap that a specific
implementation currently produces belongs only in the regression tests of that named index family,
and cannot be the shared expectation for all candidate Readers.

[AssertHelpers.h](test_utils/AssertHelpers.h) centralizes bitmap construction and the equality,
null, and error-code assertions, so that each contract file does not repeat its own comparison
logic.

## Build, Open, and resource lifecycle

[ScalarReaderFactory.cpp](test_utils/ScalarReaderFactory.cpp) splits a run into
`Build`, `Open`, and the combined entry point `Create`:

1. `make_data` creates the input that owns the actual bytes; `ScalarTestInput` creates borrowed
   spans/views.
2. `Build` creates the builder from the production `BuilderRegistry`. The builder is destroyed after
   it consumes the input and returns an `ArtifactPtr`; the input owner lives at least until `Build`
   returns.
3. `Open` takes the `Serialize` or `Consume` path according to the backend configuration.
4. Before returning the Reader, the framework checks the Reader against the Caps derived by the
   actual loader, and checks Domain and ValueType.
5. The Reader is released after the query or observation callback completes; the mapping, file, and
   directory owners held by the Reader are released with it. The expected data is released last.

The `Serialize` path writes the Artifact into the in-memory V3 sink provided by
[TestArtifactIO.h](test_utils/TestArtifactIO.h), then resolves the actual loader index family
through the source and opens it. By the time `Open` returns, the original Artifact, the sink, the
source, and the in-memory transport data have all left local scope, so the returned Reader must
own the state it needs for queries.
For loaders that need local files, the source writes them atomically to its own temporary location.

The `Consume` path hands the `ArtifactPtr` to `IReaderConvertible::FromArtifact` without
serialization; the returned Reader takes over the artifact state it needs to stay alive. This path
verifies RAM/consumable Artifacts rather than simulating a persisted load.

mmap configurations use the system temporary directory as the parent directory. Files or
[LocalDirectory.h](../storage/artifact/LocalDirectory.h) subdirectories created by the loader are
held by RAII owners stored inside the Reader: mappings and file handles are released first, and then
the last directory owner deletes the subdirectory it created; the parent directory does not belong
to the test and is not deleted. `enable_mmap` is a request, and a specific data layout may still
choose heap memory, so implementation regressions that need mmap pick separate data that actually
triggers a file layout.

An observation case that sets `ReleaseBeforeBody` calls `make_data` twice: once for the
expectations and once for Build. The input owner is destroyed right after Build/Open completes,
before the Reader assertions run, which checks that the Reader no longer borrows the test input.
Cases that need to verify finer ownership behavior can explicitly reset the Reader or Artifact, but
ordinary cases do not need to manage temporary paths.

## Entry points

- [ScalarTestData.h](test_utils/ScalarTestData.h): data ownership, input shapes, and lazily
  generated dataset descriptors.
- [ScalarReaderFactory.h](test_utils/ScalarReaderFactory.h) /
  [ScalarReaderFactory.cpp](test_utils/ScalarReaderFactory.cpp): backend descriptors, selection,
  Build/Open/Create.
- [CaseTestDriver.h](test_utils/CaseTestDriver.h): common case configuration, stages, lifetimes, and
  expansion.
- [AssertHelpers.h](test_utils/AssertHelpers.h): shared bitmap, null, and error-code assertions.
- [TestArtifactIO.h](test_utils/TestArtifactIO.h) /
  [TestArtifactIO.cpp](test_utils/TestArtifactIO.cpp): in-memory V3 transport for tests and atomic
  local writes to disk.
