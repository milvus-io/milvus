# Read-only bitmap views and statistics in Milvus

Status: implemented; covered native suites pass, with validation limits below.

Related issue: #53669. Related PR: #53865.

Milvus's expression and MVCC pipeline previously wrote bitmap windows through
`milvus::bitset::BitsetView`. Read-only use and output mutation shared the same
type, so adding a count cache directly to the view could return stale results.
This change separates owning writes from borrowed reads and gives view statistics
an explicit invalidation source.

## Lifecycle

1. A producer allocates owning data and validity bitsets, then fills them.
2. ColumnVector retains the owners instead of extracting their byte vectors.
3. Mutable outputs borrow owners and explicit destination offsets. Readers borrow
   views, keeping the existing bit semantics for each stage.
4. A view caches population count and all/none results for its own interval.
   Owner writes invalidate these values through a mutation version.
5. SQL predicate conversion computes excluded rows as `~(data & valid)`, then
   resets validity. MVCC and sampling mutate owners.
6. ANN receives a read-only filter whose storage remains owned by the query or
   iterator result. Backend mappings and windows keep their own ID-domain counts.
7. Releasing an owner ends its borrowed views' lifetime. Shared result caches and
   asynchronous iterators must retain actual owners for the entire read period.

There is one owning bitmap type and one read-only dense view type. Writer
callbacks use an owner reference plus offsets. A writable view is not exposed.
Scalar indexes that return newly allocated results continue returning owners;
a view cannot replace ownership of a temporary result.

## Cache contract

The cached facts are bitmap population count and all/none. They do not replace
expression result caching, its admission/eviction policy, or query decision
flags such as all-rows-visible.

The owner stores mutation/escape/write-scope metadata with an address that moves
alongside its buffer. The statistics themselves are stored on the view. An
owner mutation changes the generation when cached views exist. View reads check
the generation and lazily recompute stale facts. Copies may reuse already known
facts; subviews represent new intervals and start with unknown statistics.

Cold all/none use their existing early-exit scans. Caching them does not force a
full count. When count is known, all/none are derived from it. A true all/none
result also establishes count as the interval size or zero.

Writable raw pointer escape disables caching because later pointer writes cannot
be observed. A raw-buffer view has no invalidation source and remains uncached.
Native kernels may use a scoped write pointer that must not outlive its scope.

A write scope makes view statistics uncached while editing and invalidates them
on scope exit, including exceptions. Nested scopes on one owner are supported.
Per-bit proxies remain safe when retained and assigned later. Owner mutation and
read access require external synchronization. Atomic cache fields permit
concurrent read-only use, including the first cache computation.

## Window and ID domains

A dense view indexes its own interval starting at zero. Output callbacks receive
owning destinations and separate bit offsets for result and validity. All writes
must preserve neighboring bits and maintain position-based evaluator cursors
when rows are skipped or offset inputs contain duplicates.

Knowhere selectors use backend IDs, potentially translated through out IDs or
contiguous windows. Their prepared counts can also include vector validity and
ID-boundary exclusions. These counters cannot be replaced with an unqualified
whole-row bitmap count. Scorers distinguish result-position masks from masks
indexed by original row offsets, even when both are read-only views.

Serialized raw bitmap payloads start at bit zero. A nonzero-offset input view is
packed into owned storage before encoding; zero-offset inputs retain the existing
borrowed raw path. Compressed output retains any packed owners until pointers
are consumed. Disk cache writes normalize windows before locking/writing slots.

## Validation requirements

The library tests must verify cached reuse and invalidation across every mutation
family, retained proxies, raw pointer escape, owner move, independent windows,
nested scopes, exception exit and concurrent readers. Integration tests must
cover SQL three-valued logic, validity masking, sequential/scattered expression
batches, MVCC snapshots/TTL, array-element folding, scorer ID domains, serializer
windows and iterator buffer lifetime.

Performance evidence should separately measure first statistics computation,
repeated statistics, mutation/build overhead and overall query behavior. Cache
hits do not establish an end-to-end QPS improvement. Full source/build provenance
and architecture-specific validation remain required before publishing claims.

## Validation results and limits

The native Release build uses GCC 14.2, Knowhere `faff72c4` and MilvusStorage
`15ab3d7`, including the real Go/native plan-parser bridge. The final source
passed 5,287 affected Milvus tests, 17 JSON numeric tests through `all_tests`,
and 349 native bitset tests. Checksums for all 141 changed C++/build files matched
between the local source and test machine.

Library-only ARM and x86 Release and ASan/UBSan checks passed 349 tests plus the
header-only check. An x86 TSAN smoke check covers concurrent read-only queries.

DiskANN and SVS were disabled. Performance tests and `SkipIndexPr51441.*` were
excluded; the latter fixtures require creating an absolute root directory that
the test user cannot write. The standalone `test_json_uint64` executable passes
its 17 assertions but aborts during process teardown, also with zero tests;
the abort trace reaches the MilvusStorage global finalizer. Those same 17 cases
pass through the initialized Milvus `all_tests` entry point with exit status 0.
The standalone exit failure remains a validation limitation.

These checks establish covered bitmap, expression and query behavior. They do
not measure end-to-end Milvus/ANN QPS or prove that every mutation workload is
faster. Statistics require reuse of a view descriptor, and owners and intervals
must remain alive; concurrent owner writes require external synchronization.
