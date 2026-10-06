# Read-only bitmap views and statistics in Milvus

Status: implemented; covered native suites pass, with validation limits below.

Related issue: #53669. Related PR: #53865.

Milvus's expression and MVCC pipeline previously wrote bitmap windows through
`milvus::bitset::BitsetView`. Read-only use and output mutation shared the same
type, so adding a count cache directly to the view could return stale results.
This change separates borrowed read and write permissions. Bitset owns the
statistics and their invalidation; views delegate reads and report writes.

## Lifecycle

1. A producer allocates owning data and validity bitsets, then fills them.
2. ColumnVector retains the owners instead of extracting their byte vectors.
3. Producers pass fixed-size WriteView windows to callbacks. Readers borrow
   ReadView windows, keeping the existing bit semantics for each stage.
4. Bitset caches population count and all/none for the complete bitmap.
   Views covering that interval reuse the cache; partial intervals are scanned.
   Owner and WriteView writes notify Bitset to clear its cached statistics.
5. SQL predicate conversion computes excluded rows as `~(data & valid)`, then
   resets validity. MVCC and sampling mutate owners.
6. ANN receives a read-only filter whose storage remains owned by the query or
   iterator result. Backend mappings and windows keep their own ID-domain counts.
7. Releasing an owner ends its borrowed views' lifetime. Shared result caches and
   asynchronous iterators must retain actual owners for the entire read period.

Bitset owns storage and exposes resize/reserve/append. ReadView borrows a read-only
interval and delegates statistics to its owner. WriteView borrows a writable
interval and cannot resize, reserve, append or replace storage. Both view types
borrow the same stable owner state. Neither view stores statistics or checks a
mutation version.
Existing BitsetView/TargetBitmapView names alias ReadView.
Scalar indexes that return newly allocated results continue returning owners;
a view cannot replace ownership of a temporary result.

## Interfaces

```cpp
auto read = bitmap.read_view(begin, length);
auto write = bitmap.write_view(begin, length);
write[i] = true;
write.inplace_and(other.read_view(other_begin, length), length);
```

Only owners and WriteView can produce a write window. ReadView and const owners
cannot be converted to WriteView. ReadView is a live read-only alias, not a frozen
snapshot: later owner/WriteView writes change its contents and clear the owner's
statistics. Column getters retain persistent ReadView descriptors and return
WriteView by value via `GetBitmapWriteView()` and `GetValidBitmapWriteView()`.

## Cache contract

The cached facts are bitmap population count and all/none. They do not replace
expression result caching, its admission/eviction policy, or query decision
flags such as all-rows-visible.

The owner stores statistics and escape/write-scope metadata with an address that
moves alongside its buffer. All supported writes notify this state, which
clears known statistics. ReadView stores only data, offset, size and a borrowed
owner-state pointer. It has no cached facts, generation checks or invalidation
logic. Both view descriptors are trivially copyable.

The owner and complete-interval views share one cache, even across fresh view
descriptors. Partial views and raw-buffer views use the existing scan kernels;
they do not reuse a complete bitmap count or retain separate range caches. This
avoids introducing a range-cache table and its allocation/synchronization costs.

Cold all/none use their existing early-exit scans. Caching them does not force a
full count. When count is known, all/none are derived from it. A true all/none
result also establishes count as the interval size or zero.

Predicate conversion checks validity before mutating output, so an all-valid
input can reuse its owner statistics. ColumnVector AllTrue/AllFalse retain
fused data/validity word checks and early termination; their pointers come
from ReadView accessors and do not mark the owner as escaped.

Writable raw pointer escape disables caching because later pointer writes cannot
be observed. A raw-buffer view has no invalidation source and remains uncached.
Native kernels may use a scoped write pointer that must not outlive its scope.
Raw pointers reference base storage, so callers must honor the view bit offset
and interval. The typed kernels apply these offsets automatically.

Tantivy result callbacks use a scoped raw pointer rather than escaping the
owner buffer. Empty callbacks leave statistics intact. PK range writers and
the MVCC deletion-list query loop open a batch scope around fixed-size writes.

A write scope clears the owner's statistics on entry and keeps reads uncached
while editing, including exception unwinding. Nested scopes on one owner are
supported. Per-bit proxies remain safe when retained and assigned later. Owner
mutation and read access require external synchronization. Atomic cache fields permit
concurrent read-only use, including the first cache computation.

## Window and ID domains

Both view types index their own interval starting at zero. Output callbacks receive
independent, bounded WriteView windows for result and validity. The producer cuts
these windows before invoking a callback, so macros and kernels use local indices.
All writes
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

The library tests must verify shared owner-cache reuse and invalidation across
every mutation family, retained proxies, raw pointer escape, owner move,
independently scanned windows, nested scopes, exception exit and concurrent
readers. Integration tests must
cover SQL three-valued logic, validity masking, sequential/scattered expression
batches, MVCC snapshots/TTL, array-element folding, scorer ID domains, serializer
windows and iterator buffer lifetime.

Performance evidence should separately measure first statistics computation,
repeated statistics, mutation/build overhead and overall query behavior. Cache
hits do not establish an end-to-end QPS improvement. Full source/build provenance
and architecture-specific validation remain required before publishing claims.

## Validation results and limits

The native Release build uses GCC 14.2, Knowhere `16e873b1`, MilvusStorage
`15ab3d7`, simdjson 5.0.2 and milvus-common `1.0.0-45fca32`, including the
real Go/native plan-parser bridge. At `0abf10de98`, after merging master
`243ebd872b`, all
three targets (`all_tests`, `bitset_test`, and `json_stats_test`) built.

The affected Milvus selection ran 5,425 tests from 150 suites: 5,423 passed and
two growing-snapshot cases were skipped for inapplicable sealed-cache backend
parameters. The selection includes all 17 JSON numeric cases, the warmed-statistics
predicate regression, fused ColumnVector predicates, vector NULL filtering,
independent cache encoding and packed-window ownership. All 354 native bitset tests
and all 118 standalone JSON stats tests also passed. Each test process and the
complete validation driver exited 0. Checksums for all 146 changed C++/build files
matched between local source and the test machine. The CI clang-format 15 script
was idempotent and `git diff --check` passed.

The subsequent batch-write follow-up rebuilt `all_tests` and `json_stats_test`.
Its PK/MVCC/callback selection passed 191 tests with exit status 0. The direct
text/inverted/JSON index selection passed 130 tests and skipped two floating-point
array-equality parameter combinations. Two TEXT/LOB fixtures failed before reaching
bitmap callbacks because they try to create `/sealed_text_lob_index_*` and
`/growing_text_lob_index_*` directories without root-directory write permission;
that test process exited 1. The standalone JSON stats suite passed all 118 tests
with exit status 0. Local and native checksums matched all 148 changed C++/build files.

A native probe instrumented full-count kernel calls for the real sealed and
growing callbacks at one million and ten million bits. Two consecutive count reads
after a write now scan once, and a subsequent write invalidates that cached count.
An empty callback preserves it. This checks cache reuse, not query latency or QPS.
PK regression coverage reads statistics within the predicate callback, throws
mid-batch, then verifies subsequent writes and reads after stack unwinding.

The bitset library sources are unchanged from `7791e7c56c`. At that head,
ARM library-only Release and ASan/UBSan each passed 354 tests plus the C++17
header-only check. Cases cover nested writable windows, owner-statistics
invalidation, retained proxies and copied views, owner moves, resize/clear,
nested write scopes, raw pointer escape and unaligned kernels with preserved
neighboring bits. New cases verify shared statistics across owners and fresh
complete-interval views, independently scanned partial intervals and trivially
copyable descriptors. An ARM64 size check with the uint64 element-wise policy
measured ReadView at 32 bytes, down from 56 bytes at the preceding implementation
head; WriteView remains 32 bytes. This measures descriptor layout, not query QPS.

The previously attempted x86 TSAN concurrent-reader smoke binary could not start: ThreadSanitizer
reports an unexpected memory mapping. Disabling ASLR for that test process was
not permitted. The preceding-head TSAN result does not verify this redesign;
the normal and ASan/UBSan suites include concurrent read-only cases.

DiskANN and SVS were disabled. Performance tests and `SkipIndexPr51441.*` were
excluded; the latter fixtures require creating an absolute root directory that
the test user cannot write. At the preceding implementation head, the standalone
`test_json_uint64` executable was observed to abort during process teardown, even
with zero tests; its abort trace
reaches the MilvusStorage global finalizer. Those same 17 numeric cases pass
through the initialized Milvus `all_tests` entry point with exit status 0. The
standalone executable was not rerun after the dependency update; the current
numeric validation uses the initialized Milvus entry point.

These checks establish covered bitmap, expression and query behavior. They do not
measure end-to-end Milvus/ANN QPS or prove that every mutation workload is
faster. Owner statistics can be reused across fresh complete-interval views.
Owners and intervals must remain alive; concurrent owner writes require external
synchronization.
