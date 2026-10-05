# Bitset ownership, read-only views and statistics

`Bitset<Policy, Container, RangeCheck>` owns its storage and exposes writes.
`BitsetView<Policy, RangeCheck>` borrows a read-only interval: `data()` returns a
const pointer and indexing returns a bool, even for a non-const descriptor.
Read-only parameters can borrow an owner through the view constructor; bitmap
bytes are not copied.

## Lifetime and statistics

A view is a live borrowed interval. Its buffer and referenced range must outlive
it. Resizing, reserving, clearing or replacing backing storage can invalidate
views. Move transfers an owner's buffer and mutation state together. Owned
results, shared cache entries and iterator backing storage remain owners.

Count and predicate caches belong to the view. The owner holds only a mutation
version and write/escape metadata, with a stable address across moves. A count
result also answers all/none. A cold all/none keeps the existing early-exit
kernel and caches the predicate result; it does not force a full population
count. A positive all/none additionally establishes the count as size/zero.
Nested subviews have their own interval statistics. Copied descriptors can reuse
computed statistics, then invalidate them independently when the owner changes.

All supported owner writes invalidate view statistics. A retained bit proxy
also tracks writes. Cache updates use atomic fields so concurrent read-only
queries can share a view; mutation of the bitmap requires external
synchronization. This does not make concurrent read/write bitmap access safe.

Raw-buffer views remain uncached because no owner can report modifications.
Obtaining an owner's public writable `data()` permanently disables caching for
that backing buffer: a retained pointer may write at any later time. Read bytes
through a const owner or a view instead.

## Writing a window

Write into the owner, and pass the destination bit offset as the final argument:

```cpp
dst.inplace_and(src.view(src_begin, length), length, dst_begin);
dst.inplace_or(src.view(src_begin, length), length, dst_begin);
dst.inplace_xor(src.view(src_begin, length), length, dst_begin);
dst.inplace_sub(src.view(src_begin, length), length, dst_begin);
dst.inplace_and_flip(src.view(src_begin, length), length, dst_begin);
dst.set(dst_begin, length, true);
dst.reset(dst_begin, length);
dst.flip(dst_begin, length);
```

The destination interval is `[dst_begin, dst_begin + length)`. Adjacent bits are
preserved. Existing zero-offset owner calls and whole-owner `flip()` remain
available. Comparisons and arithmetic comparisons accept the same optional final
destination offset, for runtime and compile-time operation selection.

Multi-input AND/OR accept read-only views or owners. Their aligned fast path
requires both destination and sources to be aligned. Counted AND returns set
bits in the modified interval; counted OR returns **unset** bits there. Existing
kernel traversal behavior for overlapping operands is preserved.

For per-row output loops, use a scope to invalidate once per batch:

```cpp
{
    auto write = dst.scoped_write();
    for (size_t i = 0; i < length; ++i) dst[dst_begin + i] = predicate(i);
}
```

Nested scopes are supported. Statistics remain uncached while a scope is open;
closing the outer scope invalidates older cached values. `write.data()` permits
scoped raw kernel writes. Its pointer must not escape the scope. The owner and
storage must outlive the scope, and must not be resized or replaced within it.

## Milvus use

Bitmap columns retain owning bitsets. `GetBitmap()`/`GetValidBitmap()` return
persistent read-only views; `GetMutableBitmap()`/`GetMutableValidBitmap()` return
owners for bit writes. Column dimensions and storage identity must not be changed
through the mutable references. Scalar column resizing uses the column API.
Null counts derive from validity view counts. Expression result cache ownership,
admission and eviction remain in the query cache manager.

The search-side `milvus::BitsetView`/Knowhere view uses a backend ID domain that
may include mappings, windows and validity filtering. Its prepared counters are
not interchangeable with a plain dense row view's population count. Position
based and offset based scorer APIs remain distinct.

## Library-only verification

GoogleTest must already be installed. The standalone entry builds the actual
kernels, tests and a header-only smoke check without downloading dependencies:

```sh
cmake -S internal/core/src/bitset/tests -B /tmp/bitset-tests \
  -DCMAKE_BUILD_TYPE=Release -DGTEST_PREFIX=/path/to/gtest
cmake --build /tmp/bitset-tests -j 6
ctest --test-dir /tmp/bitset-tests --output-on-failure
```

Use a separate directory with `-DSANITIZE=ON -DSANITIZER_TEST_O1=ON` for
ASan/UBSan. O1 applies to the large test translation unit, not library kernels.
Library-only checks do not replace Milvus expression, MVCC and query tests.
