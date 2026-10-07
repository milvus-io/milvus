# Bitset ownership, read/write views and statistics

`Bitset<Policy, Container, RangeCheck>` owns its storage and exposes writes.
`BitsetReadView<Policy, RangeCheck>` (also named `BitsetView`) borrows a read-only interval: `data()` returns a
const pointer and indexing returns a bool, even for a non-const descriptor.
Read-only parameters can borrow an owner through the view constructor; bitmap
bytes are not copied. `BitsetWriteView<Policy, RangeCheck>` borrows a fixed-size
writable interval and reports writes to the owner state. It cannot
resize, reserve, append or replace storage.

## Lifetime and statistics

A view is a live borrowed interval. Its buffer and referenced range must outlive
it. Resizing, reserving, clearing or replacing backing storage can invalidate
views. Move transfers an owner's buffer and state together. Owned
results, shared cache entries and iterator backing storage remain owners.

Count and predicate caches belong to the owner state, with a stable address
across moves. ReadView and WriteView have no cache fields, version checks or
invalidation logic. An owner and all views covering its complete interval share
statistics, including newly created views. Partial intervals use the existing
range kernels without caching; they cannot reuse the whole bitmap count.

A count result also answers all/none. A cold all/none keeps the existing
early-exit kernel and caches the predicate result; it does not force a full
population count. A positive all/none additionally establishes count as
size/zero.

All supported owner and WriteView writes notify the owner state, which clears
its statistics. A retained bit proxy also reports writes. Cache updates use
atomic fields so concurrent read-only queries can share an owner through
independent views; mutation of the bitmap requires external
synchronization. This does not make concurrent read/write bitmap access safe.

Raw-buffer views remain uncached because no owner can report modifications.
Obtaining an owner's public writable `data()` permanently disables caching for
that backing buffer: a retained pointer may write at any later time. Read bytes
through a const owner or a view instead.

## Writing a window

Create a write window; indices and kernel offsets are local to that window:

```cpp
auto dst = bitmap.write_view(dst_begin, length);
auto src = source.read_view(src_begin, length);
dst.inplace_and(src, length);
dst.inplace_or(src, length);
dst.inplace_xor(src, length);
dst.inplace_sub(src, length);
dst.inplace_and_flip(src, length);
dst.set();
dst.reset();
dst.flip();
dst[i] = predicate(i);
```

The destination interval is `[dst_begin, dst_begin + length)`. Adjacent bits are
preserved. Only a mutable owner or WriteView can create a WriteView; a ReadView
and const owner cannot. `view()`/`read_view()` always produce read-only views;
`WriteView + offset` produces a writable suffix for existing window callbacks.
Owner kernels retain optional destination offsets for compatibility.

Multi-input AND/OR accept read-only views or owners. Their aligned fast path
requires both destination and sources to be aligned. Counted AND returns set
bits in the modified interval; counted OR returns **unset** bits there. Existing
kernel traversal behavior for overlapping operands is preserved.

For per-row output loops, use a scope to clear statistics once per batch:

```cpp
{
    auto write = dst.scoped_write();
    for (size_t i = 0; i < dst.size(); ++i) dst[i] = predicate(i);
}
```

Nested scopes are supported. Statistics remain uncached while a scope is open;
the owner clears older cached values when the outer scope opens. `write.data()`
permits scoped raw kernel writes. Its pointer must not escape the scope. The owner and
storage must outlive the scope, and must not be resized or replaced within it.

## Milvus use

Bitmap columns retain owning bitsets. `GetBitmap()`/`GetValidBitmap()` return
persistent read-only views; `GetBitmapWriteView()`/`GetValidBitmapWriteView()` return
fixed-size write windows. Column dimensions and storage identity stay behind the
column API. Scalar column resizing uses the column API.
Null counts derive from validity counts delegated to the owner. Expression
result cache ownership, admission and eviction remain in the query cache manager.

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
