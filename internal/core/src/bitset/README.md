# Bitset ownership, read/write views and statistics

`Bitset<Policy, Container>` owns its storage and exposes writes.
`BitsetReadView<Policy>` (also named `BitsetView`) borrows a read-only interval: `data()` returns a
const pointer and indexing returns a bool, even for a non-const descriptor.
Read-only parameters can borrow an owner through the view constructor; bitmap
bytes are not copied. `BitsetWriteView<Policy>` borrows a fixed-size
writable interval. It cannot
resize, reserve, append or replace storage.

## Lifetime and statistics

A view is a live borrowed interval. Its buffer and referenced range must outlive
it. Resizing, reserving, clearing or replacing backing storage can invalidate
views. Move transfers an owner's buffer. Owned
results, shared cache entries and iterator backing storage remain owners.

Count and predicate caches belong only to each ReadView descriptor. Owners and
WriteViews always use the kernels directly; writes do not track or invalidate
ReadView caches. Copying a ReadView preserves known statistics, while creating
a subview starts an independent cache. Raw-buffer ReadViews follow the same
rules. The backing bits must remain stable during a read phase; create a fresh
ReadView after modifying the buffer.

A count result also answers all/none. A cold all/none keeps the early-exit
kernel rather than forcing a full population count. Concurrent read-only use
of a stable ReadView is supported; concurrent mutation requires external
synchronization.

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

## Milvus use

Bitmap columns retain owning bitsets. `GetBitmap()`/`GetValidBitmap()` return
read-only views by value. Bitmap columns expose no writable view or raw pointer.
Expression producers fill private owning buffers before publishing a column;
logical parents cannot modify child results. MVCC owns the final mutable query
mask, reads filter inputs and publishes a read-only result after its writes.
Scalar column resizing stays behind the column API.
Null counts derive from fresh validity ReadViews. Expression
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
