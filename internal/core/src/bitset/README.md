# Bitset ownership and read-only views

This interface change is staged on top of the dense-kernel optimization. It
changes the bitset library and its tests only. Milvus/segcore callers that write
through `TargetBitmapView` or `BitsetTypeView` need a separate migration before
this interface can be integrated into a full Milvus build.

## Storage types

| Type | Owns storage | Reads | Writes |
| --- | --- | --- | --- |
| `Bitset<Policy, Container, RangeCheck>` | Yes | Yes | Yes |
| `BitsetView<Policy, RangeCheck>` | No | Yes | No |

Both types keep the existing policy-based kernels. The split adds no virtual
calls, representation switches, bitmap copies, or statistics to a view.
`BitsetBase` shares read operations; `detail::BitsetMutatingBase` implements owner
writes. These CRTP helpers are implementation details, not additional storage
types.

A view returns `const data_type*` from `data()` and a `bool` from `operator[]`,
including when the view descriptor itself is non-const. It can be constructed
from a const owner or a const external buffer. It supports `size`, `empty`,
`count`, `all`, `any`, `none`, equality, `read`, `find_first`, `find_next`, and
nested `view` operations. Existing bit ordering, empty-range semantics and
policy-dependent storage requirements are unchanged. `empty()` means zero bits;
it does not mean zero set bits.

A view is a live borrowed range, not a frozen snapshot. Owner changes are visible
through existing views. Callers must keep the buffer and referenced range valid;
resize, reserve, clear, append, move assignment, storage extraction and owner
destruction may invalidate views. Concurrent reads and writes require external
synchronization. There is no lazy count cache in this change because the owner
can still mutate its bytes, including through `data()`.

## Writing a window

Source windows are read-only views. Write into the owning destination and pass its
bit offset as the last argument:

```cpp
auto src_window = src.view(src_begin, length);
dst.inplace_and(src_window, length, dst_begin);
dst.inplace_or(src_window, length, dst_begin);
dst.inplace_xor(src_window, length, dst_begin);
dst.inplace_sub(src_window, length, dst_begin);       // dst & ~src
dst.inplace_and_flip(src_window, length, dst_begin);  // ~(dst & src)

dst.set(dst_begin, length, true);
dst.reset(dst_begin, length);
dst.flip(dst_begin, length);
```

The destination interval is `[dst_begin, dst_begin + length)`. Source positions
are relative to the supplied source view. Adjacent bits are preserved, including
unaligned boundary bits. Existing calls without the final destination offset
still write from bit zero. `flip()` still flips the whole owner.

Multi-input AND/OR accept arrays of read-only views or owners, followed by the
number of inputs, length and optional destination offset. Counted AND/OR use the
same offset convention. Their existing return meanings are preserved:
`inplace_and_with_count` returns set bits in the destination interval;
`inplace_or_with_count` returns **unset** bits in that interval.

Column/value comparisons, range comparisons and arithmetic comparisons also
accept the optional final destination offset, for both runtime dispatch and
compile-time operation parameters:

```cpp
dst.inplace_compare_val(values, length, threshold, CompareOpType::GT, dst_begin);
dst.template inplace_compare_val<int32_t, CompareOpType::GT>(
    values, length, threshold, dst_begin);
```

The multi-input AND/OR fast path now requires the destination to be aligned as
well as all sources. An unaligned destination with aligned sources uses the
existing general path to preserve bit positions and adjacent bits. The kernels'
aliasing and traversal behavior remains unchanged. This
interface does not introduce snapshot semantics for overlapping binary operands.
External writable buffers need an owning destination in the subsequent caller
migration; a view cannot write to them.

## Library-only verification

The standalone entry point builds the actual library kernels and the existing
bitset tests without Milvus/segcore dependencies. GoogleTest must already be
installed; no dependency downloads occur.

```sh
cmake -S internal/core/src/bitset/tests -B /tmp/bitset-tests \
  -DCMAKE_BUILD_TYPE=Release -DGTEST_PREFIX=/path/to/gtest
cmake --build /tmp/bitset-tests -j 6
ctest --test-dir /tmp/bitset-tests --output-on-failure
```

For ASan/UBSan, use a separate build directory and add `-DSANITIZE=ON` and
`-DSANITIZER_TEST_O1=ON`. The latter reduces compiler memory usage for the large
template-heavy test translation unit; it does not reduce optimization of library
kernels. Tests cover the read-only API at compile time, owner writes with source
and destination offsets, preserved adjacent bits, empty ranges, runtime and
compile-time comparison dispatch, existing alias regressions, and header-only
use.
