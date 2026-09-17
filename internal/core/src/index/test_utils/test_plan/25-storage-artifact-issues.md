# 剩余存储/产物生产问题

Date: 2026-09-15

## 缺失物理 slice 可在 NamedBufferSource 构造期间终止进程

Status: source-confirmed; centralized runtime verification pending. Production
code is unchanged.

`NamedBufferSource::Impl` copies each supplied physical named buffer into a
`BinarySet` and immediately calls `milvus::Assemble` from its constructor
(`storage/artifact/FileSource.cpp`, current lines 1141-1153). `Assemble` trusts
the parsed `SLICE_META` shape, erases every declared `<name>_<ordinal>` entry,
and dereferences the returned pointer without checking it
(`common/Slice.cpp`, current lines 76-87). If one declared slice is absent,
`Erase` supplies no usable entry and the dereference occurs before
`NamedBufferSource::Impl::Entry` can classify ordinary missing entries as
`DataFormatBroken`.

The public `FileSource` contract describes logical entry reads and atomic local
publication but does not prescribe an exact error code for malformed slice
metadata. The required invariant for this corrupt transport input is therefore
a controlled rejection rather than a process crash; no ideal error code is
hard-coded here.

A process-isolated GTest was considered so this defect could remain visible
without ending the complete `index_tests` process. The execution tool blocked
that construction, and the user directed us not to retry or bypass the block.
The unsafe corrupt-slice case is consequently absent from source tests. Valid
small-slice assembly and ordinary missing logical entries remain covered in
`FileSourceTest.cpp`.
