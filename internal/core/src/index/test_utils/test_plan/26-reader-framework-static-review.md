# 共享标量契约框架静态审查

Scope: `ScalarTestData.h`, `FilterTestDriver.h`, `ScalarReaderFactory.{h,cpp}`, `ScalarReaderBackends.cpp`, and one level of production build/load/query dependencies. Review followed `~/.claude/CODE_REVIEW_GUIDE.md`. No configuration, compilation, test, or Milvus binary was run.

## 审查期间已解决

- The first inspected `MemoryArtifactSource` wrote local destinations directly, contrary to the atomic publication contract in `storage/artifact/FileSource.h:73-76,87-91`. The current snapshot fixes single/concatenated publication through `WriteLocalFile` (`ScalarReaderFactory.cpp:254-259,379-403`) and stages every directory entry before `CommitAll` (`ScalarReaderFactory.cpp:405-433`). `CommitAll` backs up existing targets, restores backups or removes new targets after a later publication failure, and leaves unpublished staged files armed for destructor cleanup (`ScalarReaderFactory.cpp:171-228`). No current finding remains on this path.

## 最终静态快照

- The frozen catalog contains 136 named specs: bitmap/sort/inverted/hybrid each register eight primitive/VARCHAR types across nullable/non-nullable and heap/mmap profiles (`ScalarReaderBackends.cpp:31-110,115-127`); Marisa and FM each register four VARCHAR profiles (`ScalarReaderBackends.cpp:129-187`). Null-bearing descriptors are filtered to nullable profiles at registration (`FilterTestDriver.h:164-179`; `ScalarReaderFactory.cpp:589-600`), and empty selection is an explicit registration error.
- Family guards resolve named configurations directly instead of passing through capability selection: bitmap/sort/inverted/hybrid cover all eight types and all four nullability/load suffixes (`BitmapIndexReaderTest.cpp:32-55`, `SortedIndexReaderTest.cpp:32-55`, `InvertedIndexReaderTest.cpp:29-52`, `HybridIndexBuilderTest.cpp:29-52`); VARCHAR policy guards cover all four profiles for inverted/hybrid/Marisa/FM (`InvertedIndexReaderTest.cpp:54-72`, `HybridIndexBuilderTest.cpp:54-72`, `MarisaIndexReaderTest.cpp:27-48`, `FmIndexReaderTest.cpp:27-47`). The hybrid source and suite use the current builder-owned name throughout.
- Mmap requests reach `LoadOptions.enable_mmap` and a concrete temp directory (`ScalarReaderFactory.cpp:522-530`). Bitmap bool/int8 cannot exceed the production greater-than-500-distinct Roaring threshold, so those named mmap-request profiles take the intended bitset/heap fallback; high-cardinality int16+ and VARCHAR descriptors exercise the physical bitmap mmap branch.
- Exact-error cases are isolated to `Op::Run`: the driver derives policy first, maps only `Unsupported` to `ErrorCode::Unsupported`, checks the exact code on `SegcoreError`, rejects other exception types, and returns before allocating ground truth or comparing a bitmap (`FilterTestDriver.h:115-159`). `expected_error` precedes `expected` in `FilterCase`; current error-case designated initializers follow declaration order and omit the later callback, while scalar descriptors do not set the error field (`FilterTestDriver.h:53-72`).

## 剩余验证限制

- Review is source-only. Constructor/template instantiation, static registration/link availability, and actual mmap behavior were not compiled or executed under the task prohibition.
- No current actionable framework finding remains after the publication fix. The scalar suite’s final statically inferred expansion is 5,570 parameters after dataset nullability filtering; pattern expansion is owned by its coverage audit.
