# int64 标量取值用例替代映射

日期：2026-09-15

本文将移除的六个
`ScalarValueInt64Test.LookupMatchesInput/*_RepeatedNullable` 参数映射到当前声明式
`ScalarValueReaderTest`。此映射未运行或修改任何测试。

## 语义映射

旧的 `RepeatedNullable<int64_t>` 输入为 `{10, 10, 30, 10}`，其中第 1 行无效
（`ScalarDataSets.cpp:588-595`）。它检查 `Lookup(offset)` 基于坐标：每个有效 offset
返回重复的有效值，即使其载荷等于有效行，无效 offset 也返回 `nullopt`。

当前套件以两个更强的数据集保留这两个维度：

- `PredicateEdges` is
  `{INT64_MIN, -1, 0, 1, INT64_MAX, INT64_MAX}`, with row 4 invalid
  (`ScalarDataSets.cpp:35-44`). `LookupMixedValidity` invokes `Lookup` for every
  offset (`ScalarValueReaderTest.cpp:46-64,206`) and distinguishes the invalid
  `INT64_MAX` row 4 from the valid equal-payload row 5.
- `PredicateAllEqual` contains four valid `1` values
  (`ScalarDataSets.cpp:146-151`). `LookupRepeatedValues` invokes `Lookup` for
  every offset (`ScalarValueReaderTest.cpp:46-64,209`) and verifies that every
  valid duplicate coordinate retains its value.

它们共同覆盖旧的混合有效性重复键行为，并新增整数端点及四坐标全有效重复分布。

## 逐参数替代

| 移除的参数 | 当前混合有效性替代项 | 当前重复有效值替代项 |
|---|---|---|
| `BitmapInt64_RepeatedNullable` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/BitmapInt64_PredicateEdges_LookupMixedValidity` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/BitmapInt64_PredicateAllEqual_LookupRepeatedValues` |
| `BitmapInt64Mmap_RepeatedNullable` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/BitmapInt64Mmap_PredicateEdges_LookupMixedValidity` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/BitmapInt64Mmap_PredicateAllEqual_LookupRepeatedValues` |
| `SortedInt64_RepeatedNullable` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/SortedInt64_PredicateEdges_LookupMixedValidity` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/SortedInt64_PredicateAllEqual_LookupRepeatedValues` |
| `SortedInt64Mmap_RepeatedNullable` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/SortedInt64Mmap_PredicateEdges_LookupMixedValidity` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/SortedInt64Mmap_PredicateAllEqual_LookupRepeatedValues` |
| `HybridInt64_RepeatedNullable` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/HybridInt64_PredicateEdges_LookupMixedValidity` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/HybridInt64_PredicateAllEqual_LookupRepeatedValues` |
| `HybridInt64Mmap_RepeatedNullable` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/HybridInt64Mmap_PredicateEdges_LookupMixedValidity` | `ScalarReaders/ScalarValueReaderTest.ObservesExpectedValues/HybridInt64Mmap_PredicateAllEqual_LookupRepeatedValues` |

## 列举与执行证据

六个混合有效性名称连续出现在
`/tmp/segcore-index-test-run-20260915-041720/current-test-names.txt:11993-11998`;
六个全相等名称出现在第 12017-12018、12021-12022 和 12025-12026 行。
`list-3.log:11669-11674` 独立列出混合有效性参数。

第一次完整运行 XML 将全部十二个替代参数记录为
`status="run" result="completed"` with no failure child:

- 混合有效性：
  `index-tests-full.xml:15686-15691`;
- 重复有效值：
  `index-tests-full.xml:15710-15711,15714-15715,15718-15719`.

移除的 `ScalarValueInt64Test.LookupMatchesInput` 套件未出现在当前列举中。上述映射通过相同六个后端/加载 profile 构成语义替代，而非重命名别名。
