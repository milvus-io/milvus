# 遗留测试：保留作参考，暂不删除

这 18 个 `*Test.cpp` 测的是阶段 1 已经退役的公开接口（`IndexBase`、`ScalarIndex<T>` 的 24 个 virtual、
`IndexFactory` 的巨型分派 switch、`InApplyFilter`/`InApplyCallback`、`HybridScalarIndex` 运行时转发类）。
它们**编译不过**——被测的头文件已经不存在了。

保留它们是为了在重写测试时参考已有行为覆盖。各索引类型仍需补齐往返一致性矩阵：
Builder → Artifact → Serialize → Loader → Reader，并比较原地打开与持久化后打开的查询结果、
null 语义和坐标域。接口说明见 [contracts/README.md](../../contracts/README.md)。
在对应索引类型的新测试写完之前，不要删这里的任何一个文件。
旧 `NgramParams`/`FMIndexParams` 仅供这些参考测试使用，定义现位于
`unittest/test_utils/LegacyIndexParams.h`，不再属于生产索引接口。

## 旧测试 → 新的索引类型目录

| 遗留测试 | 覆盖的旧类 | 新的所属组件 |
|---|---|---|
| `BitmapIndexTest.cpp` | `BitmapIndex` | `scalar/bitmap/` |
| `BitmapIndexArrayTest.cpp` | `BitmapIndex` + sort/string-sort 的 nested 路径 | `scalar/bitmap/`、`scalar/sort/`（元素级） |
| `BoolIndexTest.cpp` | `BoolIndex`（类型别名）、`ScalarIndexSort` | `scalar/bitmap/` |
| `ScalarIndexSortTest.cpp` | `ScalarIndexSort` | `scalar/sort/` |
| `StringIndexSortTest.cpp` | `StringIndexSort`（自带 pImpl 与带版本二进制格式） | `scalar/sort/` |
| `StringIndexTest.cpp` | `StringIndexMarisa` | `scalar/marisa/` |
| `InvertedIndexTest.cpp` | `InvertedIndexTantivy` | `scalar/inverted/` |
| `InvertedIndexArrayTest.cpp` | `InvertedIndexTantivy` 的 nested 路径 | `scalar/inverted/` |
| `NgramInvertedIndexTest.cpp` | `NgramInvertedIndex`（含已删除的 Phase2） | `scalar/ngram/` + exec 侧 refine |
| `TextMatchIndexTest.cpp` | `TextMatchIndex` 四个构造函数 | `scalar/text/` + `growing/` |
| `FMIndexTest.cpp` | `FMIndex`（含 `ShouldUseOp` 代价护栏） | `scalar/fmindex/` |
| `RTreeIndexTest.cpp` | `RTreeIndex` | `scalar/spatial/` |
| `RTreeIndexWrapperTest.cpp` | `RTreeIndexWrapper`（已按 build/query 两模式拆开） | `scalar/spatial/` |
| `JsonFlatIndexTest.cpp` | `JsonFlatIndex` | `scalar/json/` |
| `JsonIndexTest.cpp` | `JsonScalarIndexWrapper` | `scalar/json/` |
| `JsonPathIndexTest.cpp` | `JsonHybridScalarIndex` + path cast index | `scalar/json/`、`scalar/hybrid/` |
| `HybridScalarIndexTest.cpp` | `HybridScalarIndex` 运行时转发 | `scalar/hybrid/`——选型成为构建期决策，运行时不再有转发对象可测 |
| `ScalarIndexTest.cpp` | 跨索引类型：`ScalarIndex<T>` 的公共接口 | 无单一所属组件；按 contracts 中各查询接口拆分覆盖 |

## 重写时要注意的三处语义变化

1. **`HybridScalarIndexTest` 无法直接改写。** 旧测试断言的是"运行时按基数转发到 bitmap 或 inverted"，
   而 Hybrid 只在一次 `Build(input)` 内选择具体索引——测试对象变成"Builder 选型是否正确 + Loader 是否
   打开了选中的那一类"，断言点从运行时移到构建期产物元数据。
2. **`NgramInvertedIndexTest` 里覆盖 Phase2 的用例属于 exec，不属于索引。** 索引侧只需断言候选是超集
   （`ReaderCaps.exact == false`），精确验证的用例迁到 exec 的 refine 路径，与 geometry 使用相同的候选/验证分工。
3. **凡断言 `In`/`Range` 返回 bitmap 尺寸的用例，注意 `Count()` 现在是 reader 自身坐标系的基数**：
   元素级索引返回的是元素总数，不是行数。旧测试里隐含"尺寸 == 行数"的断言在 nested
   索引类型上不再成立。
