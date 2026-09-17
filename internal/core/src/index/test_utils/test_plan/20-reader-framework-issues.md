# 读取器契约中的生产问题

对 `SegcoreRefactor/6-tests-index` 的仅源码审计。这些是读取器契约暴露的生产行为发现；本任务不修改生产代码。

## Marisa 截断嵌入的 NUL 字节

`ScalarPredicateReader<std::string_view>` 和 `PatternMatchReader` 接受携带长度的字符串视图，但 Marisa 将值规范化为第一个 NUL 之前的前缀：

- `internal/core/src/index/scalar/marisa/MarisaIndexBuilder.cpp:42-44`，在第 119 和 137 行构建时应用。
- `internal/core/src/index/scalar/marisa/MarisaIndexReader.cpp:58-60`，在第 413 行精确 Lookup 和第 424 行前缀 Lookup 时应用。

后果：

- 已索引的 `"a"` 和 `std::string("a\0b", 3)` 合并为同一个 key。
- 精确谓词查询和前缀查询无法保留调用者显式传入的长度。
- Lookup 无法恢复原始字节。

共享数据集保留两个值并使用长度感知的期望结果。Marisa 仍被注册，因此契约会暴露该缺陷。

## Inverted 字符串谓词 FFI 丢弃长度

`InvertedIndexReader` 首先以显式长度复制查询 `string_view` 值（`internal/core/src/index/scalar/inverted/InvertedIndexReader.cpp:33-36`，第 198-204、235-248 和 285-288 行）。Tantivy wrapper 随后仅传递 C 字符串指针：

- term：`internal/core/thirdparty/tantivy/tantivy-wrapper.h:826-833`
- 下界：第 890-895 行
- 上界：第 938-943 行
- 区间范围：第 997-1004 行

FFI 未接收长度数组，因此查询值中的嵌入 NUL 会被截断，尽管构建输入和 C++ 读取器契约都携带长度。Inverted 仍适用于 NUL 谓词用例，因此该不匹配可见。
