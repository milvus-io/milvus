# 读取器契约中的生产问题

源码快照：`SegcoreRefactor/6-tests-index`。未修改生产代码。获授权的 Release/关闭 ASan 运行执行了全部 7,960 个已注册 `index_tests`；日志和 XML 位于 `/tmp/segcore-index-test-run-20260914-230828`。运行时识别失败用例和 profile。以下精确错误行解释基于冻结数据、独立期望结果和生产源码，因为当前断言仅报告位图不相等而不输出 offset。

## Marisa 将字符串视图视为 C 字符串

**契约期望。** `ScalarPredicateReader<std::string_view>` 和 `PatternMatchReader` 接收携带长度的视图。精确比较、排序、LIKE、regex 和字面量模式操作保留每个字节，包括嵌入的 NUL。

**代表性覆盖。** `PredicateEdges` 包含不同的有效值 `"a"` 和 `std::string("a\0b", 3)`；标量用例覆盖成员关系、补集、相等、不等、区间边界和多批输入。`PatternBinaryNullable` 包含前导、尾随、重复和内部 NUL 值；23 个查询用例覆盖字面量、LIKE 和 regex 形式。`PredicateAllValid_AllValidPostfix` 也观察嵌入 NUL 之后的后缀。Marisa 对每个用例仍适用。

**生产源码。** `internal/core/src/index/scalar/marisa/MarisaIndexBuilder.cpp:42-44` 将 `LegacyCStringValue` 定义为 `\0` 之前的前缀，并在第 119 和 137 行插入 key 和分配行时应用它。`internal/core/src/index/scalar/marisa/MarisaIndexReader.cpp:59-61` 在第 412-431 行为精确和前缀 Lookup 重复该规范化；其他操作检查构建期间已合并的 key。

**观察到的及源码推导的行为。** 最终运行报告 74 个 Marisa 失败：24 个 Scalar 失败（heap 和 mmap 上的 12 个描述符）及 50 个 Pattern 失败（两个可空 profile 上的 23 个二进制描述符，另加全部四个 profile 上的 `AllValidPostfix`）。构建 `"a"` 和 `"a\0b"` 会将两行都分配给 `"a"`；Lookup 和排序无法保留后缀。相同的 heap/mmap 失败集合表明持久化不会改变缺陷。用例/profile 失败由运行时复现；其精确错误行由源码推导，而非测试输出。

## Inverted 标量关键词查询在 FFI 处省略字符串长度

**契约期望。** 对 `std::string_view` 的 In、NotIn、相等、不等和区间边界比较完整字节序列，包括嵌入的 NUL。

**代表性覆盖。** 相同的 `PredicateEdges` 值运行于显式嵌入 NUL 用例、全值成员关系/补集，以及适用的可空/不可空和 heap/mmap profile 上无 validity 的全有效用例。Inverted 仍适用并使用长度感知的期望结果。

**生产源码。** 构建保留长度：`internal/core/thirdparty/tantivy/tantivy-wrapper.h:351-364` 向 `tantivy_index_add_string` 传递 `data()` 和 `size()`。`InvertedIndexReader.cpp:33-36` 也拥有完整查询字节。查询 wrapper 随后仅发送 `.c_str()` 指针：term 位于 `tantivy-wrapper.h:825-833`，下界位于 891-893，上界位于 939-941，区间边界位于 998-1001。

**观察到的及源码推导的行为。** 最终运行报告预期 heap/mmap 及可空/不可空 profile 上的 18 个 Inverted VARCHAR 失败。对已存储 `"a\0b"` 的查询以 `"a"` 到达关键词 FFI，转而选中 `"a"` 行；补集和范围继承该不匹配。18 个用例/profile 失败由运行时复现。由于未输出 offset，精确错误行映射由源码推导。

## Inverted 浮点谓词区分负零和正零

**契约期望。** 标量 float/double 谓词遵循普通 C++ 比较语义，其中 `-0.0 == +0.0`。成员关系、相等、补集和有序范围因此必须将两种表示视为相等。

**代表性覆盖。** `PredicateEdges` 将 `-0.0` 和 `+0.0` 放在不同有效行，并执行全值/重复键成员关系、补集、相等/不等、大于等于、小于和闭合相等边界区间。`PredicateAllValid_InAllValuesWithoutValidity` 在两种可空性元数据布局中重复成员关系。

**生产源码。** `InvertedIndexBuilder.cpp:252-259` 将 Milvus FLOAT 和 DOUBLE 都映射为 Tantivy `f64`。`tantivy-wrapper.h:337-352` 和 `index_writer_c.rs:333-383` 扩展 FLOAT 并将两种类型存为 `f64`；查询 term 和边界在 `tantivy-wrapper.h:808-823`、882-887、930-935 和 988-994 处作为 `f64` 发送。选定的 Tantivy V5 依赖使用符号位感知的可排序编码（锁定修订 `bc211a5a76930b120b1eeb25073be27f20ba0387` 的 `common/src/lib.rs:96-102`），因此两个零相邻但为不同 term。

**观察到的及源码推导的行为。** 最终运行报告 52 个失败：26 个 FLOAT 和 26 个 DOUBLE，完全相同地分布在 heap/mmap 与可空/不可空 profile。正零查询遗漏负零行；负零相等仅选择该表示；范围边界分别排序两种表示。失败标识由运行时复现。由于未输出 offset，精确行差异由源码推导。鉴于 profile 相同，FLOAT 扩展和持久化不是独立原因。

## 无效 regex 被归类为 UnexpectedError

**契约期望。** 调用者提供的无效 regex 在查询调用处以 `SegcoreError` 失败。测试固定生产当前声明的精确 code；这不认可该分类，也不虚构替代 code。

**代表性覆盖。** `RegexBackreferenceRejected` 传入 RE2 无效的 `(a)\\1`。十个已实现的直接查询 profile 期望 `ErrorCode::UnexpectedError`；两个 FM profile 期望各自声明的 `ErrorCode::Unsupported`。驱动仅在 `PatternMatch` 周围捕获，因此构建/加载失败不能满足期望。

**生产源码。** `internal/core/src/common/RegexQuery.h:92-101` 构造 `PartialRegexMatcher`，检查 `re2_->ok()`，并在没有显式 code 的情况下调用 `AssertInfo`。其默认值为 `ErrorCode::UnexpectedError`。

**观察到的行为。** 全部 12 个错误用例均以声明的精确 code 在最终运行中通过。该运行时确认当前行为。由于格式错误的 regex 语法由请求内容导致，`UnexpectedError` 系统分类仍是生产分类问题。

这些发现未使用生产实现修复、测试跳过、后端排除或期望结果规范化。
