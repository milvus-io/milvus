# Marisa/FM 产物独立静态审查

日期：2026-09-15

范围：`scalar/marisa/MarisaIndexArtifactTest.cpp`、
`scalar/fmindex/FmIndexArtifactTest.cpp`、产物生命周期清单、当前公开的 Artifact/Loader 实现，以及适用的固定 master
序列化/加载行为。审查遵循
`~/.claude/CODE_REVIEW_GUIDE.md`。未运行构建或测试。

初始审查发现两个普通缺口，均已在审查的快照中修正：

- Marisa 现在损坏非空公开 trie 条目，并独立于 row ID 和 CSR 损坏断言精确
  `DataFormatBroken`；
- FM 现在具有正向非空 V3 形状/往返用例，断言
  `nullable=false`、不存在空值 bitmap、重开后的查询结果和零个空行。

其余 Marisa 用例覆盖 V1/V2 CSR 重建、V3 mmap 所有权、必需
trie/ID 条目、畸形 ID、全部四个不完整 CSR 部分、有类型元数据
关系和定长 CSR 损坏。其余 FM 用例覆盖精确
V1/V2 拒绝、可空 heap/mmap 所有权、必需 V3 部分、行和
可空元数据关系、损坏 blob，以及可空 bitmap 存在性、
长度和 padding。

最终源码计数为 16 个 Marisa 和 16 个 FM 独立 GTest
用例/参数。期望错误与当前 loader/serializer
构造位置相符，正向查询/空值结果与源码数据集相符。
未发现剩余可操作规格或代码质量问题。
集中式门禁开启前，运行时行为仍未验证。
