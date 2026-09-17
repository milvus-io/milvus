# Marisa/FM 产物契约覆盖

仅源码快照，2026-09-15。固定 master `a876f471053edb2f68a06a9894afee9810ea7906`；PR #64 `b7cfc84da8`，PR #65 `f672fd5967`。集中式首次构建门禁已关闭。

## MarisaIndexArtifactTest.cpp

16 个独立 GTest 用例/参数：

- V1/V2 `LegacyRoundTripRebuildsCsr`：序列化输出仅有 trie 和 row ID；加载器重建重复/空值 CSR 状态，且 mmap 读取器在 Artifact 和 NamedBufferSet 销毁后仍可查询。
- V3 `V3MmapReaderOwnsPersistedState`：发布 trie、row ID、CSR index/offset 和两个 CSR 元数据字段；mmap 读取器在 Artifact、source 对象和序列化 map 销毁后仍可查询，并报告文件支持的字节数。
- 必需条目：缺失 trie 与缺失 row ID 分别为独立参数。
- row ID 损坏：非整数倍字节长度和越界 key ID 分别为独立用例。
- 非空的损坏 trie 载荷会到达引擎加载器，并以精确的 `DataFormatBroken` 拒绝。
- CSR 完整性：缺失 index、offset、version 和 key count 分别为四个独立参数。
- CSR 元数据关系：未知 version 及其与 trie key count 不一致。
- CSR 字节：截断的 index、截断的 offset，以及表面长度正常但指向行域外的 offset。

固定 master 的 `StringIndexMarisaTest.Codec`、`UnifiedCodecRecreatesMissingLocalChunkDir`、可空 `In`/`IsNull` 和 mmap 重载行为映射至这些往返/所有权用例。宽泛查询表仍位于 ScalarPredicateReaderTest/PatternMatchReaderTest/ScalarValueReaderTest/NullReaderTest。

## FmIndexArtifactTest.cpp

16 independent GTest cases/parameters:

- V1/V2 序列化以精确的 `UnexpectedError` 拒绝。
- V3 heap 和 mmap profile 独立发布 blob/null bitmap/行计数/可空性，在 Artifact/source/序列化 map 销毁后仍可查询，并保留精确的空值/前缀结果；mmap 报告文件支持的字节数。
- 一个非空 V3 profile 独立发布不含空值 bitmap 的 `nullable=false`，重开后保留全有效的空值/查询结果。
- 必需 V3 部分：缺失 blob、行计数元数据和可空元数据。
- 行元数据：负行计数及行计数与 blob 文档计数不一致。
- 可空元数据：非布尔值及运行时/产物不一致。
- 损坏的 FM blob。
- 空值位图关系：可空产物缺少 bitmap、不可空产物包含 bitmap、打包长度错误以及 padding 位被置位。

固定 master 的 `SerializeLoadRoundTripMmap`、`SerializeLoadRoundTripNoMmap`、`NullVsEmptyStringDistinctAfterReload`、`LibraryLoadViewZeroCopyAndLazyExtract` 和 `LibraryRejectsCorruptHeaderMetadata` 映射至这些公开 Artifact/Loader 用例及现有 reader/Null/Pattern 套件。私有库 codec 差分/fuzz 用例仍是专门的库测试，此处不重建。

两个文件仅复用 `ArtifactTestUtils.h`、中央 ReaderBackend 清单、生产 NamedBuffer V1/V2 及测试 V3 FileSink/FileSource。未复制私有 codec。每个失败期望均将精确 `SegcoreError` 检查限定在序列化或打开操作。

静态验证：clang-format dry-run 和 git diff 检查通过。编译与运行时仍等待集中式门禁。
