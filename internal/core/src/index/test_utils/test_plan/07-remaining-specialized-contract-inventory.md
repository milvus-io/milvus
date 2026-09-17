# 专用标量契约测试清单

日期：2026-09-15

证据：master `a876f471053edb2f68a06a9894afee9810ea7906`；PR64 `b7cfc84da8`；PR65 `f672fd5967`；集成 `af90d32547`；当前 `contracts/README.md`、`scalar/README.md`、query 头文件和系列源码。仅做只读清单。

## TextMatchReader / 文本

方法：精确的 Count 大小 MatchQuery(query,min_should_match)、PhraseMatchQuery(query,slop)、FuzzyMatchQuery(query,max_edit_distance)；Row 域；string ValueType；text_match caps；系列 NullReader。ScalarBuildInput<string_view> 必须跨 batch 保留全局 offsets，并区分 null 与有效空值。

数据集：中间含 null、有效空值和 2+1+2 batch 的英文 football/basketball/pingpang 语料；all-valid/all-null；Jieba 中文语料；长/Unicode/emoji/punctuation 语料。

独立手工用例：
- Match football/min1、nothing/min1、football+pingpang+cricket/min2、basketball+swimming/min1 和 min2。
- Phrase football/slop0、swimming-football/slop0、反向 football-swimming slop0/1/2、football-pingpang slop0/1。
- Fuzzy footbal distance0/1 和 fotbal distance1/2。
- Jieba 唯一前缀、公共后缀和完整短语。
- multi-batch/all-valid/all-null IsNull/IsNotNull；有效空值；空读取器/query；长/Unicode。

后端配置：直接消费的 Varchar RAM V7；持久化 V7 heap/mmap；持久化 V5 heap/mmap；聚焦 STRING/TEXT 类型；聚焦 Jieba RAM/持久化。Build 和 load analyzer 设置必须匹配。RAM 不能 serialize；V5 不能使用 RAM；nested 拒绝。

固定 master 直接映射：Index、FuzzyIndex、BuildIndexFromFieldDataMultiBatchNullable、BuildIndexFromFieldDataSingleBatchNullable、BuildIndexFromTextFieldData、SealedNaive/Nullable、SealedJieBa/Nullable。
框架/生命周期映射：WriterMemoryBudgets、RawSealedFinalizationMaterializesOnlyWhenNeeded、V2LoadFinalizesValidityBitmap、V2LoadSlicedNullOffsets、TranslatorResourceAccountsForValidityBitmap。
自定义暂缓：ParseJson 和 ParseTokenizerParams helper；Upload* build 服务；offset/growing/concurrency 用例（#66）；TextLob 服务输入；ExprResCache*（#67）。

## NgramReader / ngram

方法：caps `ngram_candidates=true/exact=false`；Row 域；Match/Regex/Inner/Prefix/Postfix 的 CanHandle；Candidates AND 合并到调用方位图；NullReader。CanHandle false 表示 fallback 且不查询。Scalar null/empty 与 JSON JsonProjectedString FieldNull/NoValue/Value 不同。

候选集断言策略：输出是初始位图子集；初始中的每个手工精确命中仍保留；选定固定用例比较精确候选位图，因此恒为 all 不能通过。精确 LIKE/regex 相等性不是默认要求，因为误报是契约允许的。

用例：
- master Wiki 表：ary、y S、y s、Sir、ool、secondary school、Sir Winston、Germany.、%Alv%y s%、%secondary%school%。
- Inner "secondary school" 的误报 "elementary school secondary"，以及不相交行。
- 前缀/后缀位置误报 abc/xabc/abcx。
- MatchersMustAgree：前缀 hello/test/app/ab；后缀 world/ing/ple/ab；内部 ello/test/app/aa/ab；Match hello%、%world、%ello%、test%ing、%aa%aa%、ab%ab、%ab%ab%。
- 重叠表 %aa%aa%、%aa%aa%aa%、%ab%ab%、%ab%ab%ab%、aa%aa、ab%ab、%abc%abc%。
- 带重音 Latin、中文、emoji；转义 %100\\%%、%file\\_%、%\\\\%；单字符与双中文字符的资格。
- Regex：无片段、短片段、一个可用片段、多片段反序误报。
- 仅通配符/空为 false；all/sparse/empty 初始 masks；null/all-null/valid-empty/multi-batch。
- JSON /a VARCHAR 投影三态、Resolve 到 NgramReader、Exists(Any)。

后端配置：Varchar heap/mmap min2-max4；聚焦 min3-max3；STRING/TEXT；JSON 投影 Ngram heap/mmap。仅当前 Tantivy generation。

固定 master 直接映射：TestNgramWikiEpisode、TestNgramSimple、TestNgramJson、MatchersMustAgree、OverlappingPatterns、UTF8Patterns、CanHandleLiteralUsesUtf8CharacterCount、EscapeSequences。
暂缓：两个 NonLike 测试是 exec 选择（无 Equal/In Ngram op）；两个 benchmark 是性能机制。

## SpatialReader / RTree

方法：caps `spatial=true/exact=false`；Row 域；权威 Count；GEOMETRY ValueType；全部 8 个 SpatialOps 的 Candidates；NullReader。当前头文件承诺与 op 无关的 MBR-intersection 候选超集。invalid/no-MBR 查询返回全部 non-null。

惰性 WKB 语料：两个点、正方形、重叠/不相交多边形、交叉线、null、截断 WKB、空 geometry、多 batch。

用例：为 Equals、Touches、Overlaps、Crosses、Contains、Intersects、Within、DWithin 分别提供手工 MBR 描述符；非平凡的包含/排除；Equals 误报多边形证据；master overlap/cross/contains/touches 粗粒度 fixture；invalid Geometry fallback；all-valid/all-null/multi-batch；invalid/empty WKB 保留 Count；空输入精确 DataIsEmpty。因 RTree fallback 到 heap，heap 和 mmap-request 后端配置均报告 `file_bytes=0`。Nested 拒绝。

固定 master 直接映射：Build_EmptyInput_ShouldThrow、Build_WithInvalidWKB_Upload_Load、Build_VariousGeometries、Build_BulkLoad_Nulls_And_BadWKB、两个 Query_* 测试的粗粒度部分、AddGeometryClassifiesNullByValidityNotPayload、wrapper BuildLoad/QueryOperations/InvalidWKB、EmptyGeometryIsIndexedWithoutUndefinedMBR、CountTracksCommittedRows。
框架/生命周期：Build_Upload_Load、filename/config/mixed-path/end-to-end/large/sliced-null 用例和 wrapper serialization failure 用例。
自定义暂缓：remote missing；GIS exact/split/legacy/corrupt refinement（#67）；bad_alloc 注入；growing/concurrency（#66）；mutable wrapper recovery/starvation。

## JsonIndexReader / JsonFlat 与投影 JSON

方法：Resolve(path,cast)、Exists(path,Any/Numeric/String/Bool)、CastTypesOf(path)。JsonFlat root 有 json_paths/exact caps、JSON 类型和字段 NullReader。拥有的 resolved view：bool Predicate<bool>；numeric Predicate<int64_t> 和 Predicate<double>；string Predicate<string_view> 加 PatternMatchReader；均有 comparable-value NullReader。JsonProjected 外层保留 inner caps 加 json_paths，不进行 sibling-cast，仅对精确 path/cast 返回 Borrowed inner，并将 Exists 与 cast validity 分别从 non-exist offsets 派生。

JsonFlat 数据：master employee 文档；scalar/array/object/empty-array/null/missing 类型系列语料；混合 -10、1、10、10.5、2^63、UINT64_MAX、string1、missing；nullable 字段；empty-root 和 /profile-root；转义 keys/numeric positional path；all-missing path；multi-batch。

用例：preferred-name Exists；object subpath 和 Any/Numeric/String/Bool；root field null 对比 comparable null；string In/NotIn/全部比较/区间和全部 PatternOps；bool In/NotIn/Range；int64/double In/NotIn/全部比较/区间形式和精度边界；array string/numeric any-element predicates；outside/sibling/numeric/malformed paths；unknown cast；支持的 absent path。

必须保留的生产失败：IJsonIndexReader.h 将 CastTypesOf 定义为受支持 cast 词汇表。JsonFlat 仅返回 {JSON}；Resolve(JSON) 为空，而 BOOL/DOUBLE/VARCHAR 和 ARRAY 元素 casts 可 Resolve。测试必须断言两个方向。不得规范化期望或跳过。

JsonProjected：DOUBLE/VARCHAR/BOOL 语料区分 valid、missing、JSON null、cast failure 和 field null；exact/wrong path/cast；exact vocabulary；multi-batch。每个 adapter 的系列：DOUBLE sort/inverted/hybrid；VARCHAR sort/bitmap/inverted/hybrid/ngram；BOOL bitmap/inverted/hybrid。ARRAY_BOOL/DOUBLE/VARCHAR 使用 ArrayView row-domain builders 和代表性 predicates。Heap/mmap。

固定 master 直接映射：所有 JsonFlat 直接 In/Exists/type-family/null/NotIn/Range/Pattern/bool/int/mixed-numeric/array 测试；ExecutorReusesMaterializedFieldValidity 的 query 语义；JsonPath SortDouble、BitmapVarchar、Exists、ComparisonUnknowns、BitmapBool、Hybrid Exists/Comparison；JsonIndex Cast/Contains 语义；投影数据集表示的 ConvertDouble/Varchar 期望结果。
框架/生命周期：Hybrid selection/cardinality、5 个 Factory_* 测试、resource-release 机制。
暂缓：3 个 sliced-offset 测试；已退役 InApply/InApplyCallback/Query；JsonFlat Expr/Contains 组（#67），保留直接真值表。

## 最小共享 API/配置需求

使用独立的声明式专用后端配置，而非修改原始 BackendCatalog keys：

- 从 ScalarReaderFactory 提取可复用的 TestArtifactData/TestArtifactSink/TestArtifactSource V3 IO；
- 包含 family、build_params、load_family、load_params、enable_mmap、consume_artifact、惰性 temp-root 注入、按数据的 load-param 回调、可选 artifact wrapper 的 BuildOpenSpec<InputT>；
- ReaderBackend 公开 Build(input) 和 Open(artifact) 也适用于生命周期测试；
- candidate 断言留在 query 测试文件；精确 FilterCase 相等性不变；
- WKB、raw JSON、ArrayView 和 JsonProjectedString 的惰性 owner/adapter；
- 聚焦的 backend-name 选择。

精确配置形状：
- Text：field_type=value_type STRING/VARCHAR/TEXT，nested=false，FIELD_ID=101 或 unique_id，analyzer name/params；V7 的 scalar engine version 3 或 V5 的 1；disk build 获得惰性 local_dir。
- Ngram scalar：FIELD_ID=101，string field/value，nested=false，MIN_GRAM=2，MAX_GRAM=4，最新 Tantivy，惰性 local_dir。JSON Ngram 添加 field_type JSON、value_type VARCHAR、json_path=/a、json_cast_type=VARCHAR 和三态 input/wrapper。
- Spatial：string_view WKB 输入但 field/value 为 GEOMETRY，nested=false；load callback 添加 num_rows。
- JsonFlat：string_view raw JSON 输入但 field/value 为 JSON，json_cast_type=JSON，FIELD_ID=101，json_path root，nested=false，V5/V7。
- Projected scalar：适合 inner input 的 build params；load params field_type JSON/value element/path/cast；wrapper 持有 row_count/non_exist offsets。ARRAY build 使用 field_type ARRAY，随后是 JSON load metadata。

CMake：添加 TextMatchReaderTest.cpp、NgramReaderTest.cpp、SpatialReaderTest.cpp、JsonIndexReaderTest.cpp 及实际共享支持。Spatial 可能需显式 GEOS link，因为测试通过 inline GEOS 调用构造 Geometry。

归属：pattern agent 负责 Text/Ngram/Spatial；scalar agent 负责 JSON；framework agent 负责通用 factory/adapters/lifecycle/CMake。所有计划源码完成前，Build gate 保持关闭。
