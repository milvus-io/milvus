# 标量和模式读取器框架覆盖

源码快照：`SegcoreRefactor/6-tests-index`，与 master commit `a876f471053edb2f68a06a9894afee9810ea7906` 对比。

源码矩阵之后进行了获授权的 Release 构建、GTest 列举和完整执行。运行时证据及剩余生产失败记录如下。

## 固定 master 基线

- [BitmapIndexTest.cpp](https://github.com/milvus-io/milvus/blob/a876f471053edb2f68a06a9894afee9810ea7906/internal/core/src/index/BitmapIndexTest.cpp)：有类型标量谓词、可空/不可空数据、batch 构建、高基数、mmap 请求、范围变体和字符串模式行为。
- [ScalarIndexSortTest.cpp](https://github.com/milvus-io/milvus/blob/a876f471053edb2f68a06a9894afee9810ea7906/internal/core/src/index/ScalarIndexSortTest.cpp)：已排序范围和成员查询及 mmap 计数。
- [InvertedIndexTest.cpp](https://github.com/milvus-io/milvus/blob/a876f471053edb2f68a06a9894afee9810ea7906/internal/core/src/index/InvertedIndexTest.cpp)：primitive/字符串谓词、可空布局、模式路由和 mmap 支持的 Tantivy 加载。
- [StringIndexTest.cpp](https://github.com/milvus-io/milvus/blob/a876f471053edb2f68a06a9894afee9810ea7906/internal/core/src/index/StringIndexTest.cpp)：Marisa 成员关系、补集、范围、前缀匹配、空值、持久化和 Lookup 行为。
- [FMIndexTest.cpp](https://github.com/milvus-io/milvus/blob/a876f471053edb2f68a06a9894afee9810ea7906/internal/core/src/index/FMIndexTest.cpp)：字面量 prefix/postfix/inner 查询、LIKE 行为、空和二进制模式、Unicode/random bytes、选择性路由、可空行和 mmap/non-mmap 加载。

## 后端配置

清单包含 136 个已命名配置。每个配置经由已命名 V3 条目和元数据往返 builder 产物，使用生产 selector 解析持久化族，随后通过生产 loader 注册表打开。

| 请求的族 | 类型 | 可空性 | 加载请求 | 配置 |
|---|---:|---:|---:|---:|
| Bitmap | bool, int8, int16, int32, int64, float, double, VARCHAR | nullable + non-nullable | memory + mmap | 32 |
| Sorted | bool, int8, int16, int32, int64, float, double, VARCHAR | nullable + non-nullable | memory + mmap | 32 |
| Inverted | bool, int8, int16, int32, int64, float, double, VARCHAR | nullable + non-nullable | memory + mmap | 32 |
| Hybrid | bool, int8, int16, int32, int64, float, double, VARCHAR | nullable + non-nullable | memory + mmap | 32 |
| Marisa | VARCHAR | nullable + non-nullable | memory + mmap | 4 |
| FM index | VARCHAR | nullable + non-nullable | memory + mmap | 4 |

总计为 68 个 memory 请求、68 个 mmap 请求、68 个可空布局和 68 个不可空布局。谓词选择见到 132 个配置，因为 FM 不声明标量谓词。模式选择见到六个族中的 24 个 VARCHAR 配置。

`requires_nullable` 是描述符元数据，在数据生成前解析。含空值数据集仅使用可空配置。全有效数据集同时使用可空和不可空元数据布局。执行检查描述符与生成的 validity bit 一致，并拒绝发送至不可空后端的空值行。

Mmap 通过 `LoadOptions.enable_mmap` 和 `LoadOptions.mmap_dir_path` 传递，而不是装饰性的 JSON 参数。物理存储仍遵循生产布局决策：

- Sorted、inverted、Marisa 和 FM loader 映射其非空 mmap 载荷。
- Bitmap 仅映射具有超过 500 个不同 key 的 Roaring 布局。高基数 int16/int32/int64/float/double/VARCHAR fixture 执行该路径。
- Bitmap bool 和 int8 无法超过 500 个不同值，因此其 mmap-request profile 覆盖生产 bitset/heap fallback。
- Hybrid 对少于 16 个不同值使用 bitmap，对高基数数值使用 sort、对高基数字符串使用 inverted。因此低基数 mmap 请求使用 bitmap heap delegate；高基数请求执行映射的 sort/inverted delegate。

Hybrid 使用持久化的 `INDEX_TYPE` selector；契约不按 family 分支。FM 配置使用 `fm_sa_sample_rate = 32`。

## 数据和输入形状

数据描述符是惰性的并拥有全部字节。`ScalarTestInput` 仅在生成后绑定 view，将 `vector<bool>` 复制到连续 `bool[]`，在 `string_view` 后保留自有字符串存储，并在每个 batch offset 创建打包 validity 子 view。

- 每种 primitive/VARCHAR 类型的 `PredicateEdges` 覆盖最小值/最大值、重复值、一个已存储但为空的值、有符号值、浮点 `-0.0/+0.0`、空/前缀相关字符串、嵌入 NUL、UTF-8 和一个 80 字节字符串。
- `PredicateAllValid` 传递缺失 validity view；`PredicateAllNull` 传递存在的全零位图；`PredicateSingleRow` 和 `PredicateAllEqual` 覆盖退化的正行计数。
- `PredicateEdgesMultiBatch` 对六行数据使用 `{2,1,3}`，对七行 float/double/VARCHAR 数据使用 `{2,1,4}`。`PredicateEdgesWithEmptyBatches` 对 int64 使用 `{0,2,0,1,3,0}`。
- `PredicateAllFalse` 补充全 true bool 相等 fixture。`PredicateFloatInfinities` 覆盖 float/double 的 `-inf` 和 `+inf`。
- `TenThousandHighCardinality` 有 10,000 行：200 个有界 int8 值、int16/int32/int64/float/double 各 2,000 个值以及 2,000 个不同字符串。
- 既有 `RepeatedNullable` 和 `HundredThousandRows` 名称仍可供 `ScalarValueReaderTest` 使用。
- `PatternStringsNullable` 具有冻结的 105 行 LIKE/regex/字面量语料库，仅第 10 行无效。
- `PatternBinaryNullable` 保留显式嵌入 NUL 长度，且有一个无效重复载荷。`PatternAllNull` 覆盖字面量和空模式空值屏蔽。
- `PatternSelective` 提供已知 rare/common/absent FM 路由输入；`PatternRandomBytes` 使用固定 seed `0xF3A1`；`PatternHighCardinality` 提供 1,000 个不同 `key_NNNN` 字符串并驱动 bitmap Roaring mmap 覆盖。

任一逻辑 bit 清除时，缺失 validity view 会被拒绝。总输入保持非空时，支持显式零大小 batch。

## 驱动与路由契约

每个后端 × 数据集 × 算子参数组合成为独立 GTest 参数。空后端选择是注册错误，以防新增类型/算子静默消失。

标量契约拥有 506 个语义用例描述符，经 `requires_nullable` 过滤后展开为 5,570 个独立参数。模式所有者最终完整 helper-source 清单拥有 207 个描述符，展开为 2,372 个参数：Prefix 24/280、Postfix 19/204、Inner 29/264、Match 89/1,080 和 Regex 46/544。最终计数包括该 helper-source 审计发现的九个额外普通 LIKE/错误描述符。契约合计提供 713 个语义描述符和 7,942 个独立命名的后端/数据集/参数参数。

驱动独立检查 row 坐标域、行计数、静态能力、接口 cast、期望位图大小和实际位图大小。期望结果为回调或契约自有的独立期望结果；不将后端族行为复制到期望结果中。

模式路由对每个后端操作声明一次：

- `UseAndRun`：guard 必须接受，并检查精确查询结果。
- `DeclineButRun`：guard 必须拒绝，但直接操作仍运行并检查其结果。
- `SelectiveAndRun`：普通结果用例始终运行；专用 rare/common/absent/empty fixture 还可断言期望 guard 值。
- `Unsupported`：guard 必须拒绝，且仅要求查询调用抛出精确 `ErrorCode::Unsupported` 的 `SegcoreError`。Build/load 错误不能满足该期望。

Inverted 将 Match/Prefix 声明为接受，将 Postfix/Inner/Regex 声明为拒绝但可运行。FM 将 Match/Regex 声明为不支持，字面量操作为选择性。Hybrid VARCHAR 保持精确结果覆盖，同时让依赖 delegate 的 Postfix/Inner/Regex 路由保持选择性。

`FilterCase::expected_error` 也表示无效 regex 等普通查询失败。仅 `Op::Run` 位于 catch 边界内，驱动要求具有声明精确 code 的 `SegcoreError`，错误用例在分配或检查期望结果位图前返回。不支持的后端操作以其后端策略的 `ErrorCode::Unsupported` 覆盖用例级 code。

## 族能力守卫与源码接线

同位置的 family 测试不经能力过滤，直接取得每个已命名配置：

- Bitmap、sorted、inverted 和 hybrid：全部八种类型 × nullable/non-nullable × memory/mmap 请求。hybrid guard 与 `HybridIndexBuilderTest.cpp` 同位置，因为 hybrid 是构建时 selector，且没有 `HybridIndexReader` 生产类。
- Marisa 和 FM：全部四种 VARCHAR 布局。

guard 断言生产能力承诺和 family 特定模式策略。`internal/core/unittest/CMakeLists.txt` 将两个契约套件、六个 family guard 和三个共享 support translation unit 接线至 `index_tests`。

内存中的 `FileSource` helper 满足本地物化发布承诺：单文件和连接文件使用同目录 staging 加原子替换；目录物化验证并读取全部条目，在发布前暂存所有文件、备份既有目标，并在失败时回滚。

## 运行时验证

既有 Ninja 构建配置为 Release 模式、`BUILD_UNIT_TEST=ON` 且关闭 ASan。两项测试/构建接线修正后，聚焦 `index_tests` target 成功构建：Lookup 参数名称 macro 现避免未加括号的 structured-binding 逗号，聚焦可执行文件使用自包含的 `libmilvus_core.so` 及其直接使用的 Folly shared library，而不重复 `milvus_core` 的公开静态依赖图。后者消除了首次列举尝试中出现的 duplicate-global teardown 崩溃。Inverted 和 Hybrid 测试 profile 现提供 builder 所需的 `field_id = 101` 元数据。

成功列举包含 7,960 个唯一测试：5,570 个 Scalar predicate 参数、2,372 个 Pattern 参数、六个既有 Lookup 参数和 12 个 family capability/selector guard。最终完整运行在 91.074 秒内执行相同 7,960 个测试且无进程崩溃：7,816 个通过、144 个失败，无跳过或禁用。每个 guard 和 Lookup 用例通过。

144 个结果失败是保留的生产发现：两个契约中 74 个 Marisa 嵌入 NUL 后果、18 个 Inverted 标量嵌入 NUL 查询失败和 52 个 Inverted float/double 符号零失败。Pattern 无效 regex 和不支持操作错误用例以其声明的精确 code 通过。完整日志和 XML 位于 `/tmp/segcore-index-test-run-20260914-230828`；最终运行是带 `index-tests-second.xml` 的 `full-run-2.log`。

## 有意延后项

- 总零行构建不属于读取器契约正向覆盖，因为 Bitmap 和 Sorted 在读取器存在前拒绝它。它归属 builder-contract 负向测试。已覆盖非空输入周围交错的零大小 batch。
- Artifact 损坏、缺失条目、cancellation、注入的发布失败和 remote V1/V2 slice 需要 persistence/failure harness。普通 helper 仍实现 FileSource 发布契约。
- Builder 输入销毁和 reader/artifact 生命周期销毁需要显式 lifetime fixture。
- Executor plan、多 chunk 执行、default-value/binlog 注入、nested 坐标和 candidate-domain 转换需要 consumer 或 end-to-end harness。
- 原始 FM 库内部、加载资源接纳计数和 performance/fuzz 机制独立于读取器查询语义。

这些延后项不从共享框架中遗漏普通 primitive、backend、可空性布局、batch 形状、直接谓词操作、模式操作、查询路由结果或 memory/mmap 加载请求。
