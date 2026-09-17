# 任务 5 生命周期静态审查

快照：2026-09-15 源码阶段，固定 master
`a876f471053edb2f68a06a9894afee9810ea7906`。范围排除共享框架
文件（由模式所有者独立审查）、三个存储测试和 Marisa/FM 产物测试（在
`34-marisa-fm-artifact-static-review.md` 中独立审查）。未运行编译、测试列举或测试命令。

## 审查顺序和架构

diff 超过 300 行。按以下顺序阅读：

1. `contracts/RegistryTest.cpp`，用于注册表协议和 434 profile 生产注册矩阵。
2. `contracts/build/ArtifactBuilderTest.cpp` 和
   `ReaderConvertibleTest.cpp`，用于借用输入和转换所有权。
3. `scalar/hybrid/HybridIndexBuilderTest.cpp`，用于阈值选择、持久化 selector 和具体 loader 路由。
4. Bitmap、Sorted、Inverted、Text 和 RTree 产物测试，针对公开持久化布局和损坏边界。
5. `internal/core/unittest/CMakeLists.txt`，用于 `index_tests` 隔离。

测试使用中央后端/profile 注册表。Builder 用例在 Artifact 序列化和打开前销毁借用的 scalar/ARRAY 所有者。Family 测试使用生产 V1/V2 NamedBuffer 和共享 V3 source/sink adapter。

## 已关闭发现

- `ArtifactBuilderTest.cpp:40-77` 和中央 ARRAY 数据集现使用与非空批交错的零大小批。scalar 和 ARRAY 生命周期矩阵仍为 70 + 192 = 262 个独立参数。
- `ReaderConvertibleTest.cpp:183-207` 为 converter-error 和 null-result 结果提供单独 GTest，每个均证明 shell 销毁和零序列化。
- `BitmapIndexArtifactTest.cpp:135-160` 独立覆盖截断的打包 validity 和 row/nested 元数据不一致。
- `SortedIndexArtifactTest.cpp:126-213` 新增数值计数不一致、不完整和无效的数值 reverse-offset 状态、精确 `Unsupported` 的不支持字符串 version、无效字符串 reverse offset，以及既有截断字符串载荷用例。
- `InvertedIndexArtifactTest.cpp:75-158` 独立覆盖缺失和空 inventory、保留 inventory 条目、缺失文件、null-sidecar 关系、越界 null offset 和无效 engine 载荷。
- `HybridIndexBuilderTest.cpp:255-404` 暴露 13 个独立生命周期参数：数值 15/16/17、VARCHAR 15/16/17、ARRAY 15/16、nested 15/16、null delegate、不支持 selector 和不可转换 envelope。仅空值和重复值影响阈值 fixture，因此 selector 断言能检测任一者是否被计为新的不同值。V1/V2 和 V3 selector 均解析具体 loader，并执行安全结果查询。
- `RegistryTest.cpp:263-396` 对同一有类型注册表执行 64 个唯一注册和重复 `Create` 调用。其有界 condition-variable 握手保证注册中点前后各有一次 Lookup。两个线程体均捕获 exception，每个错误或 timeout 唤醒对端，主测试始终在报告前 join。唯一名称保证重复 GTest 运行安全。
- `internal/core/unittest/CMakeLists.txt:82-120,212-235` 将每个 contract、family 和 storage 源置于 `index_tests`，从 `all_tests` 移除这些测试文件，链接已构建的 `milvus_core` image 而不带其宽泛静态传递接口，并有一条 `index_tests` install 语句。

## 映射至其他位置的覆盖

- Task 2 在全部 64 个 nested profile 上执行 nested-element 输入生命周期和元数据；Task 5 不复制该矩阵。
- Text RAM 输入生命周期由 Text 观察运行器执行；`ReaderConvertibleTest` 证明真实转换能力和 tracking 依赖转移。
- RTree reader/source 生命周期由 SpatialReader 观察覆盖；产物文件聚焦 `.bgi`、行计数完成、null inventory 和缺失/损坏 archive。
- 每个 profile 的通用 V3 成功打开在 query/observation 套件中执行。聚焦产物文件检查公开布局关系和损坏，不复制查询矩阵。

## 最终静态计数和限制

Task 5 展开为 839 个 GTest：Registry 442、ArtifactBuilder 262、Consume 7、Bitmap/Sorted/Inverted/Hybrid/Text/RTree 59、Marisa/FM 32 以及 storage 37。这包含两个既有 Hybrid 能力守卫，净新增测试为 837 个。

已审查 C++ 文件通过 `clang-format --dry-run --Werror`，这些文件及 `index_tests` CMake 接线通过 `git diff --check`。本审查范围内不存在剩余可操作发现。集中式构建/运行门禁开启前，profile 注册、selector 结果、engine 解析和清理仍仅作静态验证。
