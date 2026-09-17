# 剩余存储/产物契约覆盖

日期：2026-09-15

证据基线：`08-remaining-lifecycle-contract-inventory.md` 中的 PR #64 生命周期清单，以及当前工作区中的生产
`FileSink`、`FileSource`、`LocalDirectory` 和 `LocalFileUtils` 接口及实现。

验证状态：仅源码检查、`git diff --check` 和 clang-format dry-run。全部计划源码任务完成前，集中式构建/测试门禁保持关闭。

## 源码矩阵

| 测试源码 | 普通 GTest | 契约表面 |
|---|---:|---|
| `storage/artifact/FileSinkTest.cpp` | 9 | `NamedBufferSink` generation, state, entries, borrowed files, stats, finish/take, unsupported and failed paths |
| `storage/artifact/FileSourceTest.cpp` | 16 | `NamedBufferSource` ownership, reads, public and independently specified slicing, local publication, preservation, collision and staging cleanup |
| `storage/artifact/LocalDirectoryTest.cpp` | 12 | `LocalDirectory` creation/ownership/cleanup plus local entry, mapping and descriptor guards |
| **总计** | **37** | 每个普通行为一个确定性 GTest |

所有文件系统用例通过 `LocalDirectory::CreateOwned` 在
`std::filesystem::temp_directory_path()` 下分配自有子目录。依赖的 source/guard 销毁后，所有者移除整个测试树。借用文件和目标路径均保留在该树内。

## NamedBufferSink

- `Gen()` 报告 V1/V2；成功 `Finish()` 前，`Data()` 和 `Take()` 拒绝访问。
- 空 finish 返回零序列化字节、无已发布文件记录和空 buffer set。`ReleaseLocalStaging()` 执行两次。
- 一个用例写入嵌入零的二进制条目、有效空条目和借用本地文件。它检查复制字节所有权、精确聚合 `MemSize`、空远程文件列表、借用文件保留、`Data` 和消耗式 `Take`。
- 已 finish 的 sink 拒绝后续写入和 finish 操作。
- raw-file 和有类型 V3 元数据操作返回精确 `Unsupported`，并使 sink 进入失败状态。
- 重复名称、非零长度的空数据和缺失借用路径以当前精确构造位置错误拒绝，并保持 sink 失败。

## NamedBufferSource

- 调用者的 `NamedBufferSet` 销毁后，source 保留共享所有权。检查有序名称、`HasEntry`、大小、嵌入零字节、有效空值、缺失元数据和 V1/V2 generation。
- 普通缺失逻辑条目返回精确 `DataFormatBroken`；非零长度的空物理所有者在复制前被拒绝；disk-engine 打开返回精确 `Unsupported`。
- 带作用域三字节 `FILE_SLICE_SIZE` 的公开 `NamedBufferSink` 生成物理 slice 和 `SLICE_META`；`NamedBufferSource` 重组精确逻辑值。第二个手动指定三 slice fixture 独立固定公开物理名称、有类型元数据值、逻辑长度和字节顺序，从而使 sink/source 错误不能相互抵消。畸形 JSON slice 元数据必须通过 exception 离开控制流，不能被接受。
- 单文件发布创建父目录并替换内容。缺失输入保留既有 target 并移除同目录 staging。
- 连接保留请求顺序、重复和空条目。空输入以空文件替换 target。较早 source 条目写入 staging 后的失败保留旧 target 并移除 staging。
- 目录物化按请求顺序返回路径、应用 basename 映射并原子替换既有 target。缺失的后续条目和 basename 冲突保留每个旧 target 并移除 staging。空请求创建请求目录但不含文件。

## LocalDirectory 和本地守卫

- 空父目录拒绝、父目录创建、精确模式路径、唯一自有子项、父目录保留、最后共享所有者递归清理，以及直接构造的未武装实例的非所有行为均为独立用例。
- `Owns` 接受既有和尚未创建的后代；在弱 canonicalization 后拒绝空、自身、父、同级前缀和 symlink escape。
- Heap 计数检查公开的对象加路径关系。
- 在真实文件上观察 `LocalEntryGuard` destructor、检查式移除、move construction、move assignment 对先前条目的清理和 `Release`。
- 在匿名 mapping 上观察 `MappedRegionGuard` move construction/assignment、对被替换 mapping 的清理和 `Release`。测试显式 unmap 已释放的 mapping。
- 在真实自有文件 descriptor 上观察 `FileDescriptorGuard::CloseChecked`，它保持关闭且不发生 destructor double-close。

## 有意边界

- Remote V1/V3 transport、cancellation、FileManager/ChunkManager 行为和 disk-vector handle 需要 #67 service 或 #66 vector 基础设施。
- 发布开始后的 commit 失败、allocator/OOM 行为、历史损坏格式和进程终止清理仍是专用故障机制。已覆盖普通发布前失败和可见 rollback 的目标保留。
- 命名缺失物理 slice 的 `SLICE_META` 记录会在 `Assemble` 中到达未检查的解引用。该损坏输入所请求的进程隔离断言受执行工具阻塞，未包含在内。静态证据和待处理的集中式诊断记录于 `25-storage-artifact-issues.md`；未添加期望、skip 或生产 workaround。
