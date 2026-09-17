# index_tests 清理证据

日期：2026-09-15  
日志根目录：`/tmp/segcore-index-test-run-20260915-041720`

## 范围限定的副作用清单

运行器获得隔离的 `TMPDIR`。测试框架和标量后端从 `std::filesystem::temp_directory_path()` 或由其填充的后端加载选项派生临时父目录。该 target 执行的源码可见名称包括：

- `inverted_*`, `marisa_trie_*`, `marisa_*`;
- `milvus-fmindex-*`, `milvus-fmindex-load-*`;
- `bitmap_portable_*`, `bitmap_frozen_*`, `sorted_heap_*`, `sorted_mmap_*`;
- `json_flat_*`, `json_projected_*`, `ngram_*`, `text_*`, `milvus-rtree-*`;
- `milvus-filesink-test-*`, `milvus-filesource-test-*`, `milvus-local-directory-test-*`;
- 同目录暂存和备份名称 `.milvus-index-test-*`、`.milvus-index-test-backup-*`、`.milvus-artifact-*` 和 `.milvus-artifact-backup-*`。

构建输出保留在既有的 `cmake_build` 树下。运行器日志、XML、状态文件、清单和摘要有意保留在日志根目录下（约 49 MB）。它们是预期输出，不是泄漏的后端数据。

## 实测清理

未过滤语义运行前（之后唯一的源码差异是空白格式化），`full-run-2-tmp` 包含 0 个条目。进程在可空 heap JsonFlat regex 用例上以 134 退出；之后隔离目录仍包含 0 个条目。

随后四个崩溃参数各自在独立隔离目录中运行：

| 进程 | Profile | 退出码 | abort 后立即的残留 | 最终残留 |
|---|---|---:|---|---:|
| `crash-0` | `JsonFlatV7` | 134 | 0 entries | 0 |
| `crash-1` | `JsonFlatV7NonNull` | 134 | 0 entries | 0 |
| `crash-2` | `JsonFlatV7Mmap` | 134 | 1 directory + 10 files, 3,051 file bytes | 0 |
| `crash-3` | `JsonFlatV7NonNullMmap` | 134 | 1 directory + 10 files, 3,051 file bytes | 0 |

mmap 清单为 `crash-2-leftovers-before-cleanup.txt` 和 `crash-3-leftovers-before-cleanup.txt`。每份包含一个具有 Tantivy 元数据、锁和 segment 文件的 `json_flat_*` 目录。SIGABRT 绕过 C++ 所有权析构器，因此这是异常终止残留。它不是自动清理成功的证据。保存清单后，运行器仅移除了两个专用崩溃目录的子项；`*-leftovers-after-cleanup.txt` 文件记录 0 个条目。未移除无关路径。

其余 12,520 个测试使用新建的 `full-run-remaining-tmp` 目录。完整的 175.73 秒运行后，它包含 0 个条目。这表明正常返回和普通 GTest 失败路径释放了所有观察到的后端文件和目录。

最终检查时，全部六个专用父目录均有意保留且为空：

- `full-run-2-tmp`;
- `full-run-remaining-tmp`;
- `crash-0-tmp` through `crash-3-tmp`.

## 已知外部根目录

前/后快照及其 diff 为：

- `cleanup2-prefix-before.txt`, `cleanup2-prefix-after-final.txt`, `cleanup2-prefix.diff`;
- `cleanup2-cwd-core-before.txt`, `cleanup2-cwd-core-after-final.txt`, `cleanup2-cwd-core.diff`;
- `cleanup2-var-crash-before.txt`, `cleanup2-var-crash-after-final.txt`, `cleanup2-var-crash.diff`.

三个 diff 文件均为 0 字节。已知 `/tmp` 前缀快照仅包含两个既有脚本 `/tmp/inverted_nopch_gate.sh` 和 `/tmp/marisa_nopch_gate.sh`，前后总计均为 3,640 字节。工作区前后均包含 0 个 `core`/`core.*` 文件。`/var/crash` 前后均包含 0 个条目。core dump 已禁用（`ulimit -c` 为 0）；系统 `core_pattern` 指向 apport。

更早中止的列举/构建实验没有专用的同期前快照。最终扫描未发现可归因于它们的 cwd core、`/var/crash` 条目或已知全局后端前缀集合变更。这是残留产物检查，不是基于 syscall 级全文件系统追踪的声明。

## 解释

正常执行清理了每个观察到的临时后端对象。两个 mmap 崩溃进程在采集清单后需要运行器显式清理，因为硬终止绕过 RAII。析构器也使用尽力而为的 unlink/remove 操作，因此此证据确立该 Release 运行和所列路径的行为；它不承诺每次可能的外部 kill 或文件系统失败后的清理。

源码确认的缺失 physical slice 构造问题仍记录于 `25-storage-artifact-issues.md`。其进程隔离测试被阻塞，未重试、执行或在此表示为已覆盖。
