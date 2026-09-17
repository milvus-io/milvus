# index_tests 清理验证

## 实测运行

最终源码清理运行使用隔离的临时根目录：

```bash
TMPDIR=/tmp/segcore-index-test-run-20260914-230828/cleanup-tmp \
  scripts/run_index_unittest.sh \
  --gtest_color=no \
  --gtest_output=xml:/tmp/segcore-index-test-run-20260914-230828/index-tests-cleanup.xml
```

命令因保留的 144 个生产结果失败仍存在而以 1 退出。它执行全部 7,960 个测试且未崩溃。GTest 报告 89.821 秒。从 XML 开始时间戳至 XML 最终修改时间为 90.019 秒，其中包括 XML 发布，是本次运行可用的 wall-clock 近似值。

`cleanup-isolated-before.txt` 为空。Inverted 用例处于活动状态时，`cleanup-isolated-during.txt` 捕获一个存活的 `inverted_b38G97` 目录。`cleanup-isolated-after.txt` 为空：进程退出后，隔离根目录包含零个子条目和零个测试创建字节。祖先目录本身作为证据有意保留，并占用一个 4,096 字节文件系统目录块。

## 副作用清单

全部后端 profile 均不设置 builder `local_dir` 和 loader `mmap_dir_path`。测试适配器通过 `std::filesystem::temp_directory_path()` 解析 mmap 请求，因此设置 `TMPDIR` 会将构建和加载暂存均路由到隔离根目录。

| 所有者 | 临时名称 | 清理所有者 |
|---|---|---|
| 内存产物适配器 | 物化条目旁的 `.milvus-index-test-XXXXXX`、`.milvus-index-test-backup-XXXXXX` | 暂存文件守卫；已提交条目保留在下方后端所有者内 |
| Bitmap loader | `bitmap_portable_XXXXXX`、`bitmap_frozen_XXXXXX` | unmap 后由 local-entry guard 或 mapped-reader 所有者清理 |
| Sorted loader | `sorted_heap_XXXXXX`、`sorted_mmap_XXXXXX` | unmap 后由 local-entry guard 或 mapped-reader 所有者清理 |
| Inverted builder/loader | `inverted_XXXXXX` 目录及所含 Tantivy 文件/null sidecar | `LocalDirectory` 共享所有者 |
| Marisa artifact/loader | `marisa_trie_XXXXXX` 和 `marisa_XXXXXX` 目录 | local-entry guard 和 `LocalDirectory` 共享所有者 |
| FM artifact/loader | `milvus-fmindex-XXXXXX`、`milvus-fmindex-load-XXXXXX` | unmap 后由产物临时文件守卫、暂存目录守卫或 mapped-reader 所有者清理 |
| Hybrid | 无单独路径；委托给 Bitmap、Sorted 或 Inverted | 所选 delegate 所有者 |
| GTest/运行 harness | XML 和全文日志 | `/tmp/segcore-index-test-run-20260914-230828` 下有意保留的证据文件 |
| Build | 对象、库、可执行文件、CMake 元数据 | 有意保留的既有 `cmake_build` 输出 |

运行后，`cleanup-known-patterns-after.txt` 在可访问 `/tmp` 的两层深度内不包含与清单匹配的后端路径。工作区没有新的 `core`/`core.*` 文件，`/var/crash` 下没有新条目。主机 core pattern 将崩溃导向 apport。拍摄运行前快照时，更早中止的列举实验也没有当前工作区 core 文件或匹配的 apport 条目。

宽泛 `/tmp` 名称比较找到一个外部路径 `/tmp/push-milvus-io.sh`。其修改时间为 23:55:01，早于测试 XML 开始时间 23:55:09.944；其名称未出现在仓库或测试源码中。它保持未触及。十个服务私有 `/tmp` 目录拒绝遍历；它们相同的前/后权限错误记录在 `cleanup-tmp-before-errors.log` 和 `cleanup-tmp-after-errors.log`。隔离 TMPDIR 加上显式已知模式扫描覆盖测试创建的索引数据；这不是关于不可访问的无关服务目录或主机所有可能写入的声明。

这证明实测运行的正常进程退出清理。后端析构器有意使用尽力而为且未检查的 `unlink`/`remove_all`；硬进程终止绕过 RAII，调用者提供的父目录不归其所有。未发现正常路径所有权缺口。

## 证据文件

- `cleanup-isolated-before.txt`, `cleanup-isolated-during.txt`, `cleanup-isolated-after.txt`
- `cleanup-tmp-before.txt`, `cleanup-tmp-after.txt`, `cleanup-tmp-new-paths.txt`, `cleanup-tmp-new-paths-external.txt`
- `cleanup-core-before.txt`, `cleanup-core-after.txt`, `cleanup-new-cores.txt`
- `cleanup-var-crash-before.txt`, `cleanup-var-crash-after.txt`, `cleanup-new-var-crash.txt`
- `cleanup-known-patterns-after.txt`
- `full-run-cleanup.log`, `index-tests-cleanup.xml`
