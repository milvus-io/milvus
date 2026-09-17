# IndexTestCase 集中验证磁盘清理记录

本记录只验收本轮集中运行产生的磁盘副作用。运行目录为
`/tmp/segcore-index-test-run-20260915-185344`。构建、列举、主测试和四个崩溃隔离进程均由
`framework_support` 执行；清理验收方未启动 `ninja` 或 `index_tests`。

## 检查边界

运行前记录以下限定范围的路径、类型、大小、mtime、inode、device 和 mode：

- 主测试专用 `main-tmp`；
- 四个隔离进程专用 `crash-0-tmp` 至 `crash-3-tmp`；
- `/tmp` 下已知索引和测试临时名前缀；
- 工作区根目录的 `core`、`core.*`；
- `/var/crash` 的直接子项。

没有扫描其他磁盘目录。`ulimit -c` 为 `0`，内核 `core_pattern` 指向 apport 管道；仍单独检查了工作区和 `/var/crash`，没有仅依赖该配置推断结果。

## 正常退出的自动清理

主进程执行 12,521 个测试，因普通 GTest 断言失败退出 1。进程退出后，
`main-tmp` 的子项数为 0。主进程创建的索引目录、mmap 文件、LocalDirectory 测试目录和分片暂存目录均未留在其专用临时根中。

这是正常退出路径的自动清理结果；未执行外部删除。

## SIGABRT 隔离进程

四个隔离进程都因已知 JSON 正则生产崩溃退出 134。SIGABRT 不执行正常栈展开，因此分别在进程结束后、任何删除之前记录目录内容。

| 隔离进程 | 测试后端与用例 | 清理前残留 | 外部清理 | 清理后 |
|---|---|---:|---|---:|
| crash-0 | `JsonFlatV7_JsonEmployees_RegexPattern` | 0 | 无 | 0 |
| crash-1 | `JsonFlatV7NonNull_JsonEmployees_RegexPattern` | 0 | 无 | 0 |
| crash-2 | `JsonFlatV7Mmap_JsonEmployees_RegexPattern` | `json_flat_Jxjfzi`：1 目录、10 文件、3051 字节 | 仅删除该目录 | 0 |
| crash-3 | `JsonFlatV7NonNullMmap_JsonEmployees_RegexPattern` | `json_flat_4G67ww`：1 目录、10 文件、3051 字节 | 仅删除该目录 | 0 |

两个 mmap 目录都严格位于各自隔离进程的专用 `TMPDIR`。清理前 manifest 记录了目录和每个 Tantivy 文件的完整路径、大小及 inode。验收方确认归属后，由运行方作为单一清理者仅删除上述两个目录；最终快照确认四个隔离目录都没有子项。

这里区分两种结果：正常主进程由对象析构自动清理；崩溃进程无法依赖析构，mmap 配置的精确残留由进程外清理。后者不是自动清理通过。

## 专用目录之外

限定的 `/tmp` 前缀在运行前命中以下 10 个旧文件：

- `/tmp/inverted_nopch_gate.sh`
- `/tmp/marisa_nopch_gate.sh`
- `/tmp/ngram_b_prod.diff`
- `/tmp/ngram_evidence_display.txt`
- `/tmp/ngram_helpers_view.txt`
- `/tmp/ngram_nopch_gate.sh`
- `/tmp/ngram_old.cpp`
- `/tmp/ngram_old_test.cpp`
- `/tmp/ngram_setenv.log`
- `/tmp/text_nopch_gate.sh`

最终快照与基线相比没有新增、删除或元数据变化；这 10 个文件的 inode、大小、mtime、device 和 mode 均未改变。本轮没有触碰这些旧文件。

运行前后，工作区 `core`/`core.*` 和 `/var/crash` 都是 0 项，没有发现本轮 core dump 或 crash 文件。

## 保留的运行产物

`/tmp/segcore-index-test-run-20260915-185344` 本身是本轮指定的证据目录，以下内容按约定保留，不计为临时资源泄漏：

- `main.xml`、`main.log`、`main.status`、`main.time.log`；
- 四组 `crash-*.log`、`crash-*.status`、`crash-*.time.log`；
- 构建、配置和 GTest 列举日志；
- 测试名称、失败名称、命令、环境与源码哈希记录；
- 本报告使用的 cleanup manifests；
- 五个已经为空的专用 `TMPDIR` 根目录。

## 证据文件

- `cleanup/baseline.json`：运行前限定范围基线；
- `cleanup/main-post-exit.json`：主进程退出后的空目录证据；
- `crash-manifest-before-cleanup.json`：运行方记录的四个隔离进程清理前摘要；
- `cleanup/post-abort-before-cleanup.json`：独立记录的崩溃残留路径、大小与 inode；
- `cleanup/final-post-cleanup.json`：精确外部清理后的最终快照；
- `cleanup/cleanup-audit-draft.md`：检查范围和验收步骤工作记录。

最终状态：正常退出主进程自动清理完成；两个 mmap 崩溃进程需要并已完成精确外部清理；所有五个专用 `TMPDIR` 均为空；限定的外部前缀、core 位置和 `/var/crash` 没有本轮遗留。
