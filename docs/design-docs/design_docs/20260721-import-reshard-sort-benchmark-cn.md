# Import Reshard 进程级 Packed-Storage Benchmark 详细测试报告

本文是 2026-08-03 Import Reshard 排序实验的永久测试记录。Go 层源码 checkout
为 `upstream/master@6c8462cefb59b81b3332c622f80e384c3a6d6361`，测试覆盖真实
packed writer/reader、`storage.Sort`、当前 `storage.MergeSort`、one-head
cursor merge、独立进程正确性校验以及进程/cgroup 内存采样。

Native 动态库不是在该 checkout 中重新构建的，而是复用主 worktree 中较早的
Release 缓存产物。`libmilvus-storage.so` 和 `libmilvus_core.so` 都没有嵌入可供
本报告验证的准确源码 revision，二者均以 SHA-256 和 GNU build ID 作为权威
binary identity。对 storage library，artifact 时间、源码 checkout reflog 和
导出符号共同指向 `e658197` 是最可能的构建候选，但不能把候选写成已证明的精确
revision。Go wrapper 与缓存 core library 之间还需要一个不进入排序路径的 ABI
compatibility shim。因此本文不会把整个二进制栈笼统描述成“完全基于
`6c8462ce`”；详细边界见第 3 节。

测试完成并将本报告推送后，临时 harness、二进制、fixture、原始 JSON/CSV、
ABI shim、测试 worktree 和本地 benchmark 分支均按要求删除，不进入 Milvus
分支。本报告因此同时承担测试说明、环境快照、结果摘要和历史审计记录的职责；
文中的本地路径与命令记录的是当时实际环境，并不表示对应临时文件仍然存在。

| 项目 | 值 |
|---|---|
| 报告日期 | 2026-08-03 |
| Go 源码 checkout | `upstream/master@6c8462cefb59b81b3332c622f80e384c3a6d6361` |
| Native 来源 | 复用缓存的 Release `.so`；未与 Go checkout 重新做整栈构建 |
| 数据种子 | `20260721` |
| 保留 supervisor 报告 | 177，全部有效 |
| 保留 final 输出校验 | 105，全部逐行通过 |
| swap 增长样本 | 0 |
| 主要内存指标 | READY baseline 后的 active PSS 增量 |
| 交叉验证指标 | cgroup `memory.stat.anon` 增量 |
| 最终设计约束 | 首期 direct merge fan-in hard cap 为 16 |

## 1. 测试目标

测试比较四个阶段：

| 路径 | 输入 | 实际执行 |
|---|---|---|
| `sort-run` | 一个无序 packed fragment | `storage.Sort` → packed sorted run |
| `full-sort-final` | K 个无序 packed fragments | `storage.Sort` → packed final file |
| `current-merge-final` | K 个已排序 packed runs | 当前 `storage.MergeSort` → packed final file |
| `cursor-final` | 相同的 K 个已排序 packed runs | one-head-per-run cursor merge → packed final file |

`prepare` 使用真实 `storage.NewPackedRecordWriter` 生成 fixture。所有被测路径使用真实 `packed.NewPackedReader` 和 packed writer；`verify` 在独立进程重新打开 final file，逐行检查 PK、ordinal、payload、总行数和 checksum。

该测试比单元 benchmark 更接近实际存储流水线，但仍不是完整 Milvus 集群 e2e，也不包含 S3、调度、RPC、etcd 或 segment commit。

## 2. 隔离与内存口径

- 使用 `go build -tags dynamic -trimpath` 的优化构建，不使用 `testing.B`、`test` tag 或 `-gcflags='all=-N -l'`。
- 每个 stage 和每个样本使用新进程；fixture 生成和正确性校验不进入测量窗口。
- Worker 在可选 GC 完成后输出 `READY`，supervisor 保存 READY baseline，再发送 `GO`。
- 每个 worker 从进程启动起位于全新的、仅归属该样本的 fresh cgroup；这表示
  accounting 生命周期隔离，不表示 CPU、内存或 I/O 资源独占。内核
  `memory.peak` 在当前主机为只读，因此不重置；若结束时的 lifetime peak 大于
  READY peak，可确认新的精确高水位发生在 READY baseline 之后。
- `/proc/<pid>/status` 与 cgroup 每 5 ms 采样，`smaps_rollup` 每 50 ms 采样。
- 同时记录 PSS、RSS、RssAnon、RssFile、Anonymous、Private、Go heap、cgroup total/anon/file、CPU、fault、context switch 和 I/O。
- 任一 process 或 cgroup swap 指标在 active phase 增长，该样本作废。
- cgroup total 包含可回收 page cache；共享 file-backed pages 也可能计费在首次触页的其他 cgroup。结论必须同时查看 process PSS、cgroup anon/file 和 Go heap，不能把 `RSS - Go heap` 称为精确的 C++/cgo 内存。

READY baseline 到实际写入 `GO` 之间存在极窄的 supervisor 采样窗口，所以精确表述是“READY baseline 后至进程退出”，不是严格纳秒级的纯 GO-to-exit。

## 3. 环境

### 3.1 主机与操作系统

| 项目 | 实测值 |
|---|---|
| Host | `zilliz-hz` |
| OS | Ubuntu 22.04.5 LTS |
| Kernel | Linux `6.8.0-124-generic`，x86-64，`PREEMPT_DYNAMIC` |
| CPU | Intel Core i7-8700 @ 3.20 GHz |
| 拓扑 | 1 socket，6 physical cores，12 logical CPUs，2 threads/core |
| 频率范围 | 800–4600 MHz |
| Cache | L1d 192 KiB，L1i 192 KiB，L2 1.5 MiB，L3 12 MiB |
| CPU governor 审计快照 | `powersave`；未在每个样本中单独记录 |
| 物理内存 | 33,489,760,256 bytes，约 31.19 GiB |
| 可用内存 | 删除前主机审计快照约 9.4–10 GiB；不是逐样本起始值 |
| Swap | 2,147,479,552 bytes，约 2 GiB；审计时主机层面已占满，但所有保留 worker 的 swap 增量均为 0 |
| 数据文件系统 | `/dev/nvme0n1p3` 上的 ext4，`rw,relatime` |
| Block device | NVMe、non-rotational，scheduler `none`，read-ahead 128 KiB |
| cgroup | cgroup v2；父目录为 systemd user `app.slice` |
| 可委派 controller | benchmark 父目录启用 `memory`、`pids` |
| Page size | 4,096 bytes |
| Go | `go1.26.5 linux/amd64` |
| C/C++ compiler | GCC 11.4.0 |
| glibc | 2.35 |

cgroup 用于建立新生命周期的 memory accounting 边界，不提供独占 CPU core。
测试没有设置 `taskset`、cpuset、CPU governor 或 `cpu.max`，也没有停止主机后台
服务。执行顺序采用固定轮转来降低温度与背景负载的系统性偏差，但时间数据仍应
结合 min/max 范围解释。

测试没有显式设置 `GOMAXPROCS`、`GOGC` 或 `GOMEMLIMIT`，也没有为 fresh
cgroup 设置 `memory.max`、`cpu.max` 或 I/O hard limit。Fresh cgroup 用于资源
归属和生命周期峰值，不用于限流。

### 3.2 Checkout、构建模式与临时目录

- Benchmark worktree：`/home/zilliz/Code/milvus-reshard-e2e-bench`；
- Harness：`.benchmarks/import-reshard-e2e/`；
- benchmark 源码和所有输出都位于未跟踪的 `.benchmarks/`；
- Go module path：`github.com/milvus-io/milvus/.benchmarks/import-reshard-e2e`；
- `go version -m` 确认 `-tags=dynamic`、`-trimpath=true`、`CGO_ENABLED=1`、
  `GOAMD64=v1`；
- 未使用 `test` tag、`testing.B` 或 `-gcflags='all=-N -l'`；
- 复用主 worktree 的 Release native build、Conan cache、Go module/build cache；
- 没有改写主 worktree 的 native library，也没有把 benchmark code 提交到仓库。

Go module build info 还记录了
`github.com/apache/arrow/go/v17` →
`github.com/milvus-io/arrow/go/v17 v17.0.1` 的 replace。Milvus root module 与
`pkg/v3` 均显示为 local devel module；这也是为什么报告必须额外记录 checkout、
源码 hash 和 binary identity，而不能只依赖 `go version -m` 的 module version。

由于 harness 是 module 内的未跟踪 package，Go build info 没有写入
`vcs.revision`，fixture 的 `commit` 字段因此为 `unknown`。Go 源码基线由
worktree HEAD 和相关源码 hash 共同确认；benchmark binary、native libraries
及 shim 则分别由其 SHA-256 和 build ID 标识。不能从 fixture 的 `commit` 字段
或 Go checkout 反推缓存 native libraries 的源码 revision。

缓存 CMake 构建配置为 `Release`，C/C++ Release flags 均为 `-O3 -DNDEBUG`，
使用 bundled dependencies，并启用了 precompiled header。CMake 中
`MILLVUS_USE_CCACHE=ON` / `WITH_CCACHE=ON`，但
`CCACHE_FOUND=CCACHE_FOUND-NOTFOUND`，且审计时找不到 `ccache` executable，
所以不能声称该 native artifact 实际由 ccache 加速构建。该配置描述实际复用的
native artifact，但不消除 Go/native revision 不完全对齐这一复现限制。

关键源码身份：

| Source | SHA-256 / revision |
|---|---|
| `internal/storage/sort.go` | `3a709184977edcee8838d801e1e7ae9e18ebc466cc50ea1cf7a563b1b24b24f0` |
| `algorithms.go` | `329243137d019b73d062043e094b4307dc3b93351ebc102493122727f872a83e` |
| `main.go` | `f16fb6632ecdfb77ef4d168495e7b2255412866810e460d195b12a72cc0bb46e` |
| `metrics.go` | `ec70a1ffe01c4d99cce2aa918207a4d994c32f8edc9445d74d1a9dcbec57dfa0` |
| `model.go` | `8e778859799a697ad77628ad129a1c062eb8ffdf97ee337cfa8503c8e5f7664d` |
| `packed_io.go` | `c339b136178e5286cb314131a659e484c08810f1e0d92cd0f33d9b3121ac7381` |
| `stages.go` | `49c3c13d269145b4b05a1cfc4cf3cd663fc613c8657a06ccb5362916757c454f` |
| `supervisor.go` | `ae400f930e8ad5df099fbe5aabaf1f12a848e4b10c9a03ed9b1d92402e68254a` |
| `aggregate/main.go` | `bc042d0a70c3ee1d50706a8cb4abc22cb98bec6b923527c9293d1d7b4d5dc020` |
| `run-k32.sh` | `fde6a4e7a936a6f5098348b58a8a2bc6db1f6a83d21876651f88041f43c8510f` |
| `libmilvus-storage.so` exact source | 构建产物未嵌入 revision，不能精确证明 |
| milvus-storage probable candidate | `e658197151deeb29967ddb08d99ba7bf6365f0f9`，`enhance: upgrade Lance to 7.0.0 for newer storage format compatibility (#582)` |
| `libmilvus_core.so` source | 构建元数据未保存准确 revision；只能按下表的 binary identity 复核 |

不能把 source tree 在测试结束时的 HEAD 当成缓存 `.so` 的 build revision。删除
benchmark worktree 前后的补充审计给出以下证据：

- `libmilvus-storage.so` mtime 为 `2026-07-27 04:12:41.881675086 -0400`；
- milvus-storage source reflog 显示该 tree 从 `2026-07-20 08:44:11 -0400` 起位于
  `e658197`，到 `2026-07-28 00:11:56 -0400` 才 checkout 到 `f9f5bd1`；
- binary 仍导出 `ChunkReaderImpl::get_chunk_size`；
- `e658197` 源码使用 `get_chunk_size`，而 `f9f5bd1` 已将该 API 改成
  `get_chunk_estimated_size`。

因此该 binary 明确不可能由当前 `f9f5bd1` tree 构建，`e658197` 是与时间线和
符号都一致的最可能候选；但在没有 embedded revision 或原始 build log 的情况
下，报告仍将 exact source revision 标为 unknown。可复核的硬身份是下一节的
library size、SHA-256 和 GNU build ID。

### 3.3 关键产物身份

| 产物 | 大小 | SHA-256 | GNU build ID |
|---|---:|---|---|
| `reshard-e2e` | 157,256,624 bytes | `87ae83d9716a677f46558ef3ec8b94e480e9a8887f2dc2e86d6f916a2180eccb` | `3630784ba9f9123de3c3b5743ee0cb0de573db6e` |
| `aggregate` | 3,481,516 bytes | `c49977fb2469e534ea8212cf348739f298149bef96758d6219b011ec8bf5f426` | `d8171643ccdbaaca0b68e2fd38b87ad896ea106c` |
| shim source | — | `d680f6e4d2ba387949d8cfb5a575d909aefaaa01061ebab8aba18962a5d0f75f` | — |
| `libmilvus_bench_compat.so` | 15,176 bytes | `a4196267638791052f8a84945180d79cf7a4c6bd34d03458c2df628fcfea319d` | `9806472eaf3392d9913ed6f27416b5e7546feec3` |
| `libmilvus_core.so` | 362,845,088 bytes | `b04a2abd93e63b310376a7ed52afdbc8c0cc8f283e561c020165b49b4e345066` | `52eeb42f75dfa9ae6a4c155674d15489f53233dc` |
| `libmilvus-storage.so` | 446,062,640 bytes | `f83c69fccb72185d5e231f411210605a38fd24cce4c51602605f7682fe2a8faf` | `bad9d90c9b7d4c82a13cae7063e771c89449205d` |
| K=8/16 raw aggregate | — | `e32acab785837b5d43b5e802f6729c6029992213322fe58cb3d53dd6fdd66612` | — |
| K=8/16 summary aggregate | — | `b515b372469c472d7e03b708c5735bbab660579c9887926663434c0a649cc970` | — |
| K=32 raw aggregate | — | `ac262e032dbe50b1be30b65402c5cfdfaaa91e65a0a1228822fe98e0af90d905` | — |
| K=32 summary aggregate | — | `9dfd3505e17ccae5625e2f46ae41a913df7c3ef88ebcbf641a8b57ea209586e1` | — |

上表分组 aggregate 是运行过程中的阶段性输出。最终同时读取五个 scale 的
authoritative `aggregate-all` 生成于
`2026-08-03T13:38:26.228388225Z`，其四个文件为：

| 文件 | SHA-256 |
|---|---|
| `results/aggregate-all/raw.csv` | `f7feded41eeecda6e77cb01fad26c7634a76f193f25d8db4f20b1b4e2f89af84` |
| `results/aggregate-all/raw.json` | `7076fe46870967d8877be3a51e087adb2f89555ac31d3f9e1816f7d9941c1e29` |
| `results/aggregate-all/summary.csv` | `6a6be0f069748b1fa08582de357c47561833fdfe59587a6c371027b93ebb55bf` |
| `results/aggregate-all/summary.json` | `6d42076c2273893e00cb728f8a9b9ff7ed534ee3fcfe309d95db2e4eb18e4082` |

## 4. 构建

若复现环境已经有与 Go checkout 同步的 native build，可使用下面的简化命令；
它不是本次复用缓存 `.so` 时的完整历史命令：

```bash
cd /home/zilliz/Code/milvus-reshard-e2e-bench

BENCH=.benchmarks/import-reshard-e2e
mkdir -p "$BENCH/bin" "$BENCH/tmp"

source scripts/setenv.sh
go build -tags dynamic -trimpath \
  -o "$BENCH/bin/reshard-e2e" \
  ./$BENCH
```

本次运行的 worktree 没有重复构建 C++，而是通过 `PKG_CONFIG_PATH`、
`CGO_CFLAGS`、`CGO_CXXFLAGS`、`CGO_LDFLAGS`、`LIBRARY_PATH` 和
`LD_LIBRARY_PATH` 指向 `/home/zilliz/Code/milvus/cmake_build`。完整历史命令、
ABI shim 和动态库解析记录见附录。

## 5. Smoke 与正确性

```bash
BENCH=.benchmarks/import-reshard-e2e
BIN=$BENCH/bin/reshard-e2e
DATA=$BENCH/data/smoke-$(date +%s)

$BIN prepare \
  --work-dir "$DATA" \
  --runs 4 \
  --rows-per-run 4096 \
  --payload-bytes 128 \
  --batch-rows 512

for i in 0 1 2 3; do
  $BIN sort-run \
    --fixture "$DATA/fixture.json" \
    --run-index "$i" \
    --output-dir "$DATA/sorted"
done

$BIN full-sort-final \
  --fixture "$DATA/fixture.json" \
  --output-dir "$DATA/final-full"

$BIN current-merge-final \
  --fixture "$DATA/fixture.json" \
  --runs-dir "$DATA/sorted" \
  --output-dir "$DATA/final-current"

$BIN cursor-final \
  --fixture "$DATA/fixture.json" \
  --runs-dir "$DATA/sorted" \
  --output-dir "$DATA/final-cursor"

$BIN verify --final "$DATA/final-full/final.json"
$BIN verify --final "$DATA/final-current/final.json"
$BIN verify --final "$DATA/final-cursor/final.json"
```

Smoke 中 full sort、current MergeSort、cursor merge 的 16,384 行输出和 checksum 完全一致。

## 6. 规模矩阵与运行方式

固定参数：

```text
K = 8
Int64 primary key
VarChar payload = 1024 bytes/row
input batch = 4096 rows
reader buffer = 32 MiB
writer buffer = 32 MiB
output batch = 64 MiB
single column group
```

`K=8` 三档每 run 的目标 packed bytes 分别为 32 MiB、64 MiB 和 128 MiB，fixture packed bytes 为 255.65 MiB、511.69 MiB 和 1024.07 MiB。另补 `K=16`、每 run 约 64 MiB、fixture 1025.22 MiB，以及 `K=32`、每 run 约 32 MiB、fixture 1023.90 MiB 的 fan-in 定向矩阵。Merge 路径的 sorted-run physical bytes 分别为 254.95 MiB、509.84 MiB、1019.68 MiB、1019.60 MiB 和 1019.85 MiB；所有路径的逻辑行集与 payload 相同，但压缩后的物理输入 bytes 略有差异。

1 GiB fixture 示例：

```bash
$BIN prepare \
  --work-dir "$BENCH/data/scale-1g" \
  --runs 8 \
  --rows-per-run 0 \
  --target-run-bytes 134217728 \
  --payload-bytes 1024 \
  --batch-rows 4096 \
  --reader-buffer-bytes 33554432 \
  --writer-buffer-bytes 33554432 \
  --output-batch-bytes 67108864
```

单个受控 stage 示例：

```bash
CGROUP_PARENT=/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice

$BIN run-stage \
  --report "$BENCH/data/scale-1g/reports/cursor-final/sample-1.json" \
  --cgroup-parent "$CGROUP_PARENT" \
  --require-cgroup \
  --status-every 5ms \
  --smaps-every 50ms \
  -- cursor-final \
    --fixture "$BENCH/data/scale-1g/fixture.json" \
    --runs-dir "$BENCH/data/scale-1g/sorted" \
    --output-dir "$BENCH/data/scale-1g/output/cursor-final-1"

$BIN verify \
  --final "$BENCH/data/scale-1g/output/cursor-final-1/final.json"
```

`K=8` 每档的八个 source runs 分别由八个独立 `sort-run` 进程生成；
`K=16/32` 分别使用十六个和三十二个独立 sort-run 样本。三条 final 路径
各运行七个独立样本：

- `K=8/16` 使用 `full/cursor/current`、`cursor/current/full`、
  `current/full/cursor` 的三轮 Latin rotation；
- `K=32` 使用 `full/current/cursor`、`current/cursor/full`、
  `cursor/full/current` 的三轮 Latin rotation。

三轮顺序重复两次，再执行第一轮，共七个样本。该安排降低温度、后台负载和
page-cache 顺序偏差。每个 final 输出由新进程重新打开并逐行校验，随后删除
GiB 级 data object，只保留 supervisor JSON、verify JSON 和包含 metrics 的
final manifest。K=32 正式矩阵开始前有一个 blocked-order 试跑样本；发现顺序
不平衡后立即删除报告、输出和 cgroup，不进入任何 aggregate 或本文数字。

## 7. 结果汇总

本地汇总工具位于 `.benchmarks/import-reshard-e2e/aggregate/`，不是测试文件，也不提交：

```bash
go build -trimpath \
  -o "$BENCH/bin/aggregate" \
  "./$BENCH/aggregate"

$BENCH/bin/aggregate \
  -out-dir "$BENCH/results/aggregate-k16" \
  "$BENCH/data/scale-256m/reports" \
  "$BENCH/data/scale-512m/reports" \
  "$BENCH/data/scale-1g/reports" \
  "$BENCH/data/scale-1g-k16/reports"

$BENCH/bin/aggregate \
  -out-dir "$BENCH/results/aggregate-k32" \
  "$BENCH/data/scale-1g-k32/reports"

$BENCH/bin/aggregate \
  -out-dir "$BENCH/results/aggregate-all" \
  "$BENCH/data/scale-256m/reports" \
  "$BENCH/data/scale-512m/reports" \
  "$BENCH/data/scale-1g/reports" \
  "$BENCH/data/scale-1g-k16/reports" \
  "$BENCH/data/scale-1g-k32/reports"
```

输出为 `raw.csv/json` 和 `summary.csv/json`。最终 `aggregate-all` 共读取 177 份
supervisor 报告，全部有效；七样本的 p90 使用 nearest-rank，因此等于该组
最大值。

### 7.1 峰值内存

下表报告 active PSS 增量中位数，括号为最小值–最大值。表中的 Selected 是
`max(single sort-run, cursor-final)`，只表示“单 worker、sort 与 final 阶段不
重叠”假设下的峰值代理。两个 stage 实际由不同进程分别测量，本测试没有运行
并发 pipeline 或同节点多任务，因此它不是实测 DataNode 节点峰值。

| Fixture 数据规模 | Full sort | 单个 sort-run | Cursor final | Selected 相对 Full sort |
|---:|---:|---:|---:|---:|
| 255.65 MiB | 1018.8 MiB（887.5–1051.5） | 201.4 MiB（188.0–208.4） | 872.6 MiB（854.1–890.9） | -14.3% |
| 511.69 MiB | 1622.8 MiB（1497.0–1653.8） | 313.5 MiB（309.6–318.3） | 1232.1 MiB（1173.3–1304.4） | -24.1% |
| 1024.07 MiB | 2545.1 MiB（2458.1–2648.7） | 587.4 MiB（568.4–624.8） | 1646.4 MiB（1343.6–1907.5） | -35.3% |

READY PSS 中位数约为 113 MiB。1 GiB 档 absolute active PSS peak 中位数为 full sort 2658.0 MiB、cursor final 1760.2 MiB。cgroup anonymous memory 增量分别交叉验证出 13.1%、24.0% 和 34.7% 的降幅。

cgroup exact total peak delta 在三档中分别为：

| Fixture 数据规模 | Full sort | Current MergeSort | Cursor merge |
|---:|---:|---:|---:|
| 255.65 MiB | 1259.7 MiB | 995.8 MiB | 979.0 MiB |
| 511.69 MiB | 2099.7 MiB | 1769.7 MiB | 1945.7 MiB |
| 1024.07 MiB | 3584.0 MiB | 2591.0 MiB | 2864.6 MiB |

该 total 指标包含输出 page cache；512 MiB 档 cursor 的 file-page 增量高于 current MergeSort，因此不能只用 cgroup total 排序算法内存。PSS 和 cgroup anon 的规模曲线更稳定。

### 7.2 Final 阶段时间

| Fixture 数据规模 | Full sort | Current MergeSort | Cursor merge |
|---:|---:|---:|---:|
| 255.65 MiB | 2.349 s | 2.459 s | 2.235 s |
| 511.69 MiB | 4.820 s | 5.208 s | 4.718 s |
| 1024.07 MiB | 12.215 s | 10.271 s | 9.207 s |

该表只覆盖 final stage。Merge 路径接收已经排序的 runs，没有计入 re-shard 端八个 sort-run 的总 CPU；不能把它解释为端到端性能提升。若 sort-run 分散到多个 DataNode 并行，critical path 可能下降，但本测试没有运行并发 pipeline，不能据此下结论。

### 7.3 Heap cardinality

本次配置中 packed reader 实测最大 batch 为 1,024 行。`K=8` 时 current
MergeSort 的 heap 上界约为 8,192 entries，cursor 为 8 entries；但 cursor 相对 current
MergeSort 的 active PSS 中位数只降低 1.5%、2.7% 和 1.7%。这一结果说明缩小
heap cardinality 并不会按同一比例缩小进程 PSS，并与 reader、Arrow/string
data、native state 和 writer buffer 等固定工作集占较大比例相一致。测试没有
allocation profile，不能据此精确拆分每类内存；one-head cursor 可以直接确认
的价值是限制高 fan-in heap 与 allocation 放大。

### 7.4 `K=16` 定向结果

`K=16`、实际输入 1025.22 MiB 时：

| Path | Active PSS 增量中位数 | 范围 | Stage 中位时间 |
|---|---:|---:|---:|
| Full sort | 2621.1 MiB | 2531.5–2650.3 MiB | 10.272 s |
| Current MergeSort | 1789.8 MiB | 1695.3–2009.8 MiB | 11.463 s |
| Cursor merge | 1914.9 MiB | 1553.4–2082.2 MiB | 10.590 s |
| Single sort-run | 311.4 MiB | 304.3–338.7 MiB | 0.610 s |

Cursor 相对 full sort 的 active PSS 中位数降低 26.9%，cgroup anon 中位数降低
27.0%。Current MergeSort 的 heap 上界约为 16,384 entries，cursor 为 16
entries；cursor active PSS 中位数相对 `K=8` 增长 16.3%，且本组中位数比
current MergeSort 高 7.0%。样本范围存在重叠；该结果与 fan-in 增长时
per-reader working set 侵蚀内存优势这一解释一致，但没有 allocation profile
对其做直接归因。可以直接确认的是：缩小 heap 不保证总工作集更低。

### 7.5 `K=32` 定向结果

`K=32`、实际输入 1023.90 MiB，与 `K=16` 使用相同的 1,359,392 行和 1 KiB payload：

| Path | Active PSS 增量中位数 | 范围 | cgroup anon 中位数 | Stage 中位时间 |
|---|---:|---:|---:|---:|
| Full sort | 2682.3 MiB | 2588.2–2729.6 MiB | 2670.4 MiB | 11.418 s |
| Current MergeSort | 2526.4 MiB | 2475.4–2717.0 MiB | 2518.3 MiB | 11.667 s |
| Cursor merge | 2683.1 MiB | 2458.8–2731.8 MiB | 2674.0 MiB | 10.636 s |
| Single sort-run | 198.1 MiB | 187.4–211.6 MiB | 204.2 MiB | 0.341 s |

53 份 supervisor 报告全部有效且无 swap；21 个 final 输出逐行校验后均为 1,359,392 行、1,069,817,126 bytes 和相同 checksum。

Cursor 相对 full sort 的 active PSS 中位数为 +0.03%，cgroup anon 为 +0.13%，
范围均重叠；相对 `K=16` cursor 的 active PSS 中位数高 40.1%。Current
MergeSort 的 heap 上界约为 32,768 entries，cursor 仅为 32 entries，但 cursor
总 PSS 中位数仍高 6.2%。因此可以确认的是：高 fan-in 下即使 heap entries
大幅减少，总 PSS 也没有随之下降。最符合这些数据的解释是同时打开的 live
readers 及其 decoded batches/native/filesystem buffers 形成了随 K 增长的固定
工作集，但本测试没有逐分配类型的 profile，不能把该归因写成已直接测得的事实。
Cursor stage 时间中位数比 full sort 低 6.8%，但范围重叠，且不包含 sorted-run
生成成本，不能解释为端到端加速。

## 8. 当前结论与边界

当前数据支持：

- 在 `K=8` 的指定本地 packed workload 中，单 worker、阶段不重叠的
  `max(sort-run, cursor-final)` 峰值代理低于 full sort，且差距随输入从约
  256 MiB 增长到约 1 GiB 而扩大。
- 每个独立测得的 re-shard sort-run 峰值都低于 cursor final；因此在上述串行
  单 worker 假设下，峰值代理由 final merge 决定。多 task 并发的真实 DataNode
  节点峰值未测。
- Cursor heap 从 batch-row 级降到 run 级是正确方向，但 `K=8` 下不会单独带来数量级的总内存下降。
- 同为约 1 GiB 数据规模时，fan-in 从 8 增至 16 后 cursor 工作集上升；增至 32 后相对 full sort 已无峰值内存收益。因此首期单次 direct merge hard cap 为 16，超限必须 hierarchical merge 或回退 legacy 路径。

当前数据不证明：

- 所有 schema、PK 类型和 fan-in 都有相同收益；
- S3、TEXT/LOB、functions 或完整集群具有相同峰值；
- 总 CPU、总 I/O 或端到端延迟一定下降；
- 已经可以据此固定生产 slot 系数或消除 OOM 风险。

下一轮应覆盖 VarChar PK、namespace、predicate、duplicate PK、wide vectors、raw TEXT、functions、约 1 MiB tails、fan-in `K=4/30/100/1000`、reader buffer/batch 扫描、S3，以及单节点 1/2/4 worker 并发。

## 9. Harness 行为规格

临时 harness 已在报告发布后删除，本节记录其实际行为，便于审查历史结果并在
需要时重新实现等价测试。

### 9.1 测试 schema

测试 collection schema 固定包含三个字段：

| Field ID | 名称 | 类型 | 用途 |
|---:|---|---|---|
| 100 | `pk` | Int64，Primary Key | 唯一排序键 |
| 101 | `ordinal` | Int64 | 正确性 oracle；值必须始终等于 PK |
| 102 | `payload` | VarChar | 固定长度的确定性 payload |

三个字段全部位于 column group 0。测试没有 nullable/default、partition key、
namespace、vector、dynamic field、TEXT/LOB、delete/TTL 或 function output。
这使 benchmark 能直接观察排序/归并、packed reader/writer 和字符串 payload
的工作集，但不能外推到上述未覆盖 schema。

### 9.2 确定性无序数据

每个 source run 都不是按 PK 排序的。对 run 内位置 `position`，harness 先构造
与 `rows_per_run` 互质的奇数 multiplier 和固定 offset，再计算 affine
permutation：

```text
permuted = (position × multiplier + offset) mod rows_per_run
pk = permuted × K + run_index
ordinal = pk
```

因此：

- 每个 run 内是固定 seed 生成的无序排列；
- 第 `r` 个 run 只包含 `pk mod K = r` 的键；
- 所有 runs 的键在全局顺序中严格交错；
- 归并不能退化成简单按 run 顺序拼接；
- 排序后的全局输出必须恰好为 `0, 1, 2, ... N-1`。

payload 使用 `pk`、seed `20260721` 和 SplitMix64 风格混合函数生成，字符集为
`0-9A-Za-z-_`，每行精确 1,024 bytes。verify 进程会按同一函数重新计算每行
payload，而不是只比较行数。

### 9.3 物理读写路径

- `prepare`：`storage.NewPackedRecordWriter` 写无序 source run；
- `sort-run`：一个 `packed.NewPackedReader` → `storage.Sort` → packed writer；
- `full-sort-final`：同时打开 K 个无序 source readers → `storage.Sort`；
- `current-merge-final`：同时打开 K 个 sorted-run readers →
  当前 `storage.MergeSort`；
- `cursor-final`：同时打开 K 个 sorted-run readers，每个 run 在 heap 中只保留
  当前 head；
- `verify`：在测量进程退出后，由新进程重新打开 final packed object，逐行验证。

所有路径的 predicate 都是 `keepAll`，排序键只有 field 100。Final stage 的
output directory 创建与一次显式 Go GC 在 READY 前完成；reader、writer、
排序/比较、writer Close/flush 和 reader Close 都包含在 GO 后测量窗口内。
Fixture 生成和 final verify 不包含在性能窗口。

### 9.4 Cursor 原型

Cursor merge 为每个 reader 保存：

```text
run index
current Arrow record
row offset
cached Int64 key
```

初始化时每个 reader 只推进到第一条记录并向 heap 放入一个 cursor。每次 pop
最小 key 后 append 一行，推进同一个 cursor，再放回 heap。相同 key 时用 run
index 打破平局。由于本 fixture 的 PK 唯一，tie-break 不影响最终结果。

当前 `storage.MergeSort` 没有直接的 heap instrumentation。报告中的 current
MergeSort heap 数量是根据实际观测到的最大 reader batch 1,024 行计算的上界：

```text
K=8  → 8,192 entries
K=16 → 16,384 entries
K=32 → 32,768 entries
```

Cursor 的 exact maximum heap entries 分别是 8、16、32。

## 10. Fixture 清单

`prepare` 在 `rows-per-run=0` 时先生成一个 calibration packed object，根据
`CloseAndTell` 后的实际 object bytes 估算正式 `rows_per_run`，随后删除
calibration directory。目标 MiB 只是输入参数；报告一律使用正式 fixture 的
实际 object bytes。

Calibration 的实际计算为：

```text
calibration_rows =
    max(8192, target_run_bytes / (payload_bytes + 24) / 16)

rows_per_run =
    calibration_rows × target_run_bytes / calibration_object_bytes
```

这里的 `+24` 只是生成 calibration row 数时的保守行宽估计；最终
`rows_per_run` 使用真实 packed object bytes 反推，不用该估计值替代实测。

| Fixture | K | Rows/run | Total rows | Target/run | Source packed bytes | Sorted-run bytes | Final bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| scale-256m | 8 | 42,481 | 339,848 | 32 MiB | 268,069,620 | 267,331,908 | 267,457,163 |
| scale-512m | 8 | 84,962 | 679,696 | 64 MiB | 536,550,539 | 534,605,682 | 534,910,482 |
| scale-1g | 8 | 169,925 | 1,359,400 | 128 MiB | 1,073,812,783 | 1,069,211,961 | 1,069,823,380 |
| scale-1g-k16 | 16 | 84,962 | 1,359,392 | 64 MiB | 1,075,016,806 | 1,069,130,366 | 1,069,817,126 |
| scale-1g-k32 | 32 | 42,481 | 1,359,392 | 32 MiB | 1,073,637,703 | 1,069,390,056 | 1,069,817,126 |

实际单 run object 范围和 fixture manifest identity 为：

| Fixture | Source run bytes 范围 | Sorted run bytes 范围 | `fixture.json` SHA-256 |
|---|---:|---:|---|
| scale-256m | 33,501,678–33,527,997 | 33,416,074–33,416,882 | `e847674841ffaf9078825ea7fd733e3ed84e3592388a3a88bc59ce472c1ce3c1` |
| scale-512m | 67,002,037–67,166,459 | 66,824,969–66,826,291 | `a14b8299263cf448979f158d2f2c874d5d7b7d3d9bcb092a423b48c9deb5bb83` |
| scale-1g | 133,788,642–134,421,471 | 133,650,104–133,652,915 | `44ad084ced149b520f0cd7e6d06be27bd1dcbb95b248ce302fc95dc55e20557e` |
| scale-1g-k16 | 67,148,787–67,208,341 | 66,820,019–66,821,361 | `b7db7085df95c077bea80e4bee0c2085f745f431ec09d5efc219bb6cc7b36b8c` |
| scale-1g-k32 | 33,519,412–33,568,245 | 33,417,728–33,419,127 | `503e946675cc1bc1b4b73611e31ab2c12af5f8524b668550c230f6aad416660e` |

Source bytes 与 sorted bytes 不完全相同，是因为无序输入与按 PK 排序后的
Parquet 编码/压缩效果不同；final 路径的逻辑行集、schema 和 payload 保持一致。

其他固定参数：

| 参数 | 值 |
|---|---:|
| seed | 20,260,721 |
| input generation batch | 4,096 rows |
| writer buffer | 33,554,432 bytes |
| reader buffer | 33,554,432 bytes |
| output batch target | 67,108,864 bytes |
| packed reader observed max batch | 1,024 rows |
| column groups | 1 |
| physical objects per source/sorted run | 1 |
| physical objects per final output | 1 |

`K=8` 的 1 GiB fixture 因 calibration rounding 比 `K=16/32` 多 8 行，所以
fan-in 比较主要使用 K=16 与 K=32 的完全相同行数；K=8 的规模曲线只在自身
fixture 内比较三条 final 路径。

## 11. 进程与 cgroup 测量协议

### 11.1 单样本生命周期

每份 supervisor 报告对应以下完整生命周期：

1. 在 cgroup v2 父目录下创建唯一的 `milvus-reshard-<operation>-<pid>-<time>`；
2. 启动新的 benchmark worker，并在执行目标 binary 前把 shell PID 写入该
   cgroup 的 `cgroup.procs`；
3. Worker 加载 fixture/run manifest、初始化 filesystem、创建空 output directory
   并执行 `runtime.GC()`；
4. Worker 输出 `READY`，随后阻塞等待 stdin 的 `GO`；
5. Supervisor 读取 READY 时的 `/proc`、`smaps_rollup` 和 cgroup baseline；
6. Supervisor 启动外部 sampler，再写入 `GO`；
7. Worker 打开 readers/writer，执行被测 stage并关闭所有资源；
8. Supervisor 等待进程退出，停止 sampler，读取结束值和 `memory.peak`；
9. 若指标有效则写 supervisor JSON，并删除空 cgroup；
10. Final 路径由另一个进程重新打开输出逐行 verify，保存 verify JSON；
11. 复制 final manifest/metrics 后删除 GiB 级 output data。

fresh cgroup 的意义是给 `memory.peak` 一个新的生命周期。当前 kernel 暴露的
`memory.peak` 是只读且不能 reset；由于每个 worker 使用新 cgroup，如果
`memory.peak` 在 READY 后继续上升，就可以确认 exact lifetime high-water mark
发生在 active phase。177 个保留样本全部满足这一条件。

### 11.2 采样频率

| 数据源 | 频率 | 主要字段 |
|---|---:|---|
| `/proc/<pid>/status` | 5 ms | VmRSS、RssAnon、RssFile、VmHWM、VmSwap、Threads |
| cgroup files | 5 ms | memory.current、memory.peak、memory.stat、swap、cpu.stat、io.stat |
| `/proc/<pid>/smaps_rollup` | 50 ms | PSS、Private、Anonymous、Swap |
| worker internal monitor | 10 ms | Go MemStats、process status；smaps 至少每 100 ms |
| `getrusage` | stage begin/end | user/system CPU、fault、context switch、ru_maxrss |
| `/proc/<pid>/io` | stage begin/end | logical chars、syscalls、block read/write bytes |
| Milvus filesystem metrics | stage begin/end | read/write count 与 bytes |

177 个有效样本的实际采样密度为：

| 项目 | 每样本观测范围 |
|---|---:|
| READY latency | 133.27–514.00 ms；中位数 157.90 ms |
| active `/proc/status` samples | 69–2,104 |
| active `smaps_rollup` samples | 7–368 |
| active cgroup samples | 70–2,105 |

READY latency 是 worker 启动、进入 fresh cgroup、完成初始化并发出 READY 所需
时间，不属于被测 stage duration。所有样本的 active phase 合计 908.390 s。
5 ms/50 ms 采样是离散观测，因此 PSS 可能遗漏短于采样周期的瞬时尖峰；fresh
cgroup 的 `memory.peak` 则提供进程生命周期内的 exact total high-water mark
交叉检查，但其中还包含 page cache 和 kernel accounting。

### 11.3 主要公式

| 指标 | 定义 |
|---|---|
| active PSS delta | active window 内 sampled max PSS − READY PSS |
| active RSS delta | active window 内 sampled max VmRSS − READY VmRSS |
| active anonymous delta | sampled max `smaps Anonymous` − READY Anonymous |
| cgroup anon delta | sampled max `memory.stat.anon` − READY anon |
| cgroup sampled total delta | sampled max `memory.current` − READY memory.current |
| cgroup exact peak delta | 若 lifetime `memory.peak` 在 READY 后上升，则 `memory.peak − READY memory.current` |
| Go heap delta | worker internal max HeapAlloc/HeapInuse − internal phase baseline |
| worker duration | worker phase monitor 从 reader/writer 创建前到 Close 完成 |
| supervisor active duration | 发出 GO 前启动 active clock，到 worker exit |

Worker duration 包含 reader/writer open、实际 read、sort/merge、append/write、
writer Close/flush 和 reader Close；不包含 worker 退出后的 copied final manifest、
独立 verify、aggregate 或大 output 删除。Supervisor active duration通常稍长，
还包含 GO 传输、worker 写最终小型 manifest 以及进程退出成本。

所有减法使用 saturating subtraction，出现 end 小于 baseline 时记为 0。
`memory.stat` 各字段的 peak 是独立采样最大值，不是同一时刻的原子快照。

PSS 是主报告指标，因为它按比例计入共享映射；cgroup anon 用于独立交叉验证。
cgroup total 同时包含输出 page cache 和可能归属该 cgroup 的 file-backed pages，
因此不能单独用来判断排序算法工作集。`RSS - Go heap` 也不能解释为精确的
C++/cgo allocation。

### 11.4 样本作废条件

以下任一条件会把样本标记为 invalid：

- 无法建立 required fresh cgroup；
- READY 时缺少 `status`、`smaps_rollup` 或 cgroup baseline；
- active window 没有 process/cgroup samples；
- worker 非零退出或超时；
- process `VmSwap`/`smaps Swap` 增长；
- cgroup `memory.swap.peak` 增长。

177 份保留报告全部 valid，invalid count 为 0，process 与 cgroup swap growth
均为 0。4 个样本记录到 1–2 次 major fault，但没有数据错误、swap 或失败。
没有按结果大小删除 outlier，也没有对成功样本做人工筛选。

17 份 valid report 的 stderr 包含 native scope metric 慢调用日志，共 5 次
`ReadNext >1s` 和 25 次 `WriteRecordBatch >1s`，最长一次约 4.683 s。这些
日志不是 worker error，样本仍完整通过输出校验，因此没有判 invalid。这些长
调用与部分 wall-clock 长尾重合；可能涉及 writer flush、本地 I/O/page cache
或主机背景负载，本测试没有做因果分解。

## 12. 样本与报告清单

| Fixture | Sort-run reports | Final reports | Verified finals | Total supervisor reports |
|---|---:|---:|---:|---:|
| scale-256m，K=8 | 8 | 21 | 21 | 29 |
| scale-512m，K=8 | 8 | 21 | 21 | 29 |
| scale-1g，K=8 | 8 | 21 | 21 | 29 |
| scale-1g-k16 | 16 | 21 | 21 | 37 |
| scale-1g-k32 | 32 | 21 | 21 | 53 |
| **合计** | **72** | **105** | **105** | **177** |

177 份保留报告的运行窗口为：

- 首个 supervisor 开始：`2026-08-03T12:29:16.634063132Z`；
- 最后一个有效样本结束：`2026-08-03T13:30:06.626357063Z`；
- active phase 累计：908.390 s。

Smoke 和被丢弃的 K=32 顺序试跑不在 177 中。包含 smoke 在内共创建约 180 个
task-specific cgroup，测试结束后均已删除，没有 cgroup cleanup error。

Aggregate 规则如下：

- raw JSON/CSV 保留所有 supervisor sample；
- summary 只使用 `valid=true` 的 sample；
- median：奇数取中间值，偶数取两个中间值平均；
- p90：nearest-rank，索引为 `ceil(0.90 × N)-1`；
- final path 每组 N=7，所以 p90 等于 max；
- 下文范围均为 min–max，没有置信区间推断。

## 13. 详细结果

### 13.1 Final stage 内存

单位均为 MiB。`Exact total` 是 cgroup exact peak delta 的中位数；
`Go Heap` 是 worker HeapAlloc peak delta 的中位数。

| Fixture | Path | N | Active PSS median [min,max] | cgroup anon median [min,max] | Exact total | Go Heap |
|---|---|---:|---:|---:|---:|---:|
| 256 MiB, K=8 | Full Sort | 7 | 1018.8 [887.5,1051.5] | 1009.9 [878.3,1044.3] | 1259.7 | 379.1 |
| 256 MiB, K=8 | Current MergeSort | 7 | 886.1 [874.8,895.1] | 876.8 [866.0,886.1] | 995.8 | 378.1 |
| 256 MiB, K=8 | Cursor | 7 | 872.6 [854.1,890.9] | 877.4 [862.1,884.1] | 979.0 | 376.5 |
| 512 MiB, K=8 | Full Sort | 7 | 1622.8 [1497.0,1653.8] | 1615.7 [1487.8,1645.0] | 2099.7 | 498.0 |
| 512 MiB, K=8 | Current MergeSort | 7 | 1266.5 [1065.9,1348.4] | 1240.6 [1056.9,1339.5] | 1769.7 | 493.0 |
| 512 MiB, K=8 | Cursor | 7 | 1232.1 [1173.3,1304.4] | 1228.2 [1184.3,1298.2] | 1945.7 | 491.5 |
| 1 GiB, K=8 | Full Sort | 7 | 2545.1 [2458.1,2648.7] | 2548.0 [2448.8,2645.0] | 3584.0 | 505.5 |
| 1 GiB, K=8 | Current MergeSort | 7 | 1674.6 [1434.5,1920.7] | 1670.9 [1439.0,1902.9] | 2591.0 | 494.0 |
| 1 GiB, K=8 | Cursor | 7 | 1646.4 [1343.6,1907.5] | 1665.0 [1332.6,1895.1] | 2864.6 | 491.5 |
| 1 GiB, K=16 | Full Sort | 7 | 2621.1 [2531.5,2650.3] | 2612.2 [2522.4,2643.0] | 3640.3 | 505.6 |
| 1 GiB, K=16 | Current MergeSort | 7 | 1789.8 [1695.3,2009.8] | 1790.1 [1707.9,2000.7] | 2936.1 | 495.5 |
| 1 GiB, K=16 | Cursor | 7 | 1914.9 [1553.4,2082.2] | 1906.7 [1544.5,2073.2] | 3381.1 | 492.9 |
| 1 GiB, K=32 | Full Sort | 7 | 2682.3 [2588.2,2729.6] | 2670.4 [2580.1,2720.7] | 4072.0 | 505.6 |
| 1 GiB, K=32 | Current MergeSort | 7 | 2526.4 [2475.4,2717.0] | 2518.3 [2466.3,2708.0] | 3640.9 | 495.4 |
| 1 GiB, K=32 | Cursor | 7 | 2683.1 [2458.8,2731.8] | 2674.0 [2449.8,2722.8] | 3422.3 | 492.8 |

READY PSS 在所有 final groups 中约为 111.5–114.7 MiB，中位数通常约 113 MiB；
READY cgroup current 中位数约 35.6–36.5 MiB。两者不同是正常现象：PSS 会按
比例计算共享映射，而 cgroup 只向实际被 charge 的 cgroup 计费。

### 13.2 Final stage 时间、CPU 与 storage I/O

CPU 为 worker user/system time 中位数；Storage Read/Write 是 Milvus filesystem
instrumentation 的逻辑 bytes 中位数，不等同于 Linux block I/O。

| Fixture | Path | Worker seconds median [min,max] | CPU user/system | Storage read/write MiB |
|---|---|---:|---:|---:|
| 256 MiB, K=8 | Full Sort | 2.349 [2.259,2.385] | 1.745 / 0.875 | 257.40 / 255.07 |
| 256 MiB, K=8 | Current MergeSort | 2.459 [2.362,2.698] | 1.879 / 0.932 | 256.69 / 255.07 |
| 256 MiB, K=8 | Cursor | 2.235 [2.175,2.379] | 1.698 / 0.898 | 256.69 / 255.07 |
| 512 MiB, K=8 | Full Sort | 4.820 [4.593,6.366] | 3.459 / 2.017 | 514.19 / 510.13 |
| 512 MiB, K=8 | Current MergeSort | 5.208 [4.767,5.388] | 3.738 / 2.093 | 512.33 / 510.13 |
| 512 MiB, K=8 | Cursor | 4.718 [4.560,5.228] | 3.235 / 2.016 | 512.33 / 510.13 |
| 1 GiB, K=8 | Full Sort | 12.215 [9.166,18.536] | 6.905 / 5.897 | 1028.05 / 1020.26 |
| 1 GiB, K=8 | Current MergeSort | 10.271 [10.014,12.344] | 7.318 / 4.253 | 1023.66 / 1020.26 |
| 1 GiB, K=8 | Cursor | 9.207 [8.577,10.482] | 6.272 / 4.021 | 1023.66 / 1020.26 |
| 1 GiB, K=16 | Full Sort | 10.272 [9.715,11.392] | 6.915 / 5.142 | 1030.20 / 1020.26 |
| 1 GiB, K=16 | Current MergeSort | 11.463 [10.693,15.540] | 7.561 / 4.875 | 1024.58 / 1020.26 |
| 1 GiB, K=16 | Cursor | 10.590 [9.709,15.096] | 6.379 / 4.737 | 1024.58 / 1020.26 |
| 1 GiB, K=32 | Full Sort | 11.418 [9.632,13.725] | 6.923 / 5.340 | 1030.89 / 1020.26 |
| 1 GiB, K=32 | Current MergeSort | 11.667 [10.904,12.135] | 7.851 / 5.277 | 1026.84 / 1020.26 |
| 1 GiB, K=32 | Cursor | 10.636 [9.730,13.114] | 6.414 / 5.171 | 1026.84 / 1020.26 |

Full Sort 的 K=8、1 GiB 时间范围明显更宽，因此不能用其 12.215 s 中位数推导
“K 越大 Full Sort 越快”。测试没有固定 CPU、清除 page cache 或重复冷启动
整机，时间结论只适用于这些样本范围。

#### Full Sort 内部分段时间

Worker 还分别记录了 Full Sort 内部三个 phase 的 wall-clock：读取并物化 records、
提取 sort key 并排序、构造 output batches 并调用 `rw.Write`。这些值由
`time.Now`/`time.Since` 得到，不是 CPU time；最后一个 phase 也不包含
`storage.Sort` 返回后由 worker 执行的 writer `Close`。下表为七个 final 样本
各 phase 的中位数：

| Fixture | Read/materialize wall | Key extraction + radix wall | Output build + `rw.Write` wall |
|---|---:|---:|---:|
| 256 MiB, K=8 | 0.831 s | 0.017 s | 1.392 s |
| 512 MiB, K=8 | 1.813 s | 0.037 s | 2.795 s |
| 1 GiB, K=8 | 3.855 s | 0.076 s | 8.101 s |
| 1 GiB, K=16 | 3.812 s | 0.082 s | 6.125 s |
| 1 GiB, K=32 | 3.896 s | 0.078 s | 7.343 s |

这组 schema 只有一个 Int64 排序键，当前实现走稳定的 8-pass LSD radix path。
包含 key extraction 的 sort phase wall-clock 中位数只有约 17–82 ms，明显小于
同组 read/materialize 和 output-build/write phase 的中位数；这不是 radix CPU
time 的单独测量。1 GiB、K=8 的 Full Sort 总时间范围为 9.166–18.536 s，
output-build/write phase 自身范围为 5.215–13.540 s，但本报告没有保存逐样本的
phase/total 相关性分析，因此不能断言总时间波动主要由该 phase 导致。可以稳妥
得出的结论是：在这个 workload 的中位数分解中，key extraction + radix 不是
最大的 wall-clock phase；read/materialize、output construction、`rw.Write`、
writer `Close` 和底层 I/O 行为仍需分别分析。

### 13.3 Sort-run 阶段

| Fixture | N | PSS median [min,max] MiB | Duration/run median [min,max] s | 所有 runs duration 之和 | 所有 runs CPU 之和 |
|---|---:|---:|---:|---:|---:|
| 256 MiB, K=8 | 8 | 201.4 [188.0,208.4] | 0.336 [0.327,0.352] | 2.692 s | 3.298 s |
| 512 MiB, K=8 | 8 | 313.5 [309.6,318.3] | 0.624 [0.603,0.795] | 5.239 s | 6.096 s |
| 1 GiB, K=8 | 8 | 587.4 [568.4,624.8] | 1.194 [1.148,1.755] | 10.134 s | 10.915 s |
| 1 GiB, K=16 | 16 | 311.4 [304.3,338.7] | 0.610 [0.584,0.656] | 9.791 s | 11.352 s |
| 1 GiB, K=32 | 32 | 198.1 [187.4,211.6] | 0.341 [0.330,0.445] | 11.231 s | 13.675 s |

“所有 runs duration 之和”表示把各独立进程 wall time 相加后的串行工作量，
不是生产 pipeline 的 elapsed time。Runs 若分散到多个 DataNode，可以并行；
本次测试没有实际并发运行这些进程。

### 13.4 约 1 GiB 的时间解释

| K | Cursor final median | Sort-run duration 总和 | 串行相加 | 理想并行估算：最慢 run + final |
|---:|---:|---:|---:|---:|
| 8 | 9.207 s | 10.134 s | 19.341 s | 10.962 s |
| 16 | 10.590 s | 9.791 s | 20.381 s | 11.246 s |
| 32 | 10.636 s | 11.231 s | 21.867 s | 11.081 s |

最后一列只是根据已分别测得的 stage 构造的理想化估算：假设所有 sort-run
完全并行、无调度/RPC/S3 代价，并等待最慢 run 后执行 final。它不是实际执行过
的并行 e2e 数据。“串行相加”和“理想并行估算”也都不是生产耗时的上下界：
任务排队、对象存储、RPC、重试和资源竞争可能让真实耗时高于本地串行相加，
不同硬件与缓存状态也会改变每个 stage。本表只能说明同一批本地样本在忽略额外
系统开销时的工作量分解，不能直接预测集群 elapsed time。

### 13.5 正确性结果

| Fixture | Final outputs | Rows/output | Object bytes/output | FNV-1a checksum |
|---|---:|---:|---:|---:|
| scale-256m | 21 | 339,848 | 267,457,163 | 7,723,492,480,486,443,249 |
| scale-512m | 21 | 679,696 | 534,910,482 | 311,251,267,044,433,579 |
| scale-1g | 21 | 1,359,400 | 1,069,823,380 | 13,086,271,395,472,501,134 |
| scale-1g-k16 | 21 | 1,359,392 | 1,069,817,126 | 6,353,352,740,151,266,626 |
| scale-1g-k32 | 21 | 1,359,392 | 1,069,817,126 | 6,353,352,740,151,266,626 |

每个 output 都验证：

- PK 从 0 连续递增到 `rows-1`；
- `ordinal == pk`；
- payload 与 `payloadFor(pk, 1024, 20260721)` 完全一致；
- manifest rows 与逐行读取 rows 一致；
- final packed object bytes 在同一 fixture 的三条路径和七个样本间一致；
- 全量 PK+payload 的 FNV-1a checksum 一致。

这里的 checksum 是逐行语义 checksum，不是 Parquet object 的 byte-for-byte
SHA-256。具体输入按输出行顺序依次为：每行 PK 的 8-byte little-endian 表示，
随后是该行 payload bytes；`ordinal == pk` 由单独断言验证。同一 fixture 的
object byte size 恰好一致是本次观测结果，但正确性判断依赖重新打开后的行序、
字段值、行数和上述语义 checksum，不能把它泛化成“所有 packed writer 输出
必然产生完全相同的 Parquet bytes”。

删除 worktree 前，对五个 scale 的 387 个 report-side JSON 文件——177 个
supervisor reports、105 个 copied final manifests、105 个 verify reports——
生成了带相对路径的有序 checksum-list digest。历史命令为：

```bash
cd /home/zilliz/Code/milvus-reshard-e2e-bench
mapfile -d '' -t REPORT_FILES < <(
  LC_ALL=C find .benchmarks/import-reshard-e2e/data/scale-*/reports \
    -type f -name '*.json' -print0 \
    | LC_ALL=C sort -z
)
test "${#REPORT_FILES[@]}" -eq 387 || exit 1
printf '%s\0' "${REPORT_FILES[@]}" | xargs -0 sha256sum | sha256sum
```

输出为：

```text
911fe54c3e01c2a29c7d3c5b38a5adeae16b1acec76378891df7b19b84e42232  -
```

该 digest 对 `sha256sum` 的完整输出再次求 SHA-256，所以成员的
worktree-relative path 也是身份的一部分；删除原始报告后，它只用于记录当时
被审计的集合，不意味着可以仅凭 digest 恢复原始 JSON。

## 14. 结果解释

### 14.1 为什么 K=8/16 能降低内存

Full Sort 需要保存 final segment 的 decoded records 和全量排序辅助数据。
Sorted-run 路径把排序工作拆到较小 run，final 阶段只流式读取当前 batches。
因此 K=8 的单 worker、阶段不重叠峰值代理相对 Full Sort 分别降低 14.3%、
24.1% 和 35.3%；K=16、约 1 GiB 仍降低 26.9%。

本 fixture 是单 Int64 PK，当前 Full Sort 使用 radix 路径，复杂度接近 O(N)，
而不是一般 comparison sort 的 O(N log N)。Sorted-run + heap merge 会增加
额外归并工作，因此本设计的首要收益是降低受控 fan-in 下的峰值内存，而不是
保证 CPU 或总 wall-clock 更低。

### 14.2 为什么 K=32 收益消失

Cursor 只把 merge heap 从最多约 32,768 entries 降到 32，但它仍同时打开
32 个 packed readers。每个 reader 可能持有 decoded Arrow batch、字符串
buffers、reader/native state 和 filesystem buffers。没有逐类型 allocation
profile 时不能直接断言哪一类占主导，但 K=32 数据与随 reader 数增长的固定
工作集抵消 heap 缩减这一解释最一致：

- Cursor PSS 2683.1 MiB；
- Full Sort PSS 2682.3 MiB；
- 差异 +0.029%，范围高度重叠；
- Cursor cgroup anon 比 Full Sort高 0.134%；
- Cursor PSS 比 K=16 Cursor 高 40.1%。

Current MergeSort 在 K=32 的 PSS 中位数为 2526.4 MiB，低于 Cursor 6.2%，
尽管其 heap 上界大得多。由于范围重叠，这不能证明 current MergeSort 普遍更优；
它只进一步证明 heap cardinality 不能单独解释当前总工作集。

### 14.3 设计结论

首期 direct merge fan-in hard cap 为 16。`K>16` 时 planner 必须：

1. 以 fan-in ≤16 执行 hierarchical merge，生成 intermediate immutable runs；
2. 或在 workload 未通过 benchmark gate 时回退 legacy Import + sort compaction。

16 不是永久协议常量。后续可以在更小 reader buffer、不同 batch、VarChar PK、
TEXT/vector/function schema、S3 和多 worker 数据充分后重新校准。

该 cap 不是因为 Cursor 在 K=16 已全面优于 current MergeSort：K=16 的 Cursor
PSS 中位数 1914.9 MiB，反而高于 current MergeSort 的 1789.8 MiB，且范围
重叠。选择 16 是基于 K=16 仍显著低于 Full Sort、K=32 收益消失，以及避免
无上限 live readers 的保守首期 gate。

## 15. 历史构建与运行命令

以下命令是已删除 worktree 的历史记录，用于说明二进制如何生成。路径中的
Conan package ID 和 build directory 对应当时主机。

### 15.1 ABI shim

复用的 `libmilvus_core.so` 早于 Go wrapper 对
`SegcoreSetEnableGISSplitFusion` 的引用。排序路径不会进入 GIS，因此临时 shim
只导出该符号：

```c
#include <stdbool.h>

void
SegcoreSetEnableGISSplitFusion(const bool value) {
    (void)value;
}
```

构建：

```bash
BENCH=/home/zilliz/Code/milvus-reshard-e2e-bench/.benchmarks/import-reshard-e2e
gcc -fPIC -shared -Wl,-soname,libmilvus_bench_compat.so \
  -o "$BENCH/native/libmilvus_bench_compat.so" \
  "$BENCH/native/milvus_bench_compat.c"
```

该 shim 不替换任何生产实现，不修改 `libmilvus_core.so`，并且 benchmark
不调用该 GIS setter。ABI 完全同步的 native build 不需要 shim。

较早原型目录中的带注释 shim source SHA-256 为
`805eec7674c13d979f6fb8706735b3052022320beb6e8464388eb5ab6d387e21`；
本节最小 shim source 的 SHA-256 为
`d680f6e4d2ba387949d8cfb5a575d909aefaaa01061ebab8aba18962a5d0f75f`。
这两份内容不同的 C source 分别编译后，得到的两份 `.so` bytes 完全相同，
`.so` SHA-256 均为
`a4196267638791052f8a84945180d79cf7a4c6bd34d03458c2df628fcfea319d`。
早期 K=8/K=16 supervisor JSON 没有持久化 `LD_LIBRARY_PATH`，因此删除环境后
不能仅凭报告证明 loader 使用了哪个候选路径；可以证明的是两个候选路径上的
library bytes 完全相同，且唯一导出补丁符号不在被测 sort/packed I/O 路径执行。

### 15.2 优化 binary

```bash
ROOT=/home/zilliz/Code/milvus-reshard-e2e-bench
BENCH=$ROOT/.benchmarks/import-reshard-e2e

env \
  GOTMPDIR=$BENCH/tmp \
  TMPDIR=$BENCH/tmp \
  PKG_CONFIG_PATH=/home/zilliz/Code/milvus/cmake_build/src:/home/zilliz/Code/milvus/cmake_build/thirdparty/milvus-storage:/home/zilliz/Code/milvus/cmake_build/thirdparty/knowhere:/home/zilliz/Code/milvus/cmake_build/thirdparty/rdkafka:/home/zilliz/Code/milvus/cmake_build/thirdparty/rocksdb:/home/zilliz/Code/milvus/internal/core/output/lib/pkgconfig \
  CGO_CFLAGS='-I/home/zilliz/Code/milvus-reshard-e2e-bench/internal/core/src -I/home/zilliz/Code/milvus/internal/core/output/include -I/home/zilliz/.conan2/p/b/milvub08b1a4a308f1/p/include -I/home/zilliz/.conan2/p/b/rocks118c4f8d93329/p/include -I/home/zilliz/.conan2/p/b/librd87b4999b0b18f/p/include -I/home/zilliz/.conan2/p/b/arrow14522b16129f3/p/include -I/home/zilliz/Code/milvus/cmake_build/thirdparty/milvus-storage/milvus-storage-src/cpp/include' \
  CGO_CXXFLAGS='-I/home/zilliz/Code/milvus-reshard-e2e-bench/internal/core/src -I/home/zilliz/Code/milvus/internal/core/output/include -I/home/zilliz/.conan2/p/b/milvub08b1a4a308f1/p/include -I/home/zilliz/.conan2/p/b/rocks118c4f8d93329/p/include -I/home/zilliz/.conan2/p/b/librd87b4999b0b18f/p/include -I/home/zilliz/.conan2/p/b/arrow14522b16129f3/p/include -I/home/zilliz/Code/milvus/cmake_build/thirdparty/milvus-storage/milvus-storage-src/cpp/include' \
  CGO_LDFLAGS="-L$BENCH/native -lmilvus_bench_compat -L/home/zilliz/Code/milvus/cmake_build/src -L/home/zilliz/Code/milvus/cmake_build/thirdparty/milvus-storage/milvus-storage-build -L/home/zilliz/Code/milvus/cmake_build/thirdparty/knowhere/knowhere-build -L/home/zilliz/Code/milvus/cmake_build/lib -L/home/zilliz/Code/milvus/internal/core/output/lib -L/tmp/milvus-libaio/aio-root/lib" \
  LIBRARY_PATH=$BENCH/native:/home/zilliz/Code/milvus/cmake_build/src:/home/zilliz/Code/milvus/cmake_build/thirdparty/milvus-storage/milvus-storage-build:/home/zilliz/Code/milvus/cmake_build/thirdparty/knowhere/knowhere-build:/home/zilliz/Code/milvus/cmake_build/lib:/home/zilliz/Code/milvus/internal/core/output/lib:/tmp/milvus-libaio/aio-root/lib \
  LD_LIBRARY_PATH=$BENCH/native:/home/zilliz/Code/milvus/cmake_build/src:/home/zilliz/Code/milvus/cmake_build/thirdparty/milvus-storage/milvus-storage-build:/home/zilliz/Code/milvus/cmake_build/thirdparty/knowhere/knowhere-build:/home/zilliz/Code/milvus/cmake_build/lib:/home/zilliz/Code/milvus/internal/core/output/lib:/tmp/milvus-libaio/aio-root/lib \
  go build -tags dynamic -trimpath \
    -o $BENCH/bin/reshard-e2e \
    ./.benchmarks/import-reshard-e2e
```

直接动态依赖包括 `libmilvus_bench_compat.so`、`libmilvus-storage.so`、
`libmilvus_core.so`、`librdkafka.so.1` 和 `librocksdb.so.6`。其他依赖从主
`cmake_build/lib`、system libraries 和 `/tmp/milvus-libaio/aio-root/lib`
解析。

### 15.3 典型受控运行

```bash
export LD_LIBRARY_PATH=$BENCH/native:/home/zilliz/Code/milvus/cmake_build/src:/home/zilliz/Code/milvus/cmake_build/thirdparty/milvus-storage/milvus-storage-build:/home/zilliz/Code/milvus/cmake_build/thirdparty/knowhere/knowhere-build:/home/zilliz/Code/milvus/cmake_build/lib:/home/zilliz/Code/milvus/internal/core/output/lib:/tmp/milvus-libaio/aio-root/lib

$BENCH/bin/reshard-e2e run-stage \
  --report <report.json> \
  --cgroup-parent /sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice \
  --require-cgroup \
  --status-every 5ms \
  --smaps-every 50ms \
  -- cursor-final \
    --fixture <fixture.json> \
    --runs-dir <sorted-run-directory> \
    --output-dir <new-output-directory>

$BENCH/bin/reshard-e2e verify --final <new-output-directory>/final.json
```

## 16. 已知限制

本报告不覆盖：

- 完整 Milvus cluster、DataCoord/DataNode RPC、etcd catalog 和 segment commit；
- S3/MinIO/云对象存储、range GET、multipart、网络带宽和远端尾延迟；
- 多 task 或多个 final worker 在同一 DataNode 上并发；
- 冷 page cache、主动 drop cache、`fsync` 或独占 block device；
- CPU pinning、CPU quota、固定 governor 或隔离后台服务；
- VarChar PK、duplicate PK、namespace multi-key comparator；
- predicate、delete、TTL、empty reader 和 injected reader/writer errors；
- vectors、raw TEXT、LOB、embedding/BM25 function output；
- 多 column group、不同 compression、不同 reader/output batch；
- 1 MiB task tails、fan-in 100/1000 和多层 hierarchical merge；
- 16 GiB task、retry、cancel、OOM、ENOSPC 和对象 GC；
- final segment stats、index build 和端到端 import 用户延迟。

另外，约 1 GiB 的 `K=32` fixture 使用 32 MiB/run，以便在固定总数据量下观察
fan-in 增长；它不是最终设计期望的 128 MiB 临时 fragment 大小。首期 direct
fan-in cap=16 是根据当前本地数据给出的保守 gate，`K>16` 的 hierarchical
merge 本身尚未在进程级 E2E harness 中执行。复用的 native `.so` 也没有与 Go
checkout 做整栈重建，因此任何依赖最新 native ABI/实现细节的性能外推都需要在
同 revision clean build 上复测。

测试在本地 ext4 上顺序执行 final samples，没有清除 page cache。Linux
`proc_io.read_bytes` 在部分样本中可能为 0，因为读取命中 cache；报告使用
Milvus filesystem logical bytes 描述数据量。cgroup total 也可能包含输出
page cache，因此性能结论以 PSS、anon、CPU、worker duration 和逻辑 I/O
联合解释。

### 16.1 后续生产验证矩阵

以下内容是尚未执行的后续验证计划，不属于本报告的已完成结果：

- `V=16, P=1024`；
- 大量小文件、单大文件、窄行和宽向量 schema；
- Int64 / VarChar PK，namespace 开启与关闭；
- 约 128 MiB 完整 runs、约 1 MiB tails，以及 fan-in `K=4/30/100/1000`；
- narrow scalar rows、wide vectors、raw UTF8 TEXT 和 embedding/BM25 function output；
- 16 GiB task 的 reader、spill、upload 和 retry；
- 单节点 1/2/4 个 worker 并发时的 PSS、cgroup anon、CPU 和 slot 安全性；
- 多 attempt 并存时的临时空间；
- ReshardManifest 文件数、大小、加载内存与 planning latency；
- S3/对象存储的请求数、bytes、multipart、page-cache 差异和小 tail 分布。

同一数据集应比较：

```text
current storage.Sort
current storage.MergeSort
cursor-based one-head-per-run merge
single re-shard sorted-run generation
```

至少记录：

- READY baseline、absolute/active-delta PSS、RSS、Anonymous、Private 和 cgroup anon/file/total；
- Go HeapAlloc/HeapInuse、GC count/pause 和 maximum heap entries；
- CPU seconds，而不只 wall-clock；
- re-shard local sort 与 external merge I/O；
- final spill bytes；
- OSS GET/PUT/range request 数和 bytes；
- first error latency、end-to-end latency 和 retry 重做量；
- final segment fill ratio 和输出一致性。

还应扫描 reader batch size、output batch size、max row size、duplicate-PK
ratio，并固定覆盖 predicate selectivity `0%`、`50%` 和 `99%`。每次迭代必须
重新创建 reader，不能复用已经耗尽的 reader。该矩阵的结果应继续只记录在本
独立测试报告中，不回填为主设计文档中的性能承诺。

## 17. 清理与保留策略

永久保留的只有本 Markdown 报告以及主设计/源码差异文档。以下临时内容在报告
推送后删除：

- benchmark harness Go/C/shell source；
- benchmark 与 aggregate binaries；
- source/sorted/final packed objects；
- supervisor、manifest、verify、raw/summary JSON/CSV；
- ABI shim source 与 shared object；
- Import Reshard benchmark/doc 临时 worktrees；
- 对应本地 `codex/import-reshard-*` branches；
- 可明确归属本任务的 `/tmp` aggregate 和 storage-audit 目录。

精确清理目标为：

```text
/home/zilliz/Code/milvus-reshard-e2e-bench
/home/zilliz/Code/milvus-reshard-sort-bench
/home/zilliz/Code/milvus-reshard-doc-restructure
/home/zilliz/Code/milvus-import-reshard-clean

codex/import-reshard-e2e-bench
codex/import-reshard-sort-bench
codex/import-reshard-doc-restructure
codex/import-reshard-clean

/tmp/import-reshard-aggregate
/tmp/import-reshard-stderr-summary.txt
/tmp/import-reshard-checksum-candidate.txt
/tmp/milvus-reshard-aggregate-smoke
/tmp/milvus-reshard-aggregate-smoke-all
/tmp/milvus-reshard-aggregate-smoke-final
/tmp/milvus-storage-reshard-audit.BwGKVx
/tmp/milvus-storage-reshard-audit.z9ZIwa
```

主仓库已有 `cmake_build`、`/tmp/milvus-libaio`、全局 Go module/build cache
和 ccache 是共享且早于本测试的资源，只被复用，不能安全归属到本任务，因此
不做全局清理。主 worktree 中与本任务无关的未跟踪研究文档也必须保留。

## 18. 早期 Go benchmark 原型历史报告

本节记录进程级 packed-storage E2E 之前完成的算法原型。它使用
`testing.B`、关闭优化/内联的测试 binary 和内存 RecordReaders，主要用于验证
heap cardinality、reader 生命周期、cursor 正确性和高 fan-in 算法趋势。它不
是生产容量证据，后续结论以第 7–14 节的优化构建 E2E 为准。

### 18.1 环境与保留身份

| 项目 | 值 |
|---|---|
| 临时 worktree | `/home/zilliz/Code/milvus-reshard-sort-bench` |
| 初始 harness commit | `e9aaf4889c4c4bb81d54269eba5ebd3cfe4e1c89` |
| 最终 isolation commit | `e068591d9329bc8a4006a4354726f6c44b9d3d84` |
| 临时 tracked file | `internal/storage/reshard_sort_benchmark_test.go` |
| Go | `go1.26.5 linux/amd64` |
| Build | `-tags dynamic,test -gcflags='all=-N -l'` |
| Benchmark CPU | `-test.cpu=1` |
| Test binary SHA-256 | `8611c06932c94af215bbf50ebb5b2225b857b9de6ca6c2cb2fa7eca64ca99134` |
| Historical README SHA-256 | `11acb71ae35806967f10924f7ebc8727c8c33858cf5edb58917b613b6380de13` |
| Validated summary SHA-256 | `2e7bd670a099be8754825122dc8ff9d7ec0706163a82ea9217cfce71c69373de` |
| Historical artifact checksum-list SHA-256 | `4f1c96fbd32296cb265b34fb0100d31b416f37526abf1ba133db9b0020261b10` |
| Shim source SHA-256 | `805eec7674c13d979f6fb8706735b3052022320beb6e8464388eb5ab6d387e21` |
| Shim library SHA-256 | `a4196267638791052f8a84945180d79cf7a4c6bd34d03458c2df628fcfea319d` |

该临时 branch 和 `_test.go` 从未进入最终 docs branch；报告推送后与 worktree
一起删除。旧 commit 可能仍短期存在于共享 Git object/reflog 中，但没有 live
branch 或 remote ref 指向它们。

上表的 historical artifact digest 不是 tarball hash。它覆盖 artifact 目录顶层
共 11 个文件：`README.md`、`validated-summary.md`、`gc-isolation.txt` 以及
8 个 `*validated.txt`。必须从 worktree 根执行以下命令，因为相对路径文本参与
第二层 hash：

```bash
cd /home/zilliz/Code/milvus-reshard-sort-bench
mapfile -d '' -t ARTIFACT_FILES < <(
  LC_ALL=C find .benchmarks/import-reshard-sort -maxdepth 1 -type f \
    \( -name '*validated.txt' -o -name 'gc-isolation.txt' \
       -o -name 'validated-summary.md' -o -name 'README.md' \) \
    -print0 \
    | LC_ALL=C sort -z
)
test "${#ARTIFACT_FILES[@]}" -eq 11 || exit 1
printf '%s\0' "${ARTIFACT_FILES[@]}" | xargs -0 sha256sum | sha256sum
```

输出即为 `4f1c96fbd32296cb265b34fb0100d31b416f37526abf1ba133db9b0020261b10`。

### 18.2 被测路径与计时范围

| Path | 输入 | 计时内容 |
|---|---|---|
| `current-storage-sort` | 一个无序内存输入流 | 当前 `storage.Sort` 全量排序 |
| `current-storage-mergesort` | K 个已排序逻辑 runs | 当前 `storage.MergeSort` |
| `cursor-one-head-merge` | 相同的 K 个已排序 runs | 每 run 一个 heap head |
| `final-local-external-sort` | K 个无序 chunks | chunk sort、Arrow IPC 写/读、cursor merge |

Timed iteration 会重建所有 readers，输出使用 count-only sink。Sort/MergeSort
内部的 Arrow RecordBuilder materialization 包含在计时内，但 final packed
segment serialization、stats、functions、TEXT/LOB 和对象存储 upload 不在
计时内。

External-sort 的 scratch directory 创建/删除在 timer 停止时完成；IPC 文件
没有 `fsync`，关闭后立即读取，可能命中 page cache。因此它只代表 local
fallback prototype，不代表 durable spill 或 S3。

### 18.3 正确性与隔离控制

- 每个 path、Int64/VarChar PK 都先在计时外逐行验证 PK、ordinal 和 payload；
- oracle readers/writers 全部关闭，scratch 删除后，在 timer 停止时执行
  `runtime.GC()`；
- 每个计时迭代重新创建 readers；
- small correctness test 对每条 path/key 连续执行三个完整生命周期；
- 非法整数参数在 package initialization 时 panic，不静默使用默认值；
- correctness、`TestSort`、`TestMergeSort` 和 append-error 测试连续运行
  `-test.count=5`，全部通过；
- `gc-isolation.txt` 证明 measured work 前存在配对强制 GC，后续 GC 来自计时
  iteration 自身。

完整 correctness 命令为：

```bash
.benchmarks/import-reshard-sort/storage.test \
  -test.run '^(TestImportReshardSortBenchmarkPrototypes|TestReshardBenchInvalidEnvIntegersPanic|TestSort|TestMergeSort|TestMergeSortReturnsRecordBuilderAppendError)$' \
  -test.count=5
```

### 18.4 Benchmark 矩阵

公共命令：

```bash
.benchmarks/import-reshard-sort/storage.test \
  -test.run '^$' \
  -test.bench '^BenchmarkImportReshardSortPaths$' \
  -test.benchmem \
  -test.cpu=1 \
  -reshard-bench-seed=20260721 \
  -reshard-bench-work-dir=/home/zilliz/Code/milvus-reshard-sort-bench/.benchmarks/import-reshard-sort/spill
```

| Matrix | 追加参数摘要 |
|---|---|
| fan-in 4–100 | count=5，benchtime=10x，K=4/8/16/30/100，rows/run=256，payload=16 B，reader batch=256 |
| fan-in 1000 | count=5，benchtime=3x，K=1000，rows/run=256，payload=16 B，reader batch=256 |
| narrow all paths | count=5，benchtime=10x，K=16，rows/run=4096，payload=16 B，output batch=256 KiB |
| wide all paths | count=5，benchtime=10x，K=8，rows/run=2048，payload=1024 B，output batch=1 MiB |

本机没有 `benchstat`。Raw output 按完整、自描述的 benchmark name 分组，每组
五个 `-test.count=5` sample 取中位数并保留 min–max。

### 18.5 Fan-in 4–100

每 run 256 行，payload 16 bytes，reader batch 256 行。单位为 ms/op。

| PK | K | Current MergeSort median [range] | Cursor median [range] | Current/Cursor heap |
|---|---:|---:|---:|---:|
| Int64 | 4 | 1.469 [1.427,1.839] | 0.527 [0.505,0.637] | 1,024 / 4 |
| Int64 | 8 | 3.185 [3.084,3.346] | 1.077 [1.067,1.165] | 2,048 / 8 |
| Int64 | 16 | 7.531 [7.051,7.849] | 2.477 [2.432,2.692] | 4,096 / 16 |
| Int64 | 30 | 14.966 [14.324,15.288] | 4.883 [4.719,5.092] | 7,680 / 30 |
| Int64 | 100 | 68.532 [63.855,69.592] | 25.693 [22.636,26.591] | 25,600 / 100 |
| VarChar | 4 | 1.837 [1.773,2.500] | 0.659 [0.625,0.701] | 1,024 / 4 |
| VarChar | 8 | 4.194 [3.980,4.376] | 1.469 [1.427,1.683] | 2,048 / 8 |
| VarChar | 16 | 10.846 [9.037,12.074] | 3.303 [3.175,3.447] | 4,096 / 16 |
| VarChar | 30 | 22.593 [22.043,23.733] | 10.269 [9.987,10.594] | 7,680 / 30 |
| VarChar | 100 | 89.538 [88.079,98.088] | 34.816 [34.164,38.903] | 25,600 / 100 |

### 18.6 Fan-in 1000

| PK | Path | Median [range] ms/op | B/op | allocs/op | Heap entries |
|---|---|---:|---:|---:|---:|
| Int64 | Current MergeSort | 986.722 [956.414,988.986] | 61,390,029 | 312,021 | 256,000 |
| Int64 | Cursor | 266.511 [262.063,270.052] | 45,524,304 | 50,963 | 1,000 |
| VarChar | Current MergeSort | 1438.317 [1425.273,1472.847] | 98,266,882 | 327,961 | 256,000 |
| VarChar | Cursor | 427.994 [422.763,453.897] | 82,401,088 | 66,901 | 1,000 |

该数据证明缩小 heap 能显著改善当前 synthetic presorted-run 算法，但 K=1000
仍意味着 1,000 个 live readers；不能把此结果解释为生产允许直接打开 1,000
个对象存储 readers。

### 18.7 Narrow all-paths

参数为 K=16、4,096 rows/run、16-byte payload。

| PK | Path | Median [range] ms/op | B/op | allocs/op | Heap | Spill bytes/op |
|---|---|---:|---:|---:|---:|---:|
| Int64 | Current Sort | 38.564 [37.021,40.692] | 15,725,675 | 12,642 | — | — |
| Int64 | Current MergeSort | 130.548 [127.614,135.416] | 12,813,060 | 78,154 | 4,096 | — |
| Int64 | Cursor | 55.111 [52.283,59.527] | 11,610,656 | 12,599 | 16 | — |
| Int64 | External sort | 93.117 [91.459,98.685] | 26,426,763 | 18,077 | 16 | 2,370,176 |
| VarChar | Current Sort | 88.830 [85.984,91.916] | 25,039,865 | 16,988 | — | — |
| VarChar | Current MergeSort | 177.280 [172.716,179.153] | 22,565,072 | 83,783 | 4,096 | — |
| VarChar | Cursor | 78.560 [76.284,81.133] | 21,254,916 | 16,721 | 16 | — |
| VarChar | External sort | 126.978 [121.112,130.948] | 42,161,892 | 22,759 | 16 | 3,615,616 |

### 18.8 Wide all-paths

参数为 K=8、2,048 rows/run、1,024-byte payload。

| PK | Path | Median [range] ms/op | B/op | allocs/op | Heap | Spill bytes/op |
|---|---|---:|---:|---:|---:|---:|
| Int64 | Current Sort | 46.918 [42.026,48.527] | 73,384,117 | 4,306 | — | — |
| Int64 | Current MergeSort | 79.418 [78.875,82.883] | 72,806,125 | 20,685 | 2,048 | — |
| Int64 | Cursor | 47.215 [46.099,57.555] | 72,496,019 | 4,284 | 8 | — |
| Int64 | External sort | 99.397 [96.914,102.959] | 143,847,312 | 9,311 | 8 | 17,114,496 |
| VarChar | Current Sort | 58.545 [55.665,61.115] | 76,066,901 | 5,476 | — | — |
| VarChar | Current MergeSort | 94.011 [91.555,95.213] | 75,570,881 | 21,794 | 2,048 | — |
| VarChar | Cursor | 53.720 [51.370,58.506] | 75,260,784 | 5,393 | 8 | — |
| VarChar | External sort | 120.039 [116.207,121.314] | 148,885,295 | 10,845 | 8 | 17,426,432 |

### 18.9 Whole-process RSS 辅助数据

Narrow Int64 每条 path 以 `benchtime=10x` 单独运行：

| Path | `/usr/bin/time -v` Maximum RSS | User | System |
|---|---:|---:|---:|
| Current Sort | 291,116 KiB | 1.85 s | 0.07 s |
| Current MergeSort | 280,336 KiB | 2.84 s | 0.08 s |
| Cursor | 280,028 KiB | 1.92 s | 0.07 s |
| External sort | 293,788 KiB | 2.52 s | 0.14 s |

这些 RSS 包含较大的 native process baseline、fixture 和计时外 oracle，且 binary
关闭了优化/内联。因此它们只作为方向性诊断，不用于主设计的内存容量结论。

### 18.10 原型结论及其与 E2E 的关系

- 原型正确确认 current MergeSort heap 约为 `K × current batch rows`，
  cursor heap 为 K；
- 在内存 synthetic runs 中，cursor 相对 current MergeSort 明显降低 CPU、
  allocation 和 heap；
- Cursor 不普遍快于 Full Sort：narrow Int64 仍由 Full Sort 更快，wide Int64
  范围重叠；
- VarChar 样本中的 cursor 优势不包含生成 sorted runs 的成本；
- External sort 因 IPC materialization 和重读而具有四条路径中最大的 B/op；
  在全部已测场景都慢于 Full Sort 和 Cursor，但 narrow 场景仍快于 current
  MergeSort，因此不能笼统称为所有路径中延迟最高；同时它没有 `fsync`；
- K=1000 支持 fan-in gate/hierarchical merge，而不是无限 readers；
- 后续优化构建 E2E 进一步证明：heap 缩小不保证总 PSS 降低；K=32 的结果与
  per-reader working set 随 fan-in 增长并抵消 cursor heap 优势这一解释一致，
  但没有 allocation profile 对其做直接归因。
