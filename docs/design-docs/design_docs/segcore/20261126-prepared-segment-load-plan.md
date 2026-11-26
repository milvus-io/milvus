# 基于 LoadDiff 的单段加载资源估算

状态：设计与实现草稿。日期：2026-11-26。

## 本期目标

C++ 用实际执行的 LoadDiff 估算单个 sealed segment 加载时可能新增的资源峰值，以及加载完成后的资源占用。Go 继续负责加载请求调度、资源预留、准入、GPU 显存检查和最终可服务状态。

本期不引入 C++ pacer、AcquireLoadPermit、跨 segment 共享资源账本或动态扣减其他正在加载任务的预留；也不调整 committed 和 committed + RSS 两类准入约束。

## 流程

```text
Go 准备未发布的 segment / pin 已有 segment
  → C++ PrepareLoadPlan
      捕获元数据和状态身份
      读取规划元数据
      生成 LoadDiff
      按加载操作估算 loading 和 loaded
  ← plan handle + 内存/磁盘估算
Go 合并 Go 自有开销和 GPU 需求，使用现有策略 reserve
  拒绝 → 释放 plan 和未发布的 segment
  通过 → C++ ExecuteLoadPlan
           校验状态 → 执行同一 LoadDiff → Finalize → Publish
         Go 完成既有后续步骤
         等加载子任务结束并清理后释放预留
```

Prepare 和估算在逻辑上是一项操作；实现可以在 Go 封装内部使用异步规划加一次纯计算 C API，避免把大小估算的内部 protobuf 改成持久化元数据。

## 资源语义

- `loading`：相对已经存在的资源，执行计划可能额外需要的峰值，包含新资源和临时缓冲。旧索引被替换时，旧资源尚未释放，不能先减掉其占用。
- `loaded`：目标 segment 的预计最终占用。它不是第二笔预留，不能简单加到 loading 上。
- 这些值是按当前元数据与运行时配置计算的估算，不是实测 RSS，也不承诺精确覆盖插件内部未知分配。
- Go 可应用准入 ratio；本期复用现有策略，不新增配置。ratio 小于 1 表示超额承诺，不能再称为硬上界。
- 同一段内 field/index 的并发、mmap、流式临时预算由 C++ 估算。不同 segment 的需求保守相加，不依赖“此刻其他段正在加载”的快照。
- GPU 显存检查留在 Go。当前检查读取各卡占用并模拟放置，并不是跨请求的 GPU reservation。本期不增加 `gpu` / `gpu_count` C ABI 数组。

## 计划与生命周期

```cpp
struct PreparedLoadPlan {
    Segment* owner;                 // Go 保护其生命周期
    weak_ptr<const State> base;     // 不保留旧运行时资源
    SegmentLoadInfo target;
    FieldLayout fields;
    LoadDiff diff;
    bool consumed;                 // 单次执行，受单段更新锁保护
};
```

准备阶段只在捕获状态时短暂持更新锁。读取 manifest 或等待 Go 准入时不持更新锁，不持查询 read lease，也不固定 cache cell。

Execute 在单段更新锁内检查状态身份。如果发生变化，计划作废并返回可重试错误；禁止在旧预留内重算更大的 diff。旧计划必须释放，重新规划和准入。成功发布的资源属于 segment，不由 ReleasePlan 销毁。

取消使用现有 CFuture/OpContext 机制；必须等待 native 操作退出后再释放 plan、segment pin 和加载预留。不同 segment 的更新锁互相独立。

## 规划元数据与实际读取

Prepare 允许读取固定版本 manifest，以及旧 V2 列组成员目录；不读取字段值或索引 payload、不构建运行时 reader/index、不进行预热。

`LoadPlanningMetadata()` 明确表示可能读取元数据；`GetColumnGroups()` 只访问已准备的缓存。已发布元数据不会由查询线程触发隐式读取。

字段大小或加密切片信息不足时保留保守估算，不把未知状态默认为未加密。本期不修改 meta/loadinfo 的持久化 proto。

## Go 与 C++ 的实际边界

| 工作 | 归属 |
|---|---|
| sealed field/index 的计划、执行、内部发布 | worker segcore |
| 节点准入、GPU 检查、跨段调度、取消和重试 | worker Go |
| delta 下载、解析、组装及调用 native 应用删除 | worker Go + segcore，保留现有顺序 |
| sealed bloom / BM25Stats | delegator Go 的 PK/IDF oracle |
| manager 注册和 distribution 可见性 | Go |

worker 和 delegator 可能在不同节点，不能把 bloom/BM25Stats 全部混入 worker 的 native 资源估算。

外部集合 reopen 当前先切换 native 视图，再执行 Go 删除回放，回放成功后更新 Go load info。本期保持该行为，不宣称提供跨 Go/C++ 的原子提交；后置失败也不能当作 native 从未发布。

## 无状态调用方

QueryCoord 没有本地 segment 实例，继续使用无状态估算入口，复用底层大小公式。growing 和其他不走 prepared sealed load 的调用路径保持既有协议。

## 必须验证

1. 未变化字段和索引不计入新增加载峰值；替换资源不提前抵扣旧占用。
2. 初次加载、空 diff、默认字段、索引替换、字段重载。
3. V1/V2/V3、manifest 元数据读取、旧 V2 成员信息补齐。
4. mmap、eager/lazy 混合投影、预算启用/关闭、加密状态未知。
5. 准入拒绝、取消、状态过期和部分失败时正确释放资源。
6. Go 的 GPU 检查、delta 执行顺序和 delegator stats 生命周期保持。
7. 无状态 QueryCoord 估算继续可用。

## 参考

- [LoadDiff 加载设计](20260204-loaddiff_based_segment_load.md)
- [Reopen 发布与查询 lease](../20260720-segment-reopen-request-read-lease-drain-design.md)
- 源码基线：`34aa0be4eb`。实现和验证进度见本地工作区，本文不代表测试已通过。
