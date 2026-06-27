# 流式 Iterator(Query / Search)

- **Created:** 2026-06-26
- **Author(s):** @xiaofanluan
- **Status:** Under Review
- **Component:** Proxy / QueryNode / milvus-proto / SDK
- **Related Issues:** #50842
- **Released:** N/A

## 摘要(Summary)

把 `QueryIterator` / `SearchIterator` 从「客户端重复 unary 分页」改造成「**基于 gRPC 流的 pull 模型 + handler 端 heap 归并**」,使大范围扫描的内存占用与结果总量解耦;同时为普通 `query()` 增加一个**增量强制的 1G 单响应硬上限**,取代当前在全量物化之后才检查 `maxOutputSize` 的做法。

## 动机(Motivation)

当前一个大扫描(如 `query(expr="tenant_id == X", output_fields=["id"])` 或深翻的 search iterator)既不安全也不高效:

- 对外 `Query` / `Search` 是 **unary**。Proxy 在 `task_query.go` 里把所有结果收进 `resultBuf` → `toReduceResults` → 一次性 `pipeline.Execute` 归并后才返回。
- `quotaAndLimits.limits.maxOutputSize`(默认 100MB)是唯一硬上限,但它在 `reduceRetrieveResults` 里、**全量物化之后**才检查——能让请求失败,但**挡不住物化时的内存尖峰 / OOM**。
- Delegator 侧普通 `Query`(`delegator.Query` 用 `executeSubTasks` 收齐全部子任务结果)**无行/字节上限、无背压**,与 `delegator.QueryStream` 形成对比。
- `QueryIterator` / `SearchIterator` 本质是**客户端分页**:带 PK/offset 游标的重复 unary 调用,每页仍在 proxy 全量物化。

因此目前**没有真正端到端的流式查询**,也没有 query 版的背压(对照 delete 的 `complexDeleteLimitEnable`)。

## 设计目标 / 非目标

**目标**

- 大扫描时,proxy/querynode 峰值内存与结果总量**解耦**。
- 用单条流式连接 + handler 端 heap 归并,取代逐页 unary 往返。
- 给单次(非 iterator)query 响应加一个增量强制的硬天花板。

**非目标(v1,刻意保持简单)**

- **search 不做有状态的 ANN 续传**(不从索引做有序 refill)。
- **不做 view 漂移恢复**:会话断开即让 iterator 失败(见「恢复语义」)。
- **不把 streamingnode 作为独立 producer**:growing 数据走现有 delegator 路径,不单独进 heap;仅用普通流式 gRPC。

## 整体设计(Design Details)

详细设计见下文分节:拓扑、归并模型、query/search 游标、归并地平线不变式、服务端会话与 view 钉住、生命周期与回收、恢复语义、硬上限。

### 1. 拓扑

```
client --stream--> iterator handler --stream--> querynode delegator --> workers/segments
                   (heap k-way 归并)             (per-session 游标 + 钉住的 view)
```

handler 终结客户端的流,做 k-way 归并;每个 querynode delegator 是一个有序 producer。v1 中 handler 默认落在 proxy。

### 2. 核心模型 —— pull 驱动的 k-way heap 归并

- 每一轮,每个 querynode 返回一批**局部有序**的候选。
- handler 维护一个归并 heap(每个 producer 一个 head),pop 出全局最优 → emit;**只有当某 producer 的 buffer 见底时,才向它 pull 下一批**(背压)。
- handler 工作集 = `O(#producer × batch)`,**与结果总量无关**。

### 3. Query iterator

- 归并 key = **PK**,游标 = 上次的 PK 位置。
- querynode **保持一个真实的扫描位置**(落在排序后的 segment 上)→ 真正「续上次」,**无重扫重叠**。
- handler 按 PK 归并,并在归并时跨 shard 去重。

### 4. Search iterator

- 归并 key = **score / distance**,游标 = 上次的 **radius** 边界。
- **不做 ANN 有状态续传**:每一轮都是一次带 radius 约束的全新 `search`,querynode 只记住 radius 这个值。这就是现有 radius-bounded 模型,只是改成走流。

### 5. 归并地平线(唯一的正确性不变式)

每轮每个 producer 至多返回 N 条,所以 handler **只能安全输出到「各 producer 本轮返回的最大 key」的最小值**为止:

- query(PK 序):node A 本轮回到 PK=100、node B 回到 PK=200 → 本轮只能输出 PK ≤ 100;游标推到 100;>100 的下一轮重取。
- search(score 序):只能输出 `distance < min(radius_A, radius_B)`;这条边界**就是** radius。

游标推进必须**夹在这条地平线上**,否则会漏 / 重。轮边界处的重叠只是重算,**不会产生错误结果**。

### 6. 服务端 iterator 会话(querynode)

- 每个 iterator 会话持有:游标(query 是 PK 位置 / search 是 radius)+ 一个**钉住的 view**(session_ts 上的 MVCC 快照)。
- 该 view 在会话生命周期内不推进,所以会话是一种**必须被释放的资源**。

### 7. 生命周期与 view 钉住

- 显式 **Open → Next* → Close**;Close 释放 view。
- **lease/TTL 兜底回收**:被遗弃的 iterator(客户端崩了、没 Close)必须自动释放 view,否则 view 低水位——以及 compaction/GC——会被卡住。这是**硬要求,不可选**。
- **view-pin 记账**:view 可推进水位 = 所有存活 iterator 钉住的最老 view;需暴露「谁钉着、钉了多久」,以便排查 / 杀掉长命 iterator。

### 8. 恢复语义 —— 决定:失败即重来(方案 b)

- querynode 会话丢失时(重启 / rebalance / segment handoff),钉住的 view + 游标都没了。
- iterator **直接失败**,客户端从头重来。
- 保证单个 iterator 全生命周期落在**单一一致 view** 上,不处理 view 漂移。为简单起见选此方案(对 scan / 导出类业务,给出明确契约后可接受)。

### 9. 硬上限

- **单次(非 iterator)query 响应上限 1G**,**增量强制**——边累加边计 running size,越过上限即 **cancel 流并返回错误**(截断即报错),绝不静默返回部分数据。cancel 透传同时 bound 住 querynode 的工作量。
- iterator 显式**总量无界**;1G 仅作用于单次 unary 响应,不是 iterator 会话总量上限。
- 后续考虑:proxy 级的**全局 in-flight 字节预算**,避免多个并发大 query 相加打爆内存(N × 上限)。

## 涉及组件(Components)

- `internal/proxy`:iterator handler、heap 归并、地平线游标、1G 强制。
- `internal/querynodev2/delegator` 及 segments:per-session 游标、钉住的 view、lease/回收。
- `milvus-proto`:流式 iterator RPC + Open/Next/Close。
- SDK:流式 iterator 客户端。

## 实施阶段(Phasing)

1. 端到端流式 **query iterator**(PK 游标、真续扫)+ handler heap 归并 + 地平线;复用 delete 的 `QueryStream` 通道。
2. querynode 上的 **iterator 会话生命周期**:钉住的 view、Open/Next/Close、lease/TTL 回收、view-pin 记账。
3. 流式 **search iterator**(radius-bounded,不做 ANN 续传)。
4. 普通 `query()` 的 **1G 增量硬上限**(截断即报错)。

## 测试计划(Test Plan)

- 单测:proxy 端 heap 归并、地平线游标推进、跨 shard 去重、1G 增量截断。
- 集成:多 segment / 多 querynode 下 query & search iterator 的端到端正确性(无漏、无重)。
- 边界:轮边界地平线、数据倾斜(单 node 命中大量行)、Close / lease 回收释放 view、恢复失败路径(b)。
- 压测:大扫描的 proxy/querynode 峰值内存随结果总量保持有界;view-pin 在长命 iterator 下的水位行为。

## 已知待定项(Open Items)

- iterator 会话 lease/TTL 的最终取值与回收机制。
- view-pin 水位记账的运维可见性接口。
- handler 落 proxy 还是流式端点(实现细节;v1 默认 proxy)。
