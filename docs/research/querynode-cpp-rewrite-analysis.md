# Milvus QueryNode C++ 重写方案与复杂度分析

## 一、Executive Summary

将 Milvus QueryNode 从 Go 完全重写为 C++ 是一个**高复杂度**的工程任务。QueryNode 的 Go 代码约 **28,600 行**（82个非测试文件），涉及 7 个子系统。当前架构中，**85-90% 的计算密集型工作已经在 C++ segcore 中执行**（~215K 行 C++），Go 层主要负责分布式编排、生命周期管理和外部依赖集成。

**关键发现**：Milvus 在 standalone 模式下确实存在**进程内直连调用优化**（`InMemResolver`），这意味着纯 gRPC 兼容不够，standalone 模式还需要 CGo/FFI 兼容层。

---

## 二、gRPC 接口分析（分布式模式）

### 2.1 接口定义

QueryNode 的 gRPC 服务定义在 `pkg/proto/query_coord.proto`（第108-188行），共 **33 个 RPC 方法**：

| 类别 | 方法 | 数量 |
|------|------|------|
| 组件管理 | GetComponentStates, GetTimeTickChannel, GetStatisticsChannel | 3 |
| Channel 管理 | WatchDmChannels, UnsubDmChannel | 2 |
| Segment 加载/释放 | LoadSegments, ReleaseCollection, LoadPartitions, ReleasePartitions, ReleaseSegments, GetSegmentInfo, SyncReplicaSegments | 7 |
| 查询/搜索 | Search, SearchSegments, Query, QueryStream(streaming), QuerySegments, QueryStreamSegments(streaming), GetStatistics | 7 |
| 配置/指标 | ShowConfigurations, GetMetrics | 2 |
| 数据分布 | GetDataDistribution, SyncDistribution | 2 |
| 删除 | Delete, DeleteBatch | 2 |
| Schema/索引 | UpdateSchema, UpdateIndex, DropIndex | 3 |
| 文本搜索 | RunAnalyzer, ValidateAnalyzer, GetHighlight, ComputePhraseMatchSlop | 4 |
| 文件资源 | SyncFileResource | 1 |

其中有 **2 个 streaming RPC**：`QueryStream` 和 `QueryStreamSegments`。

### 2.2 关键文件

| 文件 | 作用 |
|------|------|
| `pkg/proto/query_coord.proto` | Proto 定义 |
| `pkg/proto/querypb/query_coord_grpc.pb.go` | 生成的 gRPC 代码 |
| `internal/distributed/querynode/service.go` | gRPC Server 包装层 |
| `internal/distributed/querynode/client/client.go` | gRPC Client 实现 |
| `internal/types/types.go:207-234` | Go 接口定义 |

### 2.3 分布式模式兼容性评估

**复杂度：低**。C++ 重写只需：
1. 用 C++ gRPC 实现 `QueryNode` 服务端的 33 个方法
2. Proto 文件可直接复用，用 `protoc --cpp_out` 生成 C++ 代码
3. Streaming RPC（QueryStream/QueryStreamSegments）在 C++ gRPC 中有成熟支持

---

## 三、Standalone 模式分析（关键发现）

### 3.1 进程内直连机制

**Milvus 确实存在 standalone 模式的函数直调优化**。核心机制如下：

```
internal/registry/in_mem_resolver.go
```

`InMemResolver` 是一个全局单例注册表。QueryNode 在启动时注册自身：

```go
// server.go:394
registry.GetInMemoryResolver().RegisterQueryNode(node.GetNodeID(), node)
```

Proxy 在需要调用 QueryNode 时，通过 resolver 解析：

```go
// internal/proxy/shardclient/manager.go:83-84
func DefaultQueryNodeClientCreator(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
    return registry.GetInMemoryResolver().ResolveQueryNode(ctx, addr, nodeID)
}
```

`ResolveQueryNode` 的逻辑：
```go
func (r *InMemResolver) ResolveQueryNode(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
    qn, ok := r.queryNodes.Get(nodeID)
    if !ok {
        return qnClient.NewClient(ctx, addr, nodeID)  // 回退到 gRPC
    }
    return wrappers.WrapQueryNodeServerAsClient(qn), nil  // 直接调用
}
```

### 3.2 Wrapper 层（`internal/util/wrappers/qn_wrapper.go`）

`qnServerWrapper` 将 `types.QueryNode`（Server 接口）包装为 `types.QueryNodeClient`（Client 接口）。对于普通 Unary RPC，直接转发调用去掉 `...grpc.CallOption`。对于 Streaming RPC，使用 `streamrpc.NewInMemoryStreamer` 在内存中模拟流：

```go
func (qn *qnServerWrapper) QueryStream(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
    streamer := streamrpc.NewInMemoryStreamer[*internalpb.RetrieveResults](ctx, 16)
    go func() {
        qn.QueryNode.QueryStream(in, streamer)
        streamer.Close()
    }()
    return streamer, nil
}
```

### 3.3 对 C++ 重写的影响

**这是一个重大约束**。在 standalone 模式下：

1. Proxy 和 QueryNode 运行在**同一个 Go 进程**中
2. Proxy 通过 `InMemResolver` 直接调用 QueryNode 的 Go 方法，**不经过 gRPC**
3. 如果 QueryNode 用 C++ 重写，有两种选择：

**选项 A：Standalone 也走 gRPC（最简方案）**
- 修改 `InMemResolver`，不注册本地 QN，强制所有调用走 gRPC
- 影响：standalone 模式下多一次 gRPC 序列化/反序列化开销
- 优点：C++ 侧无需任何 CGo 兼容
- 代价：Search/Query 请求可能增加几百微秒延迟（对于向量搜索这种毫秒级操作，影响可控）

**选项 B：CGo 桥接（高性能方案）**
- 用 CGo 将 C++ QueryNode 的 33 个方法暴露给 Go
- Go 侧的 `InMemResolver` 将调用转发到 CGo 函数
- 优点：零序列化开销
- 代价：需要维护 33 个 CGo 桥接函数 + protobuf 序列化/反序列化

**选项 C：C++ Standalone（完整方案）**
- 将整个 standalone binary 也用 C++ 重写（包括 Proxy/Coordinator）
- 代价：工程量级提升数倍
- 不推荐

**建议**：选项 A 作为第一阶段，必要时演进到选项 B。

### 3.4 Standalone 模式下 QN 仍启动 gRPC Server

即使在 standalone 模式下，QueryNode 也会启动自己的 gRPC Server（`internal/distributed/querynode/service.go:168-224`），因为：
- QueryCoord（混合进 MixCoord）仍然通过 gRPC 管理 QueryNode
- 但 Proxy 到 QueryNode 的**查询路径**是直连的

---

## 四、QueryNode 内部架构

### 4.1 代码规模

```
internal/querynodev2/  (28,635行 Go, 82个非测试文件)
├── 根目录          3,391行  (server.go, services.go, handlers.go 等)
├── segments/       9,200行  (segment管理、加载、搜索、CGo桥接)
├── delegator/      6,164行  (分片代理、请求路由、删除缓冲)
├── pipeline/       1,207行  (消息消费pipeline)
├── tasks/            708行  (搜索/查询任务)
├── pkoracle/         569行  (Bloom filter/PK过滤)
├── cluster/          368行  (多节点Worker抽象)
└── collector/        223行  (指标采集)
```

加上依赖的 util 层：
```
internal/util/segcore/   1,587行  (CGo桥接层)
internal/util/initcore/    966行  (C++初始化)
```

### 4.2 核心子系统

#### A. Delegator（分片委托者）— 最复杂的子系统

`ShardDelegator` 是每个 DML Channel 的"分片代理"，职责包括：
- **请求路由**：将 Search/Query 请求拆分到 sealed segments（可能在远程 QN 上）和 growing segments（本地）
- **分布式版本管理**：追踪 segment 在各节点上的分布
- **删除缓冲**：管理流式删除数据，转发到远程节点
- **IDF Oracle**：BM25 全文检索的统计信息维护
- **Partition 统计**：用于 segment pruning

搜索流程：
```
QueryNode.Search(req)
  → delegator.Search(ctx, req)
    → 分割为 sealed/growing segments
    → 并行执行 (errgroup)
      → LocalWorker.SearchSegments()   // 本地 segment → C++ segcore
      → RemoteWorker.SearchSegments()  // 远程 segment → gRPC
    → Reduce 合并结果
```

#### B. Segments（段管理）

- **LocalSegment**：持有 C++ segment 指针（`C.CSegmentInterface`）
- **Loader**：从对象存储（MinIO/S3）加载 segment 数据、索引、delta log
- **Manager**：管理 sealed/growing/L0 segments 的生命周期和引用计数
- 大量 CGo 调用用于创建/搜索/检索/释放 segment

#### C. Pipeline（消息消费管道）

```
消息源(MQ/WAL) → Dispatcher → FilterNode → EmbeddingNode → InsertNode → DeleteNode
                                                              ↓              ↓
                                                      Growing Segments   Delegator
```

#### D. Cluster（集群管理）

`Worker` 接口统一了本地调用和远程 gRPC 调用：
- `LocalWorker`：直接调用本节点的 QueryNode 方法
- `remoteWorker`：通过 gRPC Client 调用远程 QueryNode

### 4.3 Go 特性的重度使用

| Go 特性 | 用途 | C++ 等价 |
|---------|------|----------|
| goroutine + errgroup | 并行 segment 搜索 | std::async / thread pool |
| channel | 消息分发、pipeline 节点间通信 | 线程安全队列 / future |
| context + cancellation | 超时控制、取消传播 | 自定义 CancellationToken |
| sync.RWMutex | segment/collection 并发访问 | std::shared_mutex |
| sync.Once | 单次初始化 | std::call_once |
| atomic | 无锁计数器、版本号 | std::atomic |
| ConcurrentMap | delegators、segments | concurrent_hash_map |
| interface | 抽象 Worker/Segment/Manager | 虚基类 |
| defer | 资源清理 | RAII / scope_guard |

---

## 五、现有 C++ 代码与 CGo 桥接

### 5.1 C++ 代码规模

```
internal/core/src/  (214,835行 C++)
├── exec/         48,692行  (表达式执行引擎)
├── segcore/      45,217行  (segment核心实现)
├── index/        33,932行  (向量/标量索引)
├── common/       26,088行  (通用工具)
├── storage/      21,985行  (多后端存储)
├── bitset/       19,432行  (SIMD位运算)
├── query/         6,579行  (查询计划)
├── mmap/          3,583行  (内存映射)
└── 其他           9,327行
```

### 5.2 C API 头文件

C++ 通过 C API 暴露给 Go，关键头文件：
- `segcore/segment_c.h` — Segment 创建/搜索/检索/删除
- `segcore/collection_c.h` — Collection 管理
- `segcore/plan_c.h` — 查询计划
- `segcore/reduce_c.h` — 结果合并
- `segcore/load_field_data_c.h` — 字段数据加载
- `segcore/load_index_c.h` — 索引加载
- `segcore/tokenizer_c.h` — 分词器
- `segcore/phrase_match_c.h` — 短语匹配

### 5.3 CGo 调用分布

QueryNode 中有 **12 个文件** 直接使用 `import "C"`：
```
querynodev2/server.go           — segcore 初始化、线程池配置
querynodev2/segments/segment.go — segment 创建/搜索/检索（最密集）
querynodev2/segments/segment_loader.go — segment 加载
querynodev2/segments/cgo_util.go — CGo 工具函数
querynodev2/segments/manager.go — 内存统计
querynodev2/segments/utils.go — 工具函数
querynodev2/segments/pool.go — CGo 调用池
querynodev2/segments/load_index_info.go — 索引加载
querynodev2/segments/index_attr_cache.go — 索引属性缓存
querynodev2/segments/trace.go — 分布式追踪
querynodev2/pkoracle/bloom_filter_set.go — Bloom filter
querynodev2/tasks/search_task.go — 搜索任务
```

加上 `internal/util/segcore/`（15个文件）和 `internal/util/initcore/`（3个文件）。

### 5.4 计算占比

| 层级 | 占比 | 内容 |
|------|------|------|
| C++ segcore | 85-90% | 向量搜索(Knowhere)、表达式过滤、索引操作、结果合并、删除过滤(SIMD) |
| Go orchestration | 10-15% | 请求路由、生命周期管理、异步协调、指标采集 |

---

## 六、外部依赖

### 6.1 etcd（服务注册与发现）

QueryNode 依赖 etcd 进行：
- **Session 注册**：`sessionutil.NewSession()` → 向 etcd 注册节点信息
- **服务发现**：`session.GetSessions()` → 发现其他 QueryNode
- **心跳**：通过 etcd lease 维持活性
- **配置监听**：通过 etcd watch 接收动态配置变更

C++ 等价方案：使用 [etcd-cpp-apiv3](https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3) 客户端库。

### 6.2 对象存储（MinIO/S3）

- 用途：加载 segment 数据（binlog、index、delta log、statistics）
- 接口：`storage.ChunkManager`（支持 MinIO S3 协议）
- C++ 等价方案：AWS S3 C++ SDK，或复用现有 `internal/core/src/storage/` 中的实现

### 6.3 消息队列

支持 4 种后端：
- Pulsar（分布式模式默认）
- Kafka
- RocksMQ（standalone 模式默认）
- Woodpecker/WAL（新一代流式系统）

QueryNode 通过 `msgdispatcher.Client` 消费消息，用于增量数据（insert/delete）的实时消费。

**关键文件**：
- `internal/distributed/streaming/msgstream_adaptor.go` — WAL 适配器
- `pkg/mq/msgdispatcher/` — 消息分发器
- `internal/querynodev2/pipeline/` — 消息处理管线

C++ 等价方案：
- Pulsar: [pulsar-client-cpp](https://github.com/apache/pulsar-client-cpp)
- Kafka: [librdkafka](https://github.com/confluentinc/librdkafka)
- RocksMQ: 自定义实现（基于 RocksDB）

### 6.4 Prometheus 指标

QueryNode 暴露大量 Prometheus 指标：
- 节点状态、segment 数量、内存/磁盘使用
- 搜索/查询延迟、QPS
- 流图和 channel 状态

C++ 等价方案：[prometheus-cpp](https://github.com/jupp0r/prometheus-cpp)

---

## 七、配置系统

QueryNode 通过 `paramtable.Get().QueryNodeCfg` 读取配置，约 **60+ 个配置项**：

| 类别 | 示例配置项 |
|------|-----------|
| 调度 | SchedulePolicyName |
| 内存 | LoadMemoryUsageFactor, OverloadedMemoryThresholdPercentage |
| 磁盘 | EnableDisk, DiskCapacityLimit, MaxDiskUsagePercentage |
| Mmap | MmapEnabled, GrowingMmapEnabled, MmapVectorIndex/Field, MmapScalarIndex/Field |
| 分层存储 | TieredWarmup*, TieredMemory*WatermarkRatio, TieredEviction* |
| 索引 | ChunkRows, EnableInterminSegmentIndex, InterimIndex* |
| 线程池 | KnowhereThreadPoolSize, IoPoolSize, HighPriorityThreadCoreCoefficient |
| 生命周期 | GracefulStopTimeout |

配置源：`configs/milvus.yaml` + etcd 动态配置。支持热更新（通过 `Watch` 机制）。

C++ 重写需要：
- YAML 解析（如 yaml-cpp）
- etcd watch 机制接收动态配置
- 或者设计一个轻量级配置桥接层

---

## 八、重写方案设计

### 8.1 推荐架构

```
┌─────────────────────────────────────────────────────┐
│                  C++ QueryNode Binary                │
│                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ gRPC     │  │ Delegator │  │ Segment Manager  │  │
│  │ Server   │──│ (Shard   │──│ (直接调用segcore) │  │
│  │ (33 RPCs)│  │  Router)  │  │                  │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│                     │                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ Pipeline  │  │ Cluster  │  │ Session/Config   │  │
│  │ (MQ消费) │  │ Manager  │  │ (etcd client)    │  │
│  └──────────┘  └──────────┘  └──────────────────┘  │
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │          现有 C++ segcore (215K行)             │  │
│  │  (Search, Retrieve, Index, Expression, ...)   │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### 8.2 Go-side 需要修改的部分（Standalone 兼容）

为了 standalone 兼容，Go 侧需要做最小修改：

```go
// internal/registry/in_mem_resolver.go 修改
func (r *InMemResolver) ResolveQueryNode(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
    // 如果QN是C++实现，始终走gRPC
    return qnClient.NewClient(ctx, addr, nodeID)
}
```

或者保留现有逻辑，只是 C++ QueryNode 不调用 `RegisterQueryNode`，让 resolver 自然回退到 gRPC。

---

## 九、复杂度评估

### 9.1 模块级复杂度

| 模块 | Go代码量 | 复杂度 | 预估C++代码量 | 说明 |
|------|---------|--------|-------------|------|
| gRPC Server 层 | 430行 | **低** | ~500行 | 直接使用 protoc 生成 |
| 核心服务逻辑 (services.go) | 1,794行 | **中** | ~2,500行 | 33个RPC方法的实现 |
| Delegator 子系统 | 6,164行 | **高** | ~10,000行 | 最复杂，涉及分布式状态管理 |
| Segment 管理 | 9,200行 | **中** | ~6,000行 | 大部分已在C++ segcore，去掉CGo层反而简化 |
| Pipeline | 1,207行 | **中** | ~2,000行 | 需要实现MQ消费和消息处理 |
| Cluster Worker | 368行 | **低** | ~500行 | gRPC client 调用 |
| 配置系统 | ~500行 | **低** | ~800行 | YAML + etcd watch |
| Session/服务注册 | ~300行 | **低** | ~500行 | etcd session 管理 |
| 指标/监控 | 332行 | **低** | ~400行 | Prometheus metrics |
| PK Oracle | 569行 | **低** | ~600行 | Bloom filter |
| **合计** | **~21,000行** | | **~24,000行** | |

### 9.2 需要引入的 C++ 第三方库

| 库 | 用途 | 成熟度 |
|----|------|--------|
| gRPC C++ | RPC 框架 | 成熟 |
| protobuf C++ | 序列化 | 成熟 |
| etcd-cpp-apiv3 | 服务发现/配置 | 中等 |
| pulsar-client-cpp | 消息消费（Pulsar） | 成熟 |
| librdkafka | 消息消费（Kafka） | 成熟 |
| prometheus-cpp | 指标暴露 | 成熟 |
| yaml-cpp | 配置解析 | 成熟 |
| AWS S3 SDK / 已有storage | 对象存储 | 成熟/已有 |
| spdlog | 日志 | 成熟 |

### 9.3 难点分析

#### 难点1：Delegator 的并发状态管理
Delegator 维护了 segment 分布的版本化视图、删除缓冲区、IDF 统计等复杂状态。Go 的 goroutine + channel + sync.RWMutex 提供了良好的并发原语。C++ 需要用 `std::shared_mutex`、`std::condition_variable`、线程池等实现同等功能。

#### 难点2：消息队列多后端支持
需要实现 Pulsar/Kafka/RocksMQ 三种 MQ 后端的消费者，以及新的 WAL 系统适配。每个后端的 seek/subscribe/consume 语义不同。

#### 难点3：Pipeline 流式处理
Go 的 channel 天然适合构建处理管道。C++ 需要用线程安全队列或 actor 模型实现类似的 pipeline 架构。

#### 难点4：优雅停机
Go 的 context cancellation + sync.Once + lifetime state machine 实现了精细的优雅停机。C++ 需要等价的状态机和协调机制。

#### 难点5：测试兼容性
现有 46 个测试文件（约 20,500 行测试代码）需要在 C++ 侧重写或通过集成测试覆盖。

### 9.4 简化因素

1. **消除 CGo 开销**：现有架构中，每次 Search/Retrieve 都要经过 Go → CGo → C++ 的边界。C++ 重写后直接调用 segcore，消除了 CGo 的上下文切换和栈复制开销。

2. **segcore 直接集成**：当前 12 个 CGo 桥接文件 + `util/segcore/` 15个文件可以完全删除，直接调用 C++ API。

3. **内存管理更精确**：消除 Go GC 对延迟的影响，内存使用更可控。

4. **现有 C++ 基础设施**：storage 层、protobuf 定义、CMake 构建系统都可复用。

---

## 十、实施路线图

### Phase 1：基础骨架（4-6周）
- [ ] C++ gRPC Server 搭建，注册 33 个 RPC 方法（stub 实现）
- [ ] etcd session 注册/服务发现
- [ ] 配置系统（读取 milvus.yaml + etcd 动态配置）
- [ ] 基础生命周期管理（Init/Start/Stop/Health check）

### Phase 2：Segment 管理（4-6周）
- [ ] Segment Manager 实现（直接调用 segcore C++ API）
- [ ] Segment Loader 实现（对象存储读取 + segment 加载）
- [ ] Collection Manager 实现
- [ ] 索引加载和管理

### Phase 3：查询引擎（6-8周）
- [ ] Delegator 子系统实现（最复杂）
  - 分布式 segment 分配追踪
  - 搜索/查询请求拆分和路由
  - 删除缓冲区管理
  - IDF Oracle
- [ ] Cluster Worker 实现（本地 + gRPC 远程）
- [ ] 结果合并和归约

### Phase 4：消息消费（4-6周）
- [ ] MQ 消费者实现（至少支持 Pulsar + Kafka）
- [ ] Message Dispatcher 实现
- [ ] Pipeline 架构（Filter → Embedding → Insert → Delete nodes）

### Phase 5：集成和测试（6-8周）
- [ ] 与现有 Milvus 集群的兼容性测试
- [ ] 性能基准测试（对比 Go 版本）
- [ ] Standalone 模式兼容性验证
- [ ] 滚动升级测试

### 总预估：6-9 个月，3-5 名工程师

---

## 十一、额外需要考虑的因素

### 11.1 滚动升级兼容性
C++ QueryNode 必须与 Go QueryNode **可以混合部署**。gRPC 接口兼容即可保证这一点。但需要注意：
- protobuf 字段的向前/向后兼容
- 新版本新增的 RPC 方法需要优雅处理 `Unimplemented` 错误（现有代码已有此模式）

### 11.2 Plugin 机制
Go 版 QueryNode 支持通过 `plugin.Open()` 加载 `.so` 插件（`initHook()`），用于参数调优。C++ 版本可以用 `dlopen/dlsym` 实现类似机制，但接口需要重新定义。

### 11.3 Tracing（分布式追踪）
Go 版使用 OpenTelemetry gRPC interceptor 实现追踪。C++ 版需要集成 [opentelemetry-cpp](https://github.com/open-telemetry/opentelemetry-cpp)。segcore 中已有 trace 支持（`C.CTraceContext`）。

### 11.4 QN-to-QN 通信
Delegator 可能将请求转发到**其他 QueryNode**（远程 sealed segment）。C++ QueryNode 需要实现 QueryNode gRPC **Client** 来调用远程节点。

### 11.5 Streaming Node 嵌入模式
Milvus 支持 StreamingNode 嵌入 QueryNode 运行（`sessionutil.EnableEmbededQueryNodeLabel()`）。C++ 重写后，这个模式可能需要特殊处理。

### 11.6 内存管理
Go 版本依赖 GC + `debug.SetGCPercent` + GC Tuner 管理内存。C++ 需要：
- jemalloc（已在 segcore 中使用）
- 显式的内存水位线管理
- OOM 保护机制

### 11.7 日志系统
Go 版使用 zap (structured logging)。C++ 可使用 spdlog 或复用 segcore 中的 glog。需要保持日志格式兼容以便运维工具分析。

### 11.8 HTTP 管理接口
QueryNode 还暴露 HTTP 健康检查和管理端点。C++ 版本也需要实现。

---

## 十二、成本收益分析

### 收益
1. **消除 CGo 开销**：每次 Search/Retrieve 减少 ~10-50μs 的 CGo 边界开销
2. **消除 GC 停顿**：Go GC 停顿在高负载下可达 1-10ms
3. **更精确的内存控制**：避免 Go 内存分配器与 C++ jemalloc 的冲突
4. **统一技术栈**：便于 segcore 团队直接优化端到端性能
5. **减少二进制大小**：无需链接 Go runtime

### 成本
1. **开发工作量**：6-9 个月，3-5 名工程师
2. **测试投入**：需要重建全部测试覆盖
3. **维护成本**：两个版本的维护过渡期
4. **人才要求**：需要熟悉分布式系统和 C++ 的工程师

### 建议
考虑到 **85-90% 的计算已在 C++**，Go 层的主要开销是 CGo 和 GC。建议先做以下优化评估：

1. **量化 CGo 开销**：Benchmark 当前 CGo 调用的实际耗时占比
2. **量化 GC 影响**：Profile GC 停顿对 P99 延迟的实际影响
3. 如果两者总和 < 5% 的查询延迟，可能不值得全量重写
4. 如果 > 10%，则 C++ 重写有明确收益
