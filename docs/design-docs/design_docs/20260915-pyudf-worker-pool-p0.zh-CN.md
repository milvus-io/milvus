# PyUDF Worker Pool P0：实施契约

- **日期：** 2026-09-15
- **状态：** P0–P5 已完成；Go client、Proxy Search 与真实 worker 链路已验证，P6/P7 待执行。
- **关联：** [开发计划](20260914-pyudf-worker-pool-plan.zh-CN.md) / [中文设计](20260914-pyudf-worker-pool.zh-CN.md) / [英文设计](20260914-pyudf-worker-pool.md)

本文固定 P1–P7 使用的启动、协议、错误及关闭契约。仅 Proxy、本地 Python supervisor、多 worker 共享端口、同步 Execute、无业务队列和运行期间不按请求强制取消的范围不变；Milvus 退出时直接终止 workers。以下默认值是实现基线，不是性能调优结论。P3 已提供独立的服务启停，尚不表示 Proxy Search 已接通。

## 1. 配置与预算

配置统一位于 `function.pyUDF`，由 Go paramtable 读取一次有效快照，第一版不动态刷新。Python 只接收 Go 传入的 argv。

| 配置 | 默认值 | 校验与用途 |
|---|---|---|
| enabled | false | 使用 paramtable 布尔解析，解析失败按 false；禁用检查在 once 外，禁用不启动 Python |
| address | `127.0.0.1:19090` | v1 接受具体本机 IPv4 字面地址和 1–65535 端口；拒绝通配地址、端口 0、域名、URL、IPv6；扩展地址格式另行版本化 |
| rpcTimeout | 30s | 1ms–24h；从 client.Execute 入口计时，包含编码、连接、传输、解码；调用者更早的 deadline 优先 |
| connectionPoolSize | 10 | 正整数；进程内共享唯一一组固定数量的独立 ClientConn，连接数不限制 RPC 并发数 |
| maxMessageBytes | 67108864（64 MiB） | 1 MiB–1 GiB；完整 Execute 请求/响应的 Protobuf 消息上限，两端 send/receive 相同 |
| server.workerCount | 1 | 正整数；配置启动的 worker 数，包含启动中和 stopping 的存活进程 |
| server.grpcConcurrency | 10 | 正整数；每 worker 的 gRPC executor 线程数 |
| server.maxConcurrentRPCs | 100 | 正整数；每 worker 接纳的在途 RPC 上限，包括 executor 队列中的请求 |
| server.shutdownTimeout | 30s | 1s–10min；Python 终止 worker 的升级期限，不等待 executor。Go 只发送 SIGTERM 并等待 supervisor；等待受调用者 ctx 限制，唯一 Wait goroutine 继续回收。不做启动就绪等待 |

连接数、worker 启动数和 worker gRPC 并发数均在 `configs/milvus.yaml` 的 `function.pyUDF` 下配置，默认连接数为 10、worker 数为 1、gRPC 执行线程数为 10、在途 RPC 上限为 100。Go client 收到 Execute 调用后直接发起 RPC，不设置在途调用上限、并发额度、信号量、限流或请求队列，也不根据 worker 数推导客户端容量。连接数仅控制复用连接数量，同一连接允许多个并发 RPC。全部请求并发接纳由 worker 的 gRPC 原生机制决定：`ThreadPoolExecutor(max_workers=grpcConcurrency)` 与 `maximum_concurrent_rpcs=maxConcurrentRPCs`，不增加额外 admission gate、自定义并发计数或执行池。默认最多执行 10 个 handler，在途 RPC 达到 100 前可在 executor 内置队列等待；排队时间包含在原 RPC deadline 中。

固定协议限制：错误说明最多 8 KiB UTF-8；参数逻辑最大深度由 worker 检查，仍为 64；Go 不做重复参数校验，protobuf 解码深度超限可在 handler 前以 gRPC INTERNAL 拒绝。每请求至少一个 query chunk，不设固定 query 数量上限，至少一个输入列，允许零个输出列；输入/输出不设固定列数上限，仍受完整消息大小限制；每 query 行数为非负 int64，解码前校验不会溢出平台整数或实际 IPC 布局。零行 query 必须保留并调用 UDF；零 query 不进入 Execute，由上游空结果路径处理。内部调用产生零 query 时返回 ServiceInternal(5)，不猜测未知输出 schema。

发送端默认不启用压缩；接收端采用 Arrow reader 的接受规则，可读取库支持的压缩流。大小上限由 gRPC 对完整 Protobuf 消息执行，不再手工扫描 IPC buffer/metadata。上述限制约束单次 RPC 的消息；Go 不限制在途调用总数，不宣称客户端总缓存或 RSS 有全局上限，也不能限制用户模型和 UDF 自行分配的内存。

传给 Python 的参数固定为：`--address`、`--worker-count`、`--grpc-concurrency`、`--max-concurrent-rpcs`、`--max-message-bytes`、`--shutdown-timeout-ms`；时间均为整数毫秒，配置要求可精确表示。不传入父进程 PID。`enabled`、`rpcTimeout`、`connectionPoolSize` 不传入 Python。Go 使用 PATH 中安装的 `python3`，与现有 runtime 镜像一致；使用 `exec.Command` 参数数组和 `python -I -m milvus_pyudf_runtime.supervisor`，不通过 shell 或 Python 环境配置覆盖。

## 2. 监听端口与平台范围

Go 和 Python 启动入口不检查操作系统名称；运行依赖 fork、信号等系统接口，Linux/macOS 是当前目标平台。Go 直接启动 supervisor，supervisor 保持单线程和仅标准库导入，在 fork 后由各 worker 创建 gRPC server，使用 SO_REUSEPORT 监听配置地址。supervisor 不 bind/probe/reserve 端口，也不创建 abstract socket、文件锁或其他端口标记。

部署负责为每套 pool 分配独占地址。开启 SO_REUSEPORT 后，两套 pool 误配同一个端口可能同时监听，而不是报冲突；Go Health/Check 只确认配置地址上的服务可用，不验证响应来自哪个 pool。不提供进程归属握手或重复 pool 检测。普通的不可绑定地址仍由 worker 的 gRPC bind 失败触发启动失败。

macOS 的端口复用分流行为与 Linux 不同，不能据 workerCount 承诺均匀分配或线性吞吐；默认 workerCount 仍为 1。[gRPC 多进程示例](https://chromium.googlesource.com/external/github.com/grpc/grpc/+/HEAD/examples/python/multiprocessing/server.py)

父进程必须保持当前仅标准库、单线程的约束，不在 fork 前初始化 gRPC、PyArrow、用户模型或系统框架。Python 对 macOS fork 的系统库线程风险有明确提示，因此 macOS 原生测试仍是平台验收的一部分。[Python 进程启动说明](https://docs.python.org/3.12/library/multiprocessing.html#contexts-and-start-methods)

## 3. 保留 Health 接口，不做主动检测

每个 worker 仍注册标准 `grpc.health.v1.Health/Check`，服务名为 proto 定义的 `milvus.proto.pyudf.PyUDFWorker`，空服务名也可查询。接口仅报告被访问 worker 的本地 gRPC 服务状态；未知服务名返回 NOT_FOUND。不启用 Watch。

Go 和 supervisor 都不主动调用 Health/Check，不汇总整池就绪，不使用 mmap、READY 标记或启动就绪超时。`server.startupTimeout` 及对应 CLI 参数已经删除。Health 与 Execute 共用同一个 gRPC server/executor，按需查询也使用 grpcConcurrency 个线程并受 maxConcurrentRPCs 接纳上限限制。

Go 的 StartSupervisor 在 enabled=true 时通过进程级 once 创建 Python 进程，cmd.Start 成功即返回。成功只表示进程已创建，不保证 Python 初始化完成、worker 已监听或 UDF 可执行。配置/exec 启动错误被缓存；进程稍后退出不会改写先前成功的启动结果，由唯一 Wait 收集退出状态。取消启动调用的 ctx 不终止已创建的服务。

supervisor 只跟踪 PID 和 waitpid：退出的 worker 回收后退避补齐，fork 失败也退避重试。存活但未监听或卡住的 worker 不被主动识别、终止或替换。首次 RPC 可能遇到 UNAVAILABLE 或超时，按正常 RPC 失败处理，不增加等待就绪、队列或重放。

## 4. Execute 协议

### 4.1 完整请求与响应

业务接口仅提供 unary Execute，另注册标准 grpc.health.v1.Health/Check。Go 的 ExecuteRequest 适配对象继续持有 ResourceName、UDFPath、Stage、Params、Inputs；wire 消息把 Arrow 输入编码为一个 bytes 字段。

| ExecuteRequest 字段 | 约定 |
|---|---|
| resource_name | 非空 UTF-8，说明性资源名，不参与 Python 路径拼接 |
| udf_path | Go 从 FileResource 取得的完整绝对 .whl 路径 |
| stage | 原样传入 UDF 的 PyUDFContext.stage；worker 不校验取值，空串和空白也保留。Milvus 当前调用方使用 L2_rerank |
| params | FunctionParamObject，缺省为空 object，保留 bool/int64/double/string/bytes/array/object 语义 |
| inputs | 唯一输入列的 Arrow IPC stream；发送端每 query 一个 RecordBatch，接收按 Arrow reader 解析 |
| input_column_indices | 参数位置到 IPC 列的零基索引；重复索引共享数组，空列表表示 IPC 原列顺序 |

每个 RecordBatch 包含该 query 的唯一输入列，发送端字段名 c0、c1……；接收端通过 input_column_indices 还原全部参数位置，重复参数共享解码数组。所有 batch 共用 schema，零行 batch 必须保留；输入仍需至少一个 batch；IPC 解析遵循 Arrow reader，不额外要求 EOS、不拒绝流结束后的尾随数据、不手工检查压缩或内部布局。query 数、每 batch 行数和列类型由 IPC 自身表达，不再重复声明布局、片段序号或累计大小。

worker 收到整个请求并完成输入校验后，按完整路径/stage 取得共享 UDF 实例。短锁定位缓存条目后释放；在条目锁内检查实例，存在则返回，否则创建并缓存，失败不写入实例，随后释放锁。每模块导入锁在进程内共享；等待实例锁或模块锁期间检查当前 RPC，factory 在模块锁之外执行。不提供用户回调 loader 的嵌套加载，不设计完成事件或依赖图。取得实例后顺序执行各 query，验证全部结果再一次返回，执行不增加 runtime 串行锁。

ExecuteResponse 的 oneof result 必须为 outputs 或 error。outputs 为独立的完整 Arrow IPC stream；输出 batch 数量和行数不与输入强制匹配，同一输出 stream 的 schema 一致、同一 batch 内各列等长。当前 transform_query 按输入 batch 分别调用 UDF，但允许每次返回任意行数；返回零列时编码为零行空 batch。由上层表达式/MapOp 检查业务所需的对应关系；空 bytes 不表示成功。

error 仅包含 ErrorCode code 和 string message。code 是执行错误类别，由 Go 映射到现有 merr；操作、query 和列位置等上下文写入 message，client 不解析这些文本。RPC 本身关联请求和响应，错误时不附带部分输出。

成功要求响应选择 outputs、IPC 校验全部通过、gRPC OK，且返回前 ctx 仍有效。无 result、未知 result 或损坏的成功输出归为内部错误；结构化 error 按第 5 节映射。最终非 OK gRPC 状态按传输失败处理；调用 ctx 已取消/超时时以 ctx 为准。没有分片消息、客户端半关闭或额外结束标志。

两端 gRPC send/receive 使用同一 maxMessageBytes，默认 64 MiB，包含 Protobuf 元数据；客户端编码完整输入后发起一次 RPC。超限直接失败，不拆分、不重放。IPC 编解码临时引用由各自调用负责，返回后不留下读取未保留输入的后台操作。当前实现尚未发布，本次直接替换原分片草案，不提供旧草案的兼容模式。

### 4.2 版本与重试

第一版不提供能力查询 RPC，也不在 ExecuteRequest 中添加 protocol_version。兼容性由 protobuf 字段演进和 gRPC 服务/消息定义维护：已删除字段的编号/名称 reserved，其余字段编号保持不变；兼容扩展新增字段，破坏性变更使用新的服务或消息定义。Arrow IPC 格式、输出类型及固定限制仍是双方契约，不能通过添加版本整数自动解决语义差异。stage 原样传给 UDF；Health/Check 仅供按需查询，不参与启动流程。

client 禁用策略重试、hedging、wait-for-ready 和业务重放；固定连接轮询只发生一次。grpc-go v1.82.1 的 WithDisableRetry 不禁止“无 transport/对端未处理”透明重试，`stream.go:shouldRetry` 在 disableRetry 检查前处理它。契约是**不自动重放可能已进入 UDF 的调用**，不是一次网络尝试或全系统 exactly-once。后续不得用新的 service config/interceptor 引入 Execute 重试。[gRPC 重试说明](https://grpc.io/docs/guides/retry/)

## 5. 错误分类与来源审查

### 5.1 Worker 错误到 Go

ExecuteError 仅含 code 和 message。ErrorCode 是 worker 协议分类，不是 Python 异常类名或 Milvus 数值码。worker 在错误产生处确定类别，Go 根据 code 转换；未知 code 或 ERROR_CODE_UNSPECIFIED 作为 ServiceInternal(5) 处理。无需独立的 phase、I/O 子类型、query/column 索引字段。

```proto
message ExecuteError {
  ErrorCode code = 1;
  string message = 2;
}
```

| 产生点 / ErrorCode | Go 映射 / Search Status.Code | 分类 / Status.Retriable | 后续注入断言 |
|---|---|---|---|
| 用户导入/factory/transform 普通异常、wheel/输出契约失败；UDF_FAILED | ErrFunctionFailed / 2400 | 保留现有 System / false | 坏 ZIP、缺入口、包冲突、用户异常、decimal32、不支持的输出类型、同一输出 batch 列长不一致或跨 batch schema 不一致；decimal32 在序列化前拒绝 |
| runtime 直接读取 wheel 发现文件不存在；RESOURCE_NOT_FOUND | ErrIoKeyNotFound / 1000 | System / false | 注入 ENOENT，不能变成用户执行异常 |
| runtime 直接读取 wheel 权限不足；RESOURCE_PERMISSION_DENIED | ErrIoPermissionDenied / 1005 | System / false | 注入 EACCES，与文件不存在保持区别 |
| runtime 直接读取 wheel 的其他 I/O 失败；RESOURCE_IO_FAILED | ErrIoFailed / 1001 | System / false | 注入其他 OSError，保留读取操作上下文 |
| 内存分配失败；OUT_OF_MEMORY | ErrServiceMemoryLimitExceeded / 3 | System / false | 可控 MemoryError；真实 OOM kill 走 transport，不能声称拿到 3 |
| 保留的不支持能力类别；UNSUPPORTED | ErrServiceUnimplemented / 10 | System / false | 当前 handler 无此产生点；后续 client 用受控响应验证映射，服务/方法不存在走 gRPC 状态 |
| 内部请求/IPC 契约损坏、runtime invariant、结果编码失败或未知内部异常；INTERNAL | ErrServiceInternal / 5 | System / false | 内部错误不标用户参数错；支持类型编码失败不能伪装 UDF_FAILED |

ErrorCode 的协议编号为 0（未指定）、1（UDF_FAILED）、2（RESOURCE_NOT_FOUND）、3（RESOURCE_PERMISSION_DENIED）、4（RESOURCE_IO_FAILED）、5（OUT_OF_MEMORY）、6（UNSUPPORTED）、7（INTERNAL），以 pyudf.proto 为准。这些编号不直接写入 Search Status；Go 的映射保留既有 merr 码。

UDF 自行抛出的 OSError 属于 UDF_FAILED；runtime 读取 wheel 的 OSError 才按来源选择三个 RESOURCE 错误码。普通用户异常不重建 worker。MemoryError 单独处理；BaseException（例如 SystemExit）在执行边界受控报告 INTERNAL；用户自行 os._exit 或 native crash 只能由进程/transport 路径处理。初始化失败回滚中的 close 异常不覆盖原错误；Milvus 退出不调用缓存实例的 close。

2400 保留现有 System 分类，不全局修改 ErrFunctionFailed，不因错误来自 Python 自动标记 InputError。请求的错误输出或异常与内部系统故障仍需在产生处区分，移除 phase 字段不等于忽略来源。

P2 loader 已在打开/读取 wheel 的位置区分 OSError 与包格式/内容错误；`_import_module`、`_load_factory`、factory 和属性访问的普通异常包装为 UDF_FAILED，MemoryError 单独传播。PyUDFInstance.execute_query 的前置 params/columns 检查属于 INTERNAL，用户返回契约和普通调用异常属于 UDF_FAILED。worker 按异常携带的来源 code 编码，不解析 message。定位信息按需写入 message，例如“transform_query query=2 column=0: unsupported decimal32”。

### 5.2 Go、框架及启动失败

| 产生点 | Go / Search 映射 | Retriable / 行为 |
|---|---|---|
| Execute 中空资源名、未设置的参数值、worker 参数深度超限 | worker INTERNAL → ServiceInternal 5，System | false；由 worker 校验 |
| 非法 UTF-8 或 protobuf 解码深度超限 | gRPC INTERNAL → ServiceInternal 5，System | false；handler 可能不执行 |
| FileResource 快照未就绪 | ServiceUnavailable 2，System | true；本 client 仍不重试 |
| 已就绪快照中请求名称不存在 | 保持现有 ParameterInvalid 1100，InputError | false |
| 内部 LocalPath 缺失/非法 | worker INTERNAL → ServiceInternal 5，System | false；由 worker 校验 |
| nil 输入、列 chunk 不一致 | ServiceInternal 5，System | false；Go IPC 编码前拒绝 |
| gRPC RESOURCE_EXHAUSTED，没有结构化 ExecuteError | ServiceResourceInsufficient 12，System；来源记录为 TRANSPORT_RESOURCE_LIMIT_UNKNOWN | true；可能是并发或消息限制，不能描述成“队列满”或靠字符串细分 |
| gRPC UNAVAILABLE / worker 退出 / 连接中断 | ServiceUnavailable 2，System | true；不改连接重放，原请求可能已经执行 |
| gRPC UNIMPLEMENTED | ServiceUnimplemented 10，System | false |
| gRPC DEADLINE_EXCEEDED / CANCELED | 保留或转换为 context.DeadlineExceeded / context.Canceled，再 merr.Wrap；10001 / 10000 | false；不能包进新的 5/2400 导致 code 被覆盖 |
| 其他非 OK gRPC 状态、畸形结果、缺 result、未知 Error | ServiceInternal 5，System | false；保留原始 gRPC status 为 cause/上下文 |
| Go 配置或启动命令构造非法 | ServiceInternal 5 | 初始化失败；没有 Search 响应、没有启动重试 |
| Python 进程 exec 失败 / 等待 supervisor 退出时观察到异常 | ServiceUnavailable 2 | 只缓存进程创建错误；稍后退出不改变 Start 的结果，Python 日志记录原因 |

同为 RESOURCE_EXHAUSTED 的框架拒绝没有可靠业务来源字段。v1 明确保留“不确定资源限制”这一类，既不伪造并发归因，也不改写 grpcio 内部接纳代码。消息大小由 gRPC 限制，完整 IPC 布局由执行端校验。库连接错误先有意识地转换成 typed merr，后续只用 merr.Wrap/Wrapf 增加上下文。

### 5.3 当前 Go 到 SDK 的路径及限制

截至本次代码审查，执行错误沿以下路径传递：

| 层 | 已读代码及结论 | 实施时的要求 |
|---|---|---|
| Python 对象 → worker | [loader.py](../../../internal/util/function/pyudf/python/milvus_pyudf_runtime/loader.py)、[instance.py](../../../internal/util/function/pyudf/python/milvus_pyudf_runtime/instance.py)；P2 已接入 worker handler | worker code 路径已通过源头审查与 RPC 故障测试；完整 Go client/Search 投影由 P4/P5 验证 |
| client → expr | [pyudf_expr.go](../../../internal/util/function/chain/expr/pyudf_expr.go) 的 Execute 保留 merr/context；未分类 client 错误转 ServiceInternal(5) | P5 已移除旧 2400 fallback；成功 wire 解码发现布局损坏按 5；用户返回契约在 Python 拒绝 |
| expr → MapOp | [operator_map.go](../../../internal/util/function/chain/operator_map.go) Execute 原样返回执行 err；之后独立校验目的列数、score 数值类型和 null，产生 2400 | null score 保留 MapOp 归属；worker 没有目的列名，不能提前拒绝所有 null |
| MapOp → FuncChain | [chain.go](../../../internal/util/function/chain/chain.go) ExecuteWithOptions 使用 merr.Wrapf 保留执行错误 | 不转为 ParameterInvalid 或 stringify |
| FuncChain → Search task | [search_pipeline.go](../../../internal/proxy/search_pipeline.go) rerankOperator.run、Node.Run、pipeline.Run 及 [task_search.go](../../../internal/proxy/task_search.go) PostExecute 返回原错误 | 核对 pipeline 后续没有执行/部分输出发布 |
| task → Search Status | [task_scheduler.go](../../../internal/proxy/scheduler/task_scheduler.go) Notify(err)；[impl.go](../../../internal/proxy/impl.go) WaitToFinish 失败用 merr.Status(err) | 断言 Code、Retriable、ExtraInfo，不只比对 Reason |
| Status 投影 | [merr/utils.go](../../../pkg/util/merr/utils.go) Status/Code/oldCode；[errors.go](../../../pkg/util/merr/errors.go) 定义 | 2400 默认 System/false；10000/10001 来自 context；不改全局 sentinel 或旧码表 |
| SDK | 本机 pymilvus 的 client/utils.py check_status → exceptions.py MilvusException.from_status 保留 code、compatible_code、is_input_error、retriable | P7 用打包支持的 SDK 版本重复验证；本机 checkout 审查不代表所有 SDK |

同时审查了 `retry.Do` 消费者、`retry.Handle` 和 Search 的 LB 路径。当前 L2 Execute 位于 querynode 检索重试之后的 PostExecute；Proxy.Search 的 retry.Handle 只因 InconsistentRequery 重试该类失败，以上 UDF 码不触发此分支。有 function rerank 时检索优化被关闭，不能把成功路径的查询补偿重跑算成 UDF 失败重放。

本机 pymilvus `retry_on_rpc_failure` 对 rate limit/连接管理器处理的异常及部分外层 gRPC 失败仍可能重发 Search，不只是读取 Status.Retriable。其他 SDK 或用户也可重发。**禁止重放的保证止于 Go PyUDF client，不覆盖 SDK→Proxy 断链后的整个 Search**；本版没有跨请求去重，不能承诺用户 UDF 副作用只发生一次。

## 6. 按信号退出

Go 的 supervisor.go 只负责创建进程与发送退出信号。Proxy.Init 在其他初始化成功后调用 StartSupervisor；Proxy.Stop 在关闭调度器之前调用 StopSupervisor，向子进程发送一次 SIGTERM，由唯一 cmd.Wait 回收 supervisor。停止失败会记录日志，并在完成 Proxy 其他清理后返回错误。

Python supervisor 收到 SIGTERM/SIGINT 后停止补齐，向 worker 发送 SIGTERM；超过 shutdownTimeout 尚未退出则 SIGKILL，waitpid 回收后自身退出。worker 按默认信号行为直接结束，不等待 executor、用户 close 或 finally。

不再读取父 PID、轮询 Milvus/supervisor 存活状态或传入 parent-pid 参数。Python 服务可以独立运行，由其启动者管理信号。若 Milvus/Proxy 或 supervisor 被 SIGKILL、崩溃等绕过正常退出路径，子进程不会自动跟随退出，需要部署环境另行清理。普通 RPC 超时仍不触发进程终止。

## 7. 实施与验收状态

P1/P2 已完成协议、配置和单 worker。P3 已完成 StartSupervisor/StopSupervisor、worker 直接监听端口、真实 fork、worker 退避补齐与 waitpid 回收。启动不等待 Health，已删除整池共享状态。Go 通过唯一 Wait 回收 supervisor，并验证进程创建失败、异步退出、正常关闭和显式信号退出。Proxy 启停已接线，完整 Search 由 P4/P5/P7 完成。

| 验收组 | 必须覆盖 | 阶段 |
|---|---|---|
| S：启动 | 进程创建后立即返回、不发 Health 请求、不改写异步退出结果、创建失败缓存、取消已创建服务不受影响、一次启动 | P1/P3/P5 |
| O：监听 | Linux/macOS 直接监听、绑定失败、同端口重复 pool 可能共存的边界、显式信号退出 | P3/P5/P7 |
| W：wire | 空参数/重复位置、零行和多 batch、完整消息超限、截断 IPC、缺 result、outputs/error 互斥 | P1/P2/P4 |
| E：错误来源 | 第 5 节各类错误从源头到 code/merr/Search Status/SDK；健康检查失败保留系统归责 | P2/P4/P5/P7 |
| L：生命周期 | Proxy.Stop 发送 SIGTERM、父进程消失不会自动退出、显式信号退出、停止不补齐、SIGKILL 升级、waitpid 实际回收 | P2/P3/P5/P7 |
| R：连接 | 默认连接/worker/并发为 10/1/10；Health 临时连接结束即关闭；Execute 无本地并发门槛、不业务重放 | P4/P5/P7 |

先前 P0 的 pipe/READY codec 实验不再作为当前生命周期方案的验证证据。单次 RPC 超时不结束 Python handler 的 gRPC 基础结论仍需在所固定的 1.74.0 上验证。Python 的历史 EOF/父 PID 实验不代表当前按信号退出的契约。

协议字段号以 [pyudf.proto](../../../pkg/proto/pyudf.proto) 为准，标准 Health 使用 gRPC 官方 health/v1 定义，不复制到该 proto。Python 依赖固定为 grpcio、grpcio-tools、grpcio-health-checking 1.74.0，protobuf 6.31.1，PyArrow 23.0.1。P2/P3 已验证独立 worker 执行和 Go 服务启停；P4 已实现生产 Execute client，完整 Search 已在 P5 接入并通过 standalone 验证。参数逻辑深度上限之外还存在 Protobuf 自身的消息递归上限，深层参数可在 handler 前被 gRPC 以 INTERNAL 拒绝；不能据此声称任意 64 层参数都已支持。

输入 IPC 仅编码唯一的 Arrow 列；Go 按共享列对象身份去重，通过 input_column_indices 记录每个 UDF 参数对应的 IPC 列。Python 按索引恢复参数顺序，重复参数引用同一个解码数组；空索引列表沿用 IPC 原列顺序。移除本地 IPC 编码预算，仅保留两端 gRPC 的完整消息大小限制。Go 与 worker wheel 必须配套更新，旧 worker 不识别列引用。
