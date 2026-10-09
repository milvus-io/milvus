# Milvus REST 请求超时统一：设计约束与实现审查

审查日期：2026-10-08。

审查基线：`0acf11fde8a215668b131f932159fc8d522c1670`，本地 `test-master`。
下列 finding 对应修改前的代码基线，均为 **pre-existing**；后续章节另记 timeout 改造草案的进度。草案已接入部分生产路径，但完整 Proxy 构建、E2E、JSON 单元上界和性能验收尚未完成，不能部署。

## 1. 已批准的行为与边界

### 1.1 一份总预算

`readHeaderTimeout` 和 `overallTimeoutBudget` 是前后相接的独立阶段。前者从 HTTP transport 开始读取 header 到完整 header 为止；后者从完整 header 的时间 `t0` 起，直到服务端响应写出完成。连接尚无请求时的 keep-alive 空闲时间不计入任一请求预算。TLS 握手、协议识别发生在请求之前，但也必须有入口保护。HTTP/1 keep-alive 的 Go 标准库特例见下文：等待下一请求最初 4 字节时由连接空闲期限保护。

`overallTimeoutBudget` 覆盖 header 之后的 body、认证、准入、排队、RPC、重试、数据转换、JSON 编解码、内存缓冲与实际写回，**不包含 header 读取时间**。阶段切换、请求转发和重试不得重新发放预算。客户端网络发送前和收到响应后的应用处理不属于服务端可控制的范围。

```text
headerDeadline = headerStart + readHeaderTimeout
serverDeadline = t0 + overallTimeoutBudget
effectiveDeadline = min(serverDeadline, t0 + Request-Timeout, inheritedDeadline)
```

两项配置分别须为正数，没有大小顺序约束。客户端 `Request-Timeout` 与服务端 overall 同从 `t0` 起算；客户端传入值可以大于服务端配置，但生效值被 `min` 截断，不能延长服务端上限。后两个可选 deadline 缺失时不参与 `min`。Header 尚未解析时只使用短 header 防护，不把 REST overall 应用于尚无法区分的 gRPC。完整 header 后，仅 REST 请求启动整体预算。

### 1.2 保留的策略

| 配置 | 唯一职责 | 与总预算的关系 |
| --- | --- | --- |
| `overallTimeoutBudget` | 从完整 header 到响应写出完成的请求时长，服务端配置 | 与 header 阶段顺序相接；客户端不能延长 |
| `readHeaderTimeout` | header 读取阶段的短上限 | 独立保护，不计入 overall |
| `maxConnectionIdleInterval` | 无活动请求/stream 时回收 HTTP 连接 | 不属于单次请求预算 |

废弃 `readTimeout`、`writeTimeout` 的独立预算配置；保留网络 read/write deadline 作为执行机制。不能在新传输层保护可用之前删除现有保护。

2026-10-09 决策修订：本轮不引入 `maxIOIdleInterval`。统一绝对截止时间必须解除 pending body read 和 response write；即使客户端持续缓慢传输，也不能越过总预算。单次请求 I/O 停滞的提前清理只是未来可选优化，若负载证明确有必要，再独立评估 HTTP/2 请求级进展事件及成本。此调整不影响无活动请求时的连接回收。

### 1.3 取消不是强杀

Deadline 同时约束请求 context 和传输层 I/O，但 Go 不会强杀任意 goroutine。CPU 循环、锁等待、同步插件与编解码库必须提供合作式退出或可验证的有界执行单元。

因此必须分别验收：

1. 到期后不再继续正常响应，阻塞 I/O 被解除。
2. 请求内工作实际停止，资源在有界时间内释放。

只满足第一项不能称为“全流程资源退出已完成”。用额外 goroutine 包裹不可取消调用然后放弃等待，也不能满足第二项。取消写入请求不代表已提交的写入回滚；不改变已有 mutation/DDL 持久化语义。

### 1.4 协议范围

目标覆盖 Proxy 对外 REST V2，以及启用时的 `/v1`；V1 纳入预算属于显式兼容变化。健康检查、metrics/debug 端口及 `/api/v1` 控制台不自动纳入本次 REST 数据接口改造。外部 gRPC 保留自己的 deadline/stream 生命周期。

共享端口在请求类型识别之前使用公共握手/header 防护，但不能把 REST 总预算设置成整条 HTTP/2 TCP 连接的寿命。完整 header 之后，一个 REST stream 到期不得终止其他健康 stream。2026-10-09 用户确认的例外：若 HTTP/2 初始 HEADERS/CONTINUATION 块始终未完成，解析器无法跳到其他 stream 的帧；短 header 限制到期时允许关闭该连接。这是尚未建立完整请求的协议层阻塞，不是 `maxConnectionIdleInterval` 的 keep-alive 回收，也不得扩大为普通 stream 超时即关闭连接。

## 2. 行业惯例：采用、调整、拒绝

- **采用**：header 短保护、post-header 请求/stream 预算、连接空闲回收分层。Go `ReadHeaderTimeout` 独立保护 header，读完后由 handler 决定 body 期限；Envoy 也区分 header、stream lifetime 和 route timeout，但各自起点不同，不直接照搬某个字段。[Go 官方文档](https://pkg.go.dev/net/http#Server.ReadHeaderTimeout)、[Envoy 官方说明](https://www.envoyproxy.io/docs/envoy/latest/faq/configuration/timeouts)
- **采用**：传递剩余 deadline，服务端工作合作式响应取消，不在每个阶段重置时间。[gRPC 官方说明](https://grpc.io/docs/guides/deadlines/)
- **采用**：HTTP/1 使用连接 I/O deadline，HTTP/2 使用请求 stream 的 deadline；通过原始 ResponseWriter 或正确的 `Unwrap` 链控制 I/O。[Go ResponseController](https://pkg.go.dev/net/http#ResponseController)
- **调整**：Envoy 的 stream idle timeout 解决长期/流式请求无进展问题；本轮 REST 非流式请求已有从完整 header 到写回的绝对预算，不照搬该独立计时器。若将来确需提前清理停滞 stream，不能把 HTTP/2 connection 活动误当单个请求的进展。
- **拒绝**：用 `http.TimeoutHandler` 或当前“独立 timer + handler goroutine + 全量响应缓存”作为全流程方案；它们不能单独覆盖 pre-handler 接收与不可取消工作。
- **证据边界**：上述分层原则有成熟实践；当前 Go/Gin/x/net 组合下，HTTP/2 header 独立防护和完整 header 时间的传播仍须传输层验证。这不是 Gin middleware 单独提供的能力。

## 3. 当前实现审查

所有位置基于上述 commit。严重级别采用 `milvus-review` 的影响模型；实施顺序不等于缺陷严重级别。

### F1 — P1：业务超时没有约束实际 body read 和最终 socket write

- **Location**：基线版本的 [`timeout_middleware.go` 第 258、300 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/httpserver/timeout_middleware.go#L258)、[`service.go` 第 264 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/service.go#L264)。
- **Boundary**：请求 context、Gin binder、真实 ResponseWriter。
- **Provenance**：pre-existing；审查的是基线实现本身。
- **Failure scenario**：客户端发送完整 header 后停止发送 body。V2 binder 等待 body；middleware 可以先取消 context，但没有设置 body 的 I/O deadline。另一方向，handler 在预算内完成，而客户端停止接收大响应；`CommitTo` 已进入真实 `Write`，不再参与 `select` 的超时仲裁。默认 server read/write timeout 都为 `0s`。
- **Evidence**：Gin v1.11.0 `context.go:878` 使用 `io.ReadAll(Request.Body)`；当前 middleware 仅设置 context deadline，未调用 ResponseController；`CommitTo:191` 直接写真实 writer。Go 的网络 deadline 能解除 pending I/O，但本路径默认未配置它们。
- **Test gap**：现有 timeout 单测使用 `httptest.NewRecorder`，不能制造 socket backpressure 或被阻塞的网络 body。
- **Safe path**：在原始传输 writer 上执行统一绝对 deadline；用真实 TCP 与 HTTP/2 flow-control 测试验证读写解除、handler 完成及资源归还。

### F2 — P1：存在 REST handler 之前的无界接收窗口

- **Location**：基线版本的 [`listener_manager.go` 第 100 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/listener_manager.go#L100)、[`service.go` 第 265 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/service.go#L265)。
- **Boundary**：TCP、cmux、h2c adapter、HTTP server、Gin。
- **Provenance**：pre-existing。
- **Failure scenario**：共享端口连接只发送 HTTP/2 preface 的前缀，cmux 持续等待，其连接尚未交给 http.Server，`readHeaderTimeout=5s` 尚不能保护这里。独立 REST 入口的 h2c Upgrade 请求发送 header 后停在 body，同样可以在进入 REST middleware 前等待。
- **Evidence**：Milvus `cmux.New` 后没有配置 `SetReadTimeout`；cmux v0.1.5 `cmux.go:78` 默认 `readTimeout=0`，`matchers.go:190` 循环读取 preface。x/net v0.58.0 `h2c/h2c.go:180` 在调用下层 handler 前 `io.ReadAll(r.Body)`。默认 HTTP ReadTimeout 为 0。
- **Test gap**：现有 h2c/TLS/共享端口测试验证连接和健康检查成功，未验证停在协议识别或 Upgrade body 的连接释放。
- **Safe path**：先建立完整入口阶段测试；复用短入口保护而不是新增一串用户配置。对 cmux/h2c 的处理必须保持 gRPC 和已有协议支持，不得只在 Gin 内补 timer。

### F3 — P2：编解码和转换工作缺乏统一取消检查

- **Location**：基线版本的 [`handler_v2.go` 第 440 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/httpserver/handler_v2.go#L440)、[`utils.go` 第 899、4087 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/httpserver/utils.go#L899)、[`json_render.go` 第 32 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/httpserver/json_render.go#L32)。
- **Boundary**：JSON decode、逐行转换、JSON encode、响应 buffer。
- **Provenance**：pre-existing。
- **Failure scenario**：body 已全部在内存中或 RPC 已返回大量结果，预算随后耗尽。转换函数不接收 context；encoder 可以继续计算，直到一次完整编码结束或尝试写入已关闭的 recorder。
- **Evidence**：`checkAndSetData`、`buildQueryResp` 的签名与行循环没有 context；`jsonRender` 调用无 context 的 `Encode`。`internal/json` 使用 Sonic v1.15.2，原生 `StreamEncoder.Encode` 先 `EncodeInto` 到内存再写 writer。Gin 默认 JSON renderer 也是先 Marshal 再 Write。
- **Test gap**：已有 late-write 测试证明输出隔离，没有证明转换/编码及时退出；没有单个巨型字符串、向量或同步插件阻塞用例。
- **Safe path**：REST 自有循环增加取消检查；编解码采用可中断或有界工作单元，并实测最坏退出延迟。不能用 writer 上的 context 检查冒充内部 JSON 计算可取消。

### F4 — P2：客户端预算缺少上限和范围校验；迁移不能视为改名

- **Location**：基线版本的 [`timeout_middleware.go` 第 241 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/internal/distributed/proxy/httpserver/timeout_middleware.go#L241)、[`http_param.go` 第 158 行](https://github.com/milvus-io/milvus/blob/0acf11fde8a215668b131f932159fc8d522c1670/pkg/util/paramtable/http_param.go#L158)。
- **Boundary**：HTTP `Request-Timeout`、paramtable、Go time.Duration、客户端契约。
- **Provenance**：pre-existing。
- **Failure scenario**：客户端设置大于服务端默认值的秒数可以延长预算；0/负数会立即到期；足够大的整数虽能通过 ParseInt，乘以 time.Second 时仍可能溢出。
- **Evidence**：只执行 ParseInt 后直接覆盖默认值。`handler_v2_test.go:669` 明确测试 32s header 可覆盖 30s 默认值；`tests/restful_client_v2/api/milvus.py:15` 使用 120s。
- **Test gap**：invalid header 测试仅有 `3.5`、`abc`，未覆盖正数范围、duration 溢出和服务端硬上限。post-header 起点与短客户端预算的兼容性也尚未验证。
- **Safe path**：纯策略函数计算 deadline；正整数与溢出校验；保留秒单位。明确发布迁移说明与硬上限取值，不能直接把原 30s 默认值宣称为兼容的硬上限。

## 4. 已确认安全与不能过度推断的边界

- 当前代码已经用 `context.WithTimeout` 传播 deadline，不应报告为“没有下游 deadline”。Proxy Search/Query 从 `TraceCtx().Deadline()` 填入 `TimeoutTimestamp`。
- `TaskCondition.WaitToFinish` 响应 `ctx.Done()`；这证明调用方停止等待，不证明每个任务内部和 C++ 工作都停止。
- 当前 recorder 有 mutex、closed 检查、独立 header 和复制的 Gin context。不能因存在后台 handler 就直接声称 buffer use-after-free 或 Gin context 数据竞争。本轮不提出此类 race finding。
- `CommitTo` 后 timer 不是立刻被 Stop；准确问题是进入该分支后不再选择 timer。
- 启用非零 server read/write timeout 确实可解除阻塞 I/O，不是无效配置。
- Gin v1.11.0 的标准 writer 提供 `Unwrap`；当前自定义 timeout recorder 不提供。ResponseController 应在替换成 recorder 之前使用，不能忽略 `ErrNotSupported`。

## 5. 实施前必须解除的验证缺口

### G1：HTTP/1 与 HTTP/2 的阶段边界

Go HTTP/1 的 `ConnState(StateActive)` 回调发生在 `readRequest` 返回以后，不是读取 header 之前；这正适合独立 header 防护后的 post-header 预算。`ConnContext` 的连接建立时间不能当每个复用请求的开始时间。当前 HTTP/1 请求在 handler 入口启动 overall，与完整 header 后的派发阶段相接，不虚构先前 header 的实际耗时。

2026-10-09 新增真实 TCP probe `TestTransportHTTP1KeepAliveFirstFourBytesPrecedeHeaderGuard`（连续三轮通过）：同一 HTTP/1 keep-alive 连接的第二次请求只发送 `G`，停 90ms 后补齐；即使 `ReadHeaderTimeout=30ms`，仍得到正常 204。Go 1.26.6 的 `server.go` 在下一次请求前先执行 `bufr.Peek(4)`，之后才设置新的 header deadline；此前等待受 `IdleTimeout` 而非短 header 限制。这是 Go 实现细节，不是 HTTP 协议规定的四字节机制。用户决定不为这一窄窗口增加连接读取跟踪器：HTTP/1 keep-alive 前四字节继续由 `maxConnectionIdleInterval` 保护。该阶段也不属于 post-header overall。其他 HTTP/1、TLS、cmux、pipelining 路径仍须验收。

x/net HTTP/2 `processHeaders` 接收的是已解析完成的 `MetaHeadersFrame`。读取 CONTINUATION 的过程发生在 `frame.go:1707` 的 Framer 内，handler/stream deadline 在后面才建立。普通 `http.Handler` API 本身没有暴露每个初始 HEADERS 的开始时间。

需要验证两件独立行为：完整 header 之前的短保护，以及完整 header 后的请求预算。现有 `ReadHeaderTimeout` 字段不能未经测试就宣称保护了每个 HTTP/2 header block。HTTP/2 的初始 HEADERS 到 END_HEADERS 必须连续；不完整块若无限等待，既不能交给 handler，也不能跳过它继续解析连接上的其他 stream。实现必须在完整 header 之前启动短 header 防护；若截止仍未完成，可按上述例外关闭连接。只收到 HTTP/2 frame header 的前几个字节时尚不能判断是否为 HEADERS，需要连接级的短协议解析保护。完整 header 的时间 `t0` 必须随 stream 保留，包括 handler 排队期间；后续到期仅影响本 stream。具体协议扩展形式及其依赖维护成本仍需审查，不以连接级 deadline 代替正常 stream 的 deadline。

Task 1 修改前实测（Go 1.26.6 / x/net v0.58.0 / cmux v0.1.5）：native-free `requestbudget/transport_probe_test.go` 当时验证了首请求和复用请求的 StateActive 均晚于 header 完成；h2c 的 HEADERS 缺 CONTINUATION 时，等待超过外层 ReadHeaderTimeout 仍可随后补齐并进入 handler；完整 header 也可在 handler 排队中等待。该测试文件随后改为验证新 fork 的期望行为，不能再把当前测试通过当作旧行为证据。cmux 的 HTTP2()/Any() 顺序下 SetReadTimeout 会把半截 preface 交给 Any()，不能直接当作 fail-closed 入口保护。G1 仍是生产接入门槛，不能标记完成。

2026-10-09 实施进度：经用户批准，已在工作树引入仅供 Proxy HTTP server 使用的 `x/net@v0.58.0` scoped fork，详见 `h2transport/UPSTREAM.md`。新增 header/frame 读取短保护；当 HEADERS/CONTINUATION 未完成时，使用短 header 上限截止，无法安装 read deadline 时 fail-closed；完整 header 后清除连接级 header deadline，并把完成时间放入 stream context，以便 handler 排队仍计入 post-header budget。HTTP/1 在 handler 入口计 post-header budget，h2c Upgrade 在外层 HTTP/1 handler 入口计时以覆盖升级前 body 读取。`overall >= header` 约束和固定倒扣 5s 的错误估计均已删除。真实 h2c 网络 probe 和 requestbudget 包测试通过；完整 Proxy 包和 E2E 尚未验证，不能宣称全流程保证已通过发布门槛。

### G2：每个请求 I/O 进展（本轮不再是发布门槛）

HTTP/2 的连接 writer、stream flow control、buffered data 具有不同进展语义。必须证明：一个 stream 没有进展时，不会被另一个 stream 或 PING 保活；流式大 Write 有持续有效进展时，不会错误触发 idle cutoff。连接级 `WriteByteTimeout` 不能单独完成这件事。

Task 1 实测：ResponseController 的 HTTP/2 body/write deadline 可解除停滞且保留同连接其他 stream；1 MiB 单次应用 Write 尚未返回时，客户端已收到 DATA，因此应用 Write 完成不是进展事件。当前公共 API 无法可靠实现可滑动的请求级 idle 策略。2026-10-09 修订后本轮不实现该策略，G2 作为将来可选优化的证据保存，不阻断 post-header 绝对 deadline 的实施。G1 的协议入口完整验收仍是门槛。

### G3：全流程执行退出

API-key 验证 `internal/proxy/util.go:1521` 调用 `VerifyAPIKey(rawToken)`，没有 context 参数；第三方插件具体实现不在本轮证据范围。Sonic/Gin 的整体 JSON 调用也没有 context。必须列出这些边界，选择合作式接口/有界执行，或者明确升级为需要批准的进程隔离/依赖变更；不得承诺通过 WithDeadline 强杀它们。

2026-10-09 选择：本轮不修改 Sonic 内部。大 JSON 按有效结构边界拆成有大小上限的编解码单元，在 Sonic 调用之间检查同一 context；单行/单值超限的容量拒绝与 timeout 是正交策略。对已接受的单元允许实测、有界的 deadline overshoot，不能宣称在 Sonic 单次调用中精确停止。用户选择草案单元上限为 **4 MiB**，仍需兼容性与真实部署验证。当前 V2 bulk insert/upsert 在首轮 data[] 解码前检查每行原始 JSON 大小；大 body 使用至多 1 MiB 的正常解码批次，小于 1 MiB 的整个 body 保持原 Gin 快路径。HTTPReturn 和 HTTPReturnStream 的新 renderer 在新请求路径中深入普通 string-key map，对找到的数组逐元素编码，并在每次 Sonic 编码后检查实际字节长度；输出侧仍缺少编码前的通用容量证明，不能称 CPU overshoot 已严格由 4 MiB 限定。V1 和非 bulk V2 JSON 解码仍有单次不可取消调用。

2026-10-09 后续基准：V2 bulk 的行分割已从 gjson 全量扫描改为 pinned Sonic AST 浅层扫描；整个 envelope 再由 Sonic Valid 校验，数据行按至多 1 MiB 常规批次解码，单行可至 4 MiB。约 9.6 MiB、8192×128 的合成请求在 M1 Pro、Go 1.26.6 禁优化测试构建中，原整包 Sonic 解码约 53 ms / 117 MB alloc；旧 gjson 批次方案约 168 ms / 105 MB；新 AST 批次方案约 77 ms / 95 MB。新方案相对旧分割大幅改善，但仍比整包解码慢约 45%，尚未达到正常请求无显著退化的发布门槛。此为单线程本机微基准，不代表生产 P99。新增 escaped data key、畸形尾部 JSON、重复 data key 和 metadata 4 MiB 上限测试。V2 后续 schema 转换改为按行 AST 读取原 JSON spelling；另一个约 6.4 MiB 的合成扫描微基准由 gjson 全量扫描约 73 ms 降至 AST 约 4 ms，尚未完成整个 endpoint 的性能验收。完整 Proxy 包在本机仍因缺少 milvus_core / milvus-storage 无法编译。

新 renderer 曾试作递归编码到每个向量标量，虽然可检查更细粒度的取消，但 1024×128 响应从约 15 ms 增至约 94 ms；已撤销该粒度，改为“map 深入、数组按元素作为 Sonic 单元”。同一禁优化构建的旧整包与新 renderer 均约 15 ms，输出分配约 9.9 MB；只表示该合成形状无显著退化，不证明生产吞吐。嵌套 `data.ids[]` 的故障注入确认 timeout 后不会继续遍历所有元素；单个巨大 map/struct/custom Marshaler 仍可能跨越 deadline，容量拒绝发生在该单次编码之后。

4 MiB 草案单元进一步实测：内存中 4 MiB 的 `[]float64` 编成约 3.1 MiB JSON，单次 Sonic 编码约 43 ms、解码约 19 ms；同等长度字符串约 2.7 ms。若 deadline 落在一次被允许的向量编码刚开始之后，最多仍可能有该调用级别的超时 overshoot，不能说精确 5ms 截止。真实 Sonic 向量 + 第二单元的测试三轮通过：5ms deadline 后 renderer 等当前调用结束，但没有进入第二单元。仍需更多形状、race/生产构建测量和编码前容量限制，不能据此给出生产 P99 上界。

V1/V2 输入范围更新：V1 insert/upsert 的大 bulk 数组复用同一行分批 binder；V1 允许的单对象 `data` 格式保留，但仅在整个请求不超过 4 MiB 时走旧绑定回退。V1 其余 JSON 请求和 V2 非 bulk 请求在单次 Sonic 绑定前限制整个 body 至 4 MiB；这会拒绝过去可能被接受的大型非 bulk 请求，是待验收的兼容变化。V1 所有 downstream context 已改为继承 `c.Request.Context()`，否则 Gin 默认不把 HTTP deadline 透传给以 Gin context 为 parent 的 RPC。原始 V1 deadline 回归测试已加入常规测试，但本机 native 依赖不足，尚未执行；不能把源码修改当作全路径验证。

2026-10-09 容量选型测量（`requestbudget/json_unit_bench_test.go`）：M1 Pro、Go 1.26.6、Sonic 1.15.2、`-tags dynamic,test,sonic,bytedance_tango -gcflags='all=-N -l'`，每组重复三轮、100ms benchmark window。单个数值数组约 0.77 MiB JSON 时，编码约 10.5–10.6ms、解码约 4.7–4.8ms；约 6.2 MiB JSON 时，编码约 84–86ms、解码约 37ms，编码单次分配约 36.7 MiB。等大小纯字符串编码明显更快，不能代表向量/复杂对象。这只是本机禁优化构建的单调用测量，不是取消 overshoot 上界、生产 P99 或容量上限批准；必须补充不同形状、平台和实际请求路径的测量后再决定发布值。

2026-10-09 补充实测：[完整 body 解码基线](rest-timeout-decoding-baseline.md)。13,828,139 字节 JSON 在请求前已完整构造，以 Gin 缓存 body 路径调用真实 decoder；20ms 预算下，Sonic 三轮约 129–133ms 才返回，均晚于 context 取消和 HTTP 408，且完整解出所有 32,768 行。仅在取消后启动的 CPU profile 捕获 Sonic decoder 栈；标准库 control 也复现。已区分真实 decoding 与慢 socket read、人为阻塞 encoding。测试使用禁优化构建，不代表生产延迟；当前缺陷复现已补齐，但取消实现和完整 Milvus E2E 仍未完成。

同日另补[真实大 JSON 响应编码基线](rest-timeout-encoding-baseline.md)。20ms 预算下，原 `jsonRender` 编码 32,768×128 数值，三轮均在约 21ms 返回 HTTP 408，而真实 Sonic renderer 在约 336–350ms 才返回；取消后 CPU profile 包含 Sonic encoder 栈。此前人工阻塞 `MarshalJSON` 的证据现在有真实编码复现补强。返回 408 只表示客户端请求结束，不能说明服务端编码工作已退出。

### G4：迁移与发布

- 用户已批准 `overallTimeoutBudget=120s` 的新产品默认值；相对于旧 `requestTimeoutMs=30s` 默认，未指定客户端 header 的预算变长，但客户端不能再延长到 120s 以上。4 MiB JSON 单元上限是草案选择，不是已验收的线上默认；性能/兼容和完整入口覆盖通过前不得宣称实施完成。
- `requestTimeoutMs` 是默认预算，不是硬上限。迁移必须显式说明语义收紧。
- 旧 `readTimeout`/`writeTimeout` 的非零配置不能无损映射为一个整体预算。建议做启动前迁移校验并要求显式新配置；不能静默忽略，也不能保留两套长期执行逻辑。
- `idleTimeout` 可作为 `maxConnectionIdleInterval` 的弃用别名；新旧值冲突必须给出诊断。
- shared-port 上过去显式 read/write timeout 也影响 gRPC，新方案只限制 REST。这一范围变化需在迁移说明中指出。

## 6. 基线阶段的验证（2026-10-08）

运行：

```bash
cd pkg
env LOCAL_STORAGE_SIZE=1 go test -tags dynamic,test -gcflags='all=-N -l' -count=1 ./util/paramtable -run '^TestHTTPConfig'
```

结果：通过。第一次未指定 `LOCAL_STORAGE_SIZE` 时，现有初始化尝试创建 `/var/lib/milvus/data`，本机权限不足；仅对测试命令设置容量值后通过，未改配置文件。

运行：

```bash
# 从 Milvus 仓库根目录运行
go test -tags dynamic,test -gcflags='all=-N -l' -count=1 ./internal/distributed/proxy/httpserver -run 'TestTimeoutMiddlewarePassesDeadline|TestTimeoutMiddlewareRejectsInvalidRequestTimeout|TestTimeoutMiddlewareLateHandlerWritesUseCopiedContext'
```

结果：编译阻塞，pkg-config 缺少 `milvus_core`、`milvus-storage`。未执行到测试，不记为通过或功能失败。

没有执行真实网络故障注入、race 检测、性能测试或完整 E2E。本文未声称任何吞吐、RSS 或退出延迟收益已实测。

`audit_logging.sh HEAD HEAD` 与 `audit_merr.sh HEAD HEAD` 已运行：没有差异可审计，这是基线审查，不构成运行时验证。无可用变更范围，因此按路径/符号分类，没有把 HEAD 的无关最后一次提交当本次 diff。

Historical context: `query_github_knowledge.py --domain error-classification --limit 5` returned no matches; cache freshness UNKNOWN (no metadata.json), STALE/incomplete. 本轮不依赖历史缓存来证明机制。

## 7. Coverage ledger

- **Chains**：REST 接收、认证/准入、body decode、Proxy wrapper、任务等待与代表性 Search/Query deadline 生产、结果转换、响应写回。DDL/DML 共用入口已识别，但不更改其持久化语义。
- **Owners/producers**：`http_param.go` 配置；`listener_manager.go` 协议分流；`service.go` HTTP/gRPC 组装；`timeout_middleware.go` 客户端解析、context/timer、buffer 所有权。
- **Consumers actually read**：V1/V2 路由与 V2 wrapper；`utils.go` 转换/返回函数；`json_render.go`；`internal/json/sonic.go`；Proxy metadata context、TaskCondition、scheduler、Search/Query deadline 填充及 Query Execute；merr deadline/code 映射；access log 与 REST metrics 的上下文消费。
- **External implementing sources read**：Go 1.26.6 `net/http/server.go`、`responsecontroller.go`；Gin 1.11.0 binder/renderer/writer；x/net 0.58.0 HTTP/2 server、Framer、pipe、h2c；cmux 0.1.5 matcher/serve；Sonic 1.15.2 stream encoder。版本与 go.mod/工具链一致。
- **Failure windows walked**：协议识别前、header 未完成、Upgrade body、普通 body 停滞、业务完成后的 write、timeout 后转换、客户端大预算；未把 source trace 当成故障注入。
- **Tests examined**：V2 TestTimeout 与 middleware deadline/invalid/late-write/metadata/abort/access-log 用例；traceid timeout 用例定位；HTTP 参数 defaults/overrides；service h2c/TLS/port-share 用例。
- **Explicitly not audited end-to-end**：每个 Proxy downstream RPC/插件实现、QueryNode/segcore 任意工作强制退出、WAL/DDL 提交后的取消行为、生产网关配置与真实负载分布。它们不构成本轮已验证收益；涉及修改时必须补相应 subsystem 文档和源代码审查。
