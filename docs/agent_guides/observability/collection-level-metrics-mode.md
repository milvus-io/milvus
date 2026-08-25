# Collection / VChannel 级 Prometheus 指标降基数模式

本文记录 `common.metrics.collectionLevelMode` 对 `milvus-io/milvus` 官方源码中全部
71 个 collection / VChannel 级 Prometheus 指标族的影响，并逐项对比修改前
（以及默认 `full` 模式）与 `aggregate` 模式的行为。清单基于 2026-08-25 的
`upstream/master`，基线提交为 `2d4343a988c5b3a66d03464801d83bcc04644ae7`。

本次按官方源码重新清点，而不是直接复用其他分支的清单：60 个指标族直接带
`collection_id` 或 `collection_name`，14 个指标族直接带 VChannel，二者重叠 3 个，
去重后为 71 个。官方源码额外包含带 `collection_name` 的
`milvus_proxy_req_count`，但不包含其他分支中的 QueryView 指标
`milvus_qv_view_state_max_age_seconds`。

## 配置和不变量

```yaml
common:
  metrics:
    collectionLevelMode: aggregate
```

| 值 | 行为 |
|---|---|
| `full`（默认） | 保留真实 `collection_id` / `collection_name` / VChannel；行为与修改前一致。 |
| `aggregate` | 不导出真实 collection 或 VChannel 标签值；保留标签键，将可安全合并的值统一写为 `all`；无法给出真实聚合语义的 Gauge 停止导出。 |

该配置是进程级、仅启动时生效的配置。所有 Milvus 进程必须使用相同值；混用
两种模式会使 Prometheus 中同时出现真实 collection 值和 `all`。非法值会在组件
启动时失败。

以下不变量在两种模式下都成立：

- 指标名和标签键不变，因此不会产生 Prometheus label schema 冲突。
- 处理 `collection_id`、`collection_name`，以及经 writer 逐项确认的 VChannel
  `channel_name` 和 C++ cache `shard`。不会根据标签名盲目处理所有
  `channel_name`；PChannel 指标保持原行为。
- `aggregate` 模式下，collection / VChannel 级删除操作不会删除多个来源共享的
  `all` 时序；`Reset` 以及只按 node 等非受控标签执行的清理仍然有效。
- Counter 的增量会求和；Histogram 的观测值进入同一组 bucket，因此 `_count`、
  `_sum` 和各 bucket 都是合并后的结果。
- Gauge 不依赖“最后一次 `Set` 恰好代表所有 collection”这种不稳定语义。只有能
  预聚合或使用 `Add/Sub` 的 GaugeVec 才继续导出。

71 个指标族在 `aggregate` 模式下分为：40 个 Counter/Histogram 自动合并、
10 个 Gauge 显式求和、21 个 Gauge 停止导出。

## Counter 和 Histogram：40 个自动合并

表中的“剩余维度”仍是正常标签；受控的 collection / VChannel 标签也仍然存在，
但值固定为 `all`。

### Proxy（18）

| 指标 | 类型 | 修改前 / `full` | `aggregate` |
|---|---|---|---|
| `milvus_proxy_received_nq` | Counter | 每个 collection 独立累计 NQ。 | 按 `node_id`, `query_type`, `db_name` 合并累计。 |
| `milvus_proxy_search_vectors_count` | Counter | 每个 collection 独立累计搜索向量数。 | 按 `node_id`, `db_name` 合并累计。 |
| `milvus_proxy_insert_vectors_count` | Counter | 每个 collection 独立累计插入向量数。 | 按 `node_id`, `db_name` 合并累计。 |
| `milvus_proxy_upsert_vectors_count` | Counter | 每个 collection 独立累计 upsert 向量数。 | 按 `node_id`, `db_name` 合并累计。 |
| `milvus_proxy_delete_vectors_count` | Counter | 每个 collection 独立累计删除向量数。 | 按 `node_id`, `db_name` 合并累计。 |
| `milvus_proxy_sq_latency` | Histogram | 每个 collection 独立记录 search/query 延迟分布。 | 按 `node_id`, `query_type`, `db_name` 合并观测值。 |
| `milvus_proxy_collection_sq_latency` | Histogram | 每个 collection 独立记录延迟；该指标已废弃。 | 按 `node_id`, `query_type`, `db_name` 合并观测值。 |
| `milvus_proxy_mutation_latency` | Histogram | 每个 collection 独立记录 mutation 延迟分布。 | 按 `node_id`, `msg_type`, `db_name` 合并观测值。 |
| `milvus_proxy_collection_mutation_latency` | Histogram | 每个 collection 独立记录延迟；该指标已废弃。 | 按 `node_id`, `msg_type`, `db_name` 合并观测值。 |
| `milvus_proxy_req_count` | Counter | 按 operation、状态、原因、DB 和 collection 独立累计请求。 | `collection_name="all"`，按 `node_id`, operation、状态、原因和 `db_name` 合并累计。 |
| `milvus_proxy_receive_bytes_count` | Counter | 每个 collection 独立累计接收字节数。 | 按 `node_id`, `msg_type`, `db_name` 合并累计。 |
| `milvus_proxy_retry_search_cnt` | Counter | 每个 collection 独立累计 retry search。 | 按 `node_id`, `query_type`, `db_name` 合并累计。 |
| `milvus_proxy_retry_search_result_insufficient_cnt` | Counter | 每个 collection 独立累计结果不足的 retry。 | 按 `node_id`, `query_type`, `db_name` 合并累计。 |
| `milvus_proxy_recall_search_cnt` | Counter | 每个 collection 独立累计 recall search。 | 按 `node_id`, `query_type`, `db_name` 合并累计。 |
| `milvus_proxy_search_sparse_num_non_zeros` | Histogram | 每个 collection 独立记录稀疏向量 non-zero 分布。 | 按 `node_id`, `query_type`, `field_id` 合并观测值。 |
| `milvus_proxy_function_udf_call_latency` | Histogram | 每个 collection 独立记录 UDF 延迟。 | 按 `node_id`, `function_type_name`, `function_provider`, `function_name` 合并观测值。 |
| `milvus_proxy_scanned_remote_mb` | Counter | 每个 collection 独立累计远端扫描量。 | 按 `node_id`, `msg_type`, `db_name` 合并累计。 |
| `milvus_proxy_scanned_total_mb` | Counter | 每个 collection 独立累计总扫描量。 | 按 `node_id`, `msg_type`, `db_name` 合并累计。 |

### DataCoord（3）

| 指标 | 类型 | 修改前 / `full` | `aggregate` |
|---|---|---|---|
| `milvus_datacoord_store_level0_segment_size` | Histogram | 每个 collection 独立记录 L0 segment size 分布。 | 所有 collection 的观测值合并到 `collection_id="all"`。 |
| `milvus_datacoord_bulk_insert_vectors_count` | Counter | 每个 collection 独立累计 bulk insert 向量数。 | 按 `db_name` 合并累计。 |
| `milvus_datacoord_compaction_latency` | Histogram | 按真实 VChannel、vector field、compaction type 和 stage 记录 compaction 延迟。 | `channel_name="all"`，按 vector field、compaction type 和 stage 合并观测值。 |

### DataNode（4）

| 指标 | 类型 | 修改前 / `full` | `aggregate` |
|---|---|---|---|
| `milvus_datanode_write_data_count` | Counter | 每个 collection 独立累计写入量。 | 按 `node_id`, `data_source`, `data_type` 合并累计。 |
| `milvus_datanode_consume_msg_count` | Counter | 每个 collection 独立累计消费消息数。 | 按 `node_id`, `msg_type` 合并累计。 |
| `milvus_datanode_compaction_delete_count` | Counter | 每个 collection 独立累计 compaction delete。 | 所有 collection 合并到 `collection_id="all"`。 |
| `milvus_datanode_compaction_missing_delete_count` | Counter | 每个 collection 独立累计 missing delete。 | 所有 collection 合并到 `collection_id="all"`。 |

### QueryCoord（1）

| 指标 | 类型 | 修改前 / `full` | `aggregate` |
|---|---|---|---|
| `milvus_querycoord_task_latency` | Histogram | 按 collection 和 VChannel 记录 task latency。 | `collection_id="all"`, `channel_name="all"`，仅按 `task_type` 合并观测值。 |

### QueryNode（14）

| 指标 | 类型 | 修改前 / `full` | `aggregate` |
|---|---|---|---|
| `milvus_querynode_consume_msg_count` | Counter | 每个 collection 独立累计消费消息数。 | 按 `node_id`, `msg_type` 合并累计。 |
| `milvus_querynode_skipped_insert_field_count` | Counter | 每个 collection 独立累计跳过的 insert field。 | 按 `node_id` 合并累计。 |
| `milvus_querynode_sq_req_count` | Counter | 每个 collection 独立累计 search/query 请求。 | 按 `node_id`, `query_type`, `status`, `scope` 合并累计。 |
| `milvus_querynode_search_fts_num_tokens` | Histogram | 每个 collection 独立记录 FTS token 数分布。 | 按 `node_id`, `field_id` 合并观测值。 |
| `milvus_querynode_search_hit_segment_num` | Histogram | 每个 collection 独立记录命中 segment 数。 | 按 `node_id`, `query_type` 合并观测值。 |
| `milvus_querynode_segment_filter_hit_segment_num` | Histogram | 每个 collection 独立记录 filter hit segment 数。 | 按 `node_id`, `query_type` 合并观测值。 |
| `milvus_querynode_segment_filter_skipped_segment_num` | Histogram | 每个 collection 独立记录 filter skipped segment 数。 | 按 `node_id`, `query_type` 合并观测值。 |
| `milvus_querynode_segment_filter_total_segment_num` | Histogram | 每个 collection 独立记录 filter total segment 数。 | 按 `node_id`, `query_type` 合并观测值。 |
| `milvus_querynode_segment_prune_latency` | Histogram | 每个 collection 独立记录 prune latency。 | 按 `node_id`, `segment_prune_label` 合并观测值。 |
| `milvus_querynode_partial_result_count` | Counter | 每个 collection 独立累计 partial result。 | 按 `node_id`, `query_type` 合并累计。 |
| `milvus_querynode_two_stage_search_stage1_latency` | Histogram | 每个 collection 独立记录 stage 1 latency。 | 按 `node_id` 合并观测值。 |
| `milvus_querynode_two_stage_search_stage2_latency` | Histogram | 每个 collection 独立记录 stage 2 latency。 | 按 `node_id` 合并观测值。 |
| `milvus_querynode_two_stage_search_fallback_total` | Counter | 每个 collection 独立累计 fallback。 | 按 `node_id`, `reason` 合并累计。 |
| `milvus_querynode_global_refine_total` | Counter | 每个 collection 独立累计 global refine。 | 按 `node_id` 合并累计。 |

## Gauge：10 个显式求和

这些 Gauge 的 writer 已改成先按剩余维度求和再 `Set`，或原本就通过
`Add/Sub` 维护可加值。每次快照型采集前会清空对应 component/node 的旧样本，
避免 DB、状态或 collection 消失后留下旧时序。

| 指标 | 修改前 / `full` | `aggregate` |
|---|---|---|
| `milvus_rootcoord_entity_num` | 按 `db_name`, `collection_name`, `status` 设置 collection 实体数。 | `collection_name="all"`，按 `db_name`, `status` 求和。`loaded` 仍沿用原 writer 的 replica 累加语义，因此本模式不修正已有的 replica 重复计数。 |
| `milvus_rootcoord_indexed_entity_num` | 按 DB、collection、index 设置已索引实体数。 | `collection_name="all"`，按 `db_name`, `index_name`, `is_vector_index` 求和。 |
| `milvus_datacoord_l0_delete_entries_num` | 按 DB、collection 设置 L0 delete entry 数。 | `collection_id="all"`，按 `db_name` 求和。 |
| `milvus_datacoord_stored_rows_num` | 按 DB、collection ID/name、segment state 设置行数。 | 两个 collection 标签均为 `all`，按 `db_name`, `segment_state` 求和。 |
| `milvus_datacoord_stored_binlog_size` | 按 DB、collection、segment state 设置 binlog 大小。 | `collection_id="all"`，按 `db_name`, `segment_state` 求和。 |
| `milvus_datacoord_segment_binlog_file_count` | 按 collection 设置 binlog 文件数。 | 所有 collection 求和到 `collection_id="all"`；即使当前为 0，也会导出值为 0 的聚合时序。 |
| `milvus_datanode_fg_buffer_size` | 按 node、collection 通过 `Add/Sub` 维护 flowgraph buffer 大小。 | `collection_id="all"`，按 `node_id` 求和，继续使用 `Add/Sub`。 |
| `milvus_querynode_entity_num` | 按 DB、collection ID/name、node、segment state 设置实体数。 | 两个 collection 标签均为 `all`，按 `db_name`, `node_id`, `segment_state` 求和。 |
| `milvus_querynode_entity_size` | 按 node、collection、segment state 设置实体内存。 | `collection_id="all"`，按 `node_id`, `segment_state` 求和。 |
| `internal_cache_shard_disk_usage_bytes` | C++ caching layer 按 `data_type`, `shard` 导出磁盘占用，其中 `shard` 是 insert VChannel。 | `CRegistry` 在解析 C++ Prometheus 输出后将 `shard="all"`，并按 `data_type` 对各 VChannel 字节数求和；C++ 内部逐 shard attribution 不变。 |

## Gauge：21 个在 `aggregate` 下停止导出

这些指标由多个 collection / VChannel 分别 `Set`。直接把受控标签改为 `all` 会使
最后一次写入覆盖其他来源；求和对 ratio、lag、checkpoint 等指标也没有稳定含义。
因此 `aggregate` 下写入变为 no-op，并且不会创建 Prometheus 时序。

| 指标 | 修改前 / `full` | `aggregate` 及原因 |
|---|---|---|
| `milvus_proxy_limiter_rate` | 按 `node_id`, `collection_id`, `msg_type` 设置 limiter rate；实际 label 还混用 root/database/collection/partition source ID。 | 不导出；直接合并会产生不确定的最后写入值，且该标签本身并非纯 collection 语义。 |
| `milvus_rootcoord_rate_limit_ratio` | 每个 collection 设置限流比例。 | 不导出；比例不能求和，最后写入也不能代表整体。 |
| `milvus_datacoord_stored_index_files_size` | 每个 collection 设置 active index 文件总大小。 | 不导出；当前 writer 是 collection 局部 `Set`，尚未维护进程级总量。 |
| `milvus_datacoord_index_task_count` | 按 collection 和 task status 设置 task 数；指标已废弃。 | 不导出；避免碰撞，替代指标 `milvus_datacoord_task_count` 不含 collection。 |
| `milvus_datacoord_snapshot_active_pins` | 按 collection 和 snapshot name 设置 active pin 数。 | 不导出；snapshot name 只在 collection 内有意义，跨 collection 合并会碰撞。 |
| `milvus_datacoord_channel_checkpoint_unix_seconds` | 按 node、VChannel 设置 checkpoint 的 Unix 时间。 | 不导出；多个 checkpoint 不能求和，最后写入也不能代表整体。若需要最慢进度，应另建语义明确的 min/max lag 指标。 |
| `milvus_datanode_consume_tt_lag_ms` | 按 node、message type、collection 设置 time-tick lag。 | 不导出；lag 需要明确定义 max/quantile，不能求和或使用最后写入。 |
| `milvus_datanode_growing_source_sync_failure_count` | 按 node、collection、VChannel 设置连续失败数。 | 不导出；它是 collection/channel 局部 `Set`，当前没有安全的跨 collection 聚合和清理生命周期。 |
| `milvus_datanode_msg_dispatcher_tt_lag_ms` | 按 node、VChannel 设置 dispatcher time-tick lag。 | 不导出；VChannel lag 不能求和，最后写入不代表整体。 |
| `milvus_querycoord_current_target_checkpoint_unix_seconds` | 按 node、VChannel 设置 current target checkpoint 时间。 | 不导出；checkpoint 需要定义 min/max 聚合，不能使用最后写入。 |
| `milvus_querycoord_current_target_all_replicas_checkpoint_unix_seconds` | 按 node、VChannel 设置所有 replica ready 时的 checkpoint 时间。 | 不导出；原因同 current target checkpoint。 |
| `milvus_querynode_consume_tt_lag_ms` | 按 node、message type、collection 设置 time-tick lag。 | 不导出；原因同 DataNode lag。 |
| `milvus_querynode_segment_num` | 按 node、collection、segment state/level 设置 segment 数。 | 不导出；现有 writer 混用 `Inc/Dec` 和 collection 局部 `Set`，直接合并不可靠。 |
| `milvus_querynode_growing_source_retained_bytes` | 按 node、VChannel 设置 release handoff 保留字节数。 | 不导出；当前 writer 是 VChannel 局部 `Set/Delete`，没有维护跨 VChannel 的增量生命周期。 |
| `milvus_querynode_growing_source_retained_segments` | 按 node、VChannel 设置 release handoff 保留 segment 数。 | 不导出；原因同 retained bytes。 |
| `milvus_querynode_segment_prune_ratio` | 每个 collection/prune type 设置 prune ratio。 | 不导出；ratio 需要权重才能合并。 |
| `milvus_querynode_segment_prune_bias` | 每个 collection/prune type 设置 workload bias。 | 不导出；bias 不能求和，最后写入没有整体含义。 |
| `milvus_querynode_level_zero_size` | 按 node、collection、VChannel 设置 L0 delete buffer 大小。 | 不导出；当前没有跨 collection 的快照聚合，折叠后也无法按 collection 安全清理 channel 时序。 |
| `milvus_querynode_msg_dispatcher_tt_lag_ms` | 按 node、VChannel 设置 dispatcher time-tick lag。 | 不导出；VChannel lag 不能求和，最后写入不代表整体。 |
| `milvus_querynode_delete_buffer_size` | 按 node、VChannel 设置 delegator delete buffer 字节数。 | 不导出；当前 writer 是 VChannel 局部 `Set/Delete`，直接折叠会互相覆盖或误删。 |
| `milvus_querynode_delete_buffer_row_num` | 按 node、VChannel 设置 delegator delete buffer 行数。 | 不导出；原因同 delete buffer size。 |

注册行为保持不变：这些 collector 仍注册到 registry；`aggregate` 模式没有写入时，
Prometheus scrape 中不会出现对应 metric family 的样本。

## 明确不受影响的边界

- `milvus_proxy_report_value`（`ProxyReportValue`）没有 `collection_id` 或
  `collection_name`，其标签是 `node_id`, `msg_type`, `db_name`, `username`，因此
  两种模式行为完全相同。这也是它没有被列入上述 71 个受控指标的原因。
- 已确认是 PChannel 的 `channel_name` 不处理，包括 RootCoord produce time-tick lag、
  Proxy msgstream/tt lag、DataCoord consume DataNode time-tick lag，以及 Streaming
  Service / WAL / CDC 指标。`StreamingCoordVChannelTotal` 虽然统计 VChannel 数量，
  其 `channel_name` 标签仍是 PChannel，也保持不变。
- C++ 原生链只有 `internal_cache_shard_disk_usage_bytes` 的 `shard` 被确认是 insert
  VChannel 并在 `/metrics` 投影边界聚合。其他 Milvus core、Knowhere、
  milvus-storage FFI 和 jemalloc 指标没有直接 collection / VChannel 标签，行为不变。
- trace 的 `message.vchannel`、日志字段、JSON metrics/control-plane response 不属于
  Prometheus label，不受这个开关影响。

## 生命周期和查询影响

`full` 模式下，原有 collection drop/unload 和 VChannel 清理行为不变。
`aggregate` 模式下，Counter、Histogram 和可保留 Gauge 的 `all` 时序由多个
collection / VChannel 共享，所以：

- `DeleteLabelValues` 或带 collection / VChannel 标签的 `Delete` /
  `DeletePartialMatch` 返回未删除，防止清理一个来源时误删其他来源的数据。
- DataNode input node 对同一组 labels 的 metrics handle 做引用计数；聚合模式下的
  collection 清理又是 no-op，因此释放一个 flowgraph 不会误删其他 collection 的
  共享时序。
- 快照式聚合 Gauge 在下一轮采集时清空自身 scope 并重建，保证 drop 后数值收敛，
  且不会删除同进程内其他 node 的样本。
- Dashboard 若过滤具体 collection，将在 `aggregate` 模式得到空结果；应改为匹配
  `collection_id="all"` / `collection_name="all"`。过滤 VChannel 的查询同样应匹配
  `channel_name="all"` / `shard="all"`，或移除对应过滤条件。

该模式降低的是 Prometheus 直接 collection / VChannel 标签的基数，不提供租户级
用量归因；需要 collection 或 VChannel 诊断时应使用 `full` 模式或专门的有界诊断接口。
