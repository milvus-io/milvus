### timetick 相关改动

Datanode 发送 timetick msg 需要有 channel 的信息，DataCoord 根据 channel 来检查 segment 是否可以 seal 和 flush

### 服务发现

DataCoord 启动时检查是否有新的或重启过的 DataNode，如果有重启过的，重新注册 channel，并 seek 到上次记录的位置。

通过 watch 机制监听 DataNode 的状态，如果 DataNode 下线，其注册的 channel 重新分配到其他 node，并 seek 到上次的位置（重新分配不一定现在做）。

如果监听到 DataNode 重新上线，向其注册 channel，并 seek 到上次记录的位置。

如果监听到有新的 DataNode 注册，记录其状态，后续向其注册 channel 或进行 load balance（load balance 不一定现在做）。

DataNode 如果由于网络原因与 etcd 断开，应该重启服务发现，DataCoord 会去重新注册 channel，DataNode 不能重复监听相同 channel 。

### 需要记录的信息

1. cluster 信息（包括 Datanode 的节点及其上面的 channel）
2. segment 分配信息（最后一次分配的过期时间，segment 的上限等）
3. stats 和 segment flush channel 最后位置
4. DataNode 向 DataCoord 发送 channel 的最后位置

### 重启恢复流程

1. 内存状态恢复：
   1. 基本组件启动（meta， etcd 等）
   2. 恢复 segment allocator 和 cluster 状态
2. 与 DataNode 节点交互：

   1. 启动 stats/segment flush channel，并 seek 到上次记录的位置
   2. 启动 timetick channel

3. 与 Master 交互：

   1. 启动时不需要和 Master 交互，Collection 信息为空，需要用到时向 Master 请求并缓存
   2. 启动 new segment channel

4. 启动服务发现

目前 channel 注册是 segment allocation 驱动的，每次请求 segment 时，检查其 channel 有没有被注册，没有则将其注册到 DataNode，并保存到本地。这里有个问题，如果 channel 注册成功，DataCoord 挂掉了，那么在重启后，DataCoord 并不知道已经注册成功，来一个新的请求，还是会去注册，而且可能注册到不同的 DataNode 上面。类似 Transaction 的情况，需要有一套机制保证原子性在多节点写入，目前不太好解决。可以用以下步骤临时解决：

1. 在 etcd 上记录分配方法 entry，状态是未完成
2. 向 DataNode 注册
3. 在 etcd 上将 entry 标记为完成

重启时/分配时，如果发现有未完成的 entry，重新向 DataNode 注册，需要 DataNode 保证幂等性。

这样保证了一个 channel 只向一个 DataNode 注册，如果 DataNode 挂掉或者与 etcd 断开，如果需要重新分配到其他 DataNode，这些 entry 也跟着变。

DataCoord 模块中，有些策略是可能频繁改变的，比如 channel 对 DataNode 的分配策略，可以是随机/顺序/平均/根据 collection 分散等等策略，比如检测到 DataNode 创建和下线，可能会不处理/balance/将下线节点的 channel 转移到其他节点等。比如 segment allocation 可能会根据文件大小/条数等来确定是否关闭。实现应该把这些策略相关抽出来，方便以后修改。

### TODO:

1. segment allocation 信息持久化及恢复
2. cluster 信息（datanode 及 channel 信息）改动+持久化及恢复
3. channel 与 datanode 分配策略实现（可选 load balance）
4. stats/segment flush channel 记录位置及重启时 seek
5. 服务注册与发现，以及对应事件的处理逻辑
6. timetick channel 的改动以及 segment 关闭逻辑改动
7. datanode 上报的 binlog 信息持久化及恢复
