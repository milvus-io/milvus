### timetick 相关改动

Datanode发送timetick msg需要有channel的信息，DataCoord根据channel来检查segment是否可以seal和flush



### 服务发现

DataCoord启动时检查是否有新的或重启过的DataNode，如果有重启过的，重新注册channel，并seek到上次记录的位置

通过watch机制监听DataNode的状态，如果DataNode下线，其注册的channel重新分配到其他node，并seek到上次的位置（重新分配不一定现在做）

如果监听到DataNode重新上线，向其注册channel，并seek到上次记录的位置

如果监听到有新的DataNode注册，记录其状态，后续向其注册channel或进行load balance（load balance不一定现在做）

DataNode如果由于网络原因与etcd断开，应该重启服务发现，DataCoord会去重新注册channel，DataNode不能重复监听相同channel

### 需要记录的信息

1. cluster信息（包括Datanode的节点及其上面的channel）
2. segment 分配信息（最后一次分配的过期时间，segment的上限等）
3. stats 和 segment flush channel最后位置
4. DataNode向DataCoord发送channel的最后位置

### 重启恢复流程

1. 内存状态恢复：
   1. 基本组件启动（meta， etcd等）
   2. 恢复segment allocator和cluster状态
   3. 
2. 与DataNode节点交互：
   1. 启动stats/segment flush channel，并seek到上次记录的位置
   2. 启动timetick channel

3. 与Master交互：
   1. 启动时不需要和Master交互，Collection信息为空，需要用到时向Master请求并缓存
   2. 启动new segment channel

4. 启动服务发现

目前channel注册是segment allocation驱动的，每次请求segment时，检查其channel有没有被注册，没有则将其注册到DataNode，并保存到本地。这里有个问题，如果channel注册成功，DataCoord挂掉了，那么在重启后，DataCoord并不知道已经注册成功，来一个新的请求，还是会去注册，而且可能注册到不同的DataNode上面。类似Transaction的情况，需要有一套机制保证原子性在多节点写入，目前不太好解决。可以用以下步骤临时解决：

1. 在etcd上记录分配方法entry，状态是未完成
2. 向DataNode注册
3. 在etcd上将entry标记为完成

重启时/分配时，如果发现有未完成的entry，重新向DataNode注册，需要DataNode保证幂等性。

这样保证了一个channel只向一个DataNode注册，如果DataNode挂掉或者与etcd断开，如果需要重新分配到其他DataNode，这些entry也跟着变。



DataCoord模块中，有些策略是可能频繁改变的，比如channel对DataNode的分配策略，可以是随机/顺序/平均/根据collection分散等等策略，比如检测到DataNode创建和下线，可能会不处理/balance/将下线节点的channel转移到其他节点等。比如segment allocation可能会根据文件大小/条数等来确定是否关闭。实现应该把这些策略相关抽出来，方便以后修改。



### TODO:

1. segment allocation信息持久化及恢复
2. cluster信息（datanode及channel信息）改动+持久化及恢复
3. channel与datanode分配策略实现（可选load balance）
4. stats/segment flush channel记录位置及重启时seek 
5. 服务注册与发现，以及对应事件的处理逻辑
6. timetick channel的改动以及segment关闭逻辑改动
7. datanode上报的binlog信息持久化及恢复 

