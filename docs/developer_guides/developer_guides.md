

# Milvus Developer Guides

​                                                                                                                                   by Rentong Guo, Sep 15, 2020



## 1. System Overview

In this section, we sketch the system design of Milvus , including data model, data organization, architecture, and state synchronization. 



#### 1.1 Data Model

Milvus exposes the following set of data features to applications:

* a data model based on schematized relational tables, in that rows must have primary-keys,

* a query language specifies data definition, data manipulation, and data query, where data definition includes create, drop, and data manipulation includes insert, upsert, delete, and data query falls into three types, primary key search, approximate nearest neighbor search (ANNS), ANNS with predicates.

The requests' execution order is strictly in accordance with their issue-time order. We take proxy's issue time as a requst's issue time. For a batch request, all its sub-requests share a same issue time. In cases there are multiple proxies, issue time from different proxies are regarded as coming from a central clock.

Transaction is currently not supported by Milvus. Only batch requests such as batch insert/delete/query are supported. A batch insert/delete is guaranteed to become visible atomically.



#### 1.2 Data Organization



<img src="./figs/data_organization.pdf" width=550>

In Milvus, 'collection' refers to the concept of table. A collection can be optionally divided into several 'partitions'. Both collection and partition are the basic execution scopes of queries. When use parition, users should clearly know how a collection should be partitioned. In most cases, parition leads to more flexible data management and more efficient quering. For a partitioned collection, queries can be executed both on the collection or a set of specified partitions.

Each collection or parition contains a set of 'segment groups'. Segment group is the basic unit of data-to-node mapping. It's also the basic unit of replica. For instance, if a query node failed, its segment groups will be redistributed accross other nodes. If a query node is overloaded, part of its  segment groups will be migrated to underloaded ones. If a hot collection/partition is detected, its segment groups will be replicated to smooth the system load skewness.

'Segment' is the finest unit of data organization. It is where the data and indexes are actually kept. Each segment contains a set of rows. In order to reduce the memory footprint during a query execution and to fully utilize SIMD, the physical data layout within segments is organized in a column-based manner. 



#### 1.3 Architecture Overview



<img src="./figs/system_framework.pdf" width=800>

The main components, proxy, WAL, query node and write node can scale to multiple instances. These components scale seperately for better tradeoff between availability and cost.

The WAL forms a hash ring. Requests (i.e. inserts and deletes) from clients will be repacked by proxy. Operations shared identical hash value (the hash value of primary key) will be routed to the same hash bucket. In addtion, some preprocessing work will be done by proxy, such as static validity checking, primary key assignment (if not given by user), timestamp assignment.

The query/write nodes are linked to the hash ring, with each node covers some portion of the buckets. Once the hash function and bucket coverage are settled, the chain 'proxy -> WAL -> query/write node' will act as a producer-consumer pipeline. Logs in each bucket is a determined operation stream. Via performing the operation stream in order, the query nodes keep themselves up to date.

The query nodes hold all the indexes in memory. Since building index is time-consuming, the query nodes will dump their index to disk (store engine) for fast failure recovery and cross node index copy.

The write nodes are stateless. They simply transforms the newly arrived WALs to binlog format, then append the binlog to store enginey. 

Note that not all the components are necessarily replicated. The system provides failure tolerance by maintaining multiple copies of WAL and binlog. When there is no in-memory index replica and there occurs a query node failure, other query nodes will take over its indexes by loading the dumped index files, or rebuilding them from binlog and WALs. The links from query nodes to the hash ring will also be adjusted such that the failure node's input WAL stream can be properly handled by its neighbors.



#### 1.4 State Synchronization

<img src="./figs/state_sync.pdf" width=800>

Data in Milvus have three different forms, namely WAL, binlog, and index. As mentioned in the previous section, WAL can be viewed as a determined operation stream. Other two data forms keep themselves up to date by performing the operation stream in time order.

Each of the WAL is attached with a timestamp, which is the time when the log is sent to the hash bucket. Binlog records, table rows, index cells will also keep that timestamp. In this way, different data forms can offer consistent snapshot for a given time T. For example, requests such as "fetch binlogs before T for point-in-time recovery", "get the row with primary key K at time T", "launch a similarity search at time T for vector V" perform on binlog, index respectively. Though different data forms these three requests are performed, they observe identical snapshot, namely all the state changes before T.

For better throughput, Milvus allows asynchronous state synchronization between WAL and index/binlog/table. Whenever the data is not fresh enough to satisfiy a query, the query will be suspended until the data is up-to-date, or timeout will be returned.



## 2. Schema

#### 2.1 Collection Schema

``` go
type CollectionSchema struct {
  Name string
  Description string
  AutoId bool
  Fields []FieldSchema
}
```

#### 2.2 Field Schema

``` go
type FieldSchema struct {
  Name string
  Description string
  DataType DataType 
  TypeParams map[string]string
  IndexParams map[string]string
}
```

###### 2.2.1 Data Types

###### 2.2.2 Type Params

###### 2.2.3 Index Params



## 3. Request

In this section, we introduce the RPCs of milvus service. A brief description of the RPCs is listed as follows.

| RPC                | description                                                  |
| :----------------- | ------------------------------------------------------------ |
| CreateCollection   | create a collection base on schema statement                 |
| DropCollection     | drop a collection                                            |
| HasCollection      | whether or not a collection exists                           |
| DescribeCollection | show a collection's schema and its descriptive statistics    |
| ShowCollections    | list all collections                                         |
| CreatePartition    | create a partition                                           |
| DropPartition      | drop a partition                                             |
| HasPartition       | whether or not a partition exists                            |
| DescribePartition  | show a partition's name and its descriptive statistics       |
| ShowPartitions     | list a collection's all partitions                           |
| Insert             | insert a batch of rows into a collection or a partition      |
| Search             | query the columns of a collection or a partition with ANNS statements and boolean expressions |



#### 3.1 Definition Requests

###### 3.2.1 Collection

* CreateCollection
* DropCollection
* HasCollection
* DescribeCollection
* ShowCollections

###### 3.2.2 Partition

* CreatePartition
* DropPartition
* HasPartition
* DescribePartition
* ShowPartitions



#### 3.2 Manipulation Requsts

###### 3.2.1 Insert

* Insert

###### 3.2.2 Delete

* DeleteByID



#### 3.3 Query



##  4. Time



#### 4.1 Timestamp

Before we discuss timestamp, let's take a brief review of Hybrid Logical Clock (HLC). HLC uses 64bits timestamps which are composed of a 46-bits physical component (thought of as and always close to local wall time) and a 18-bits logical component (used to distinguish between events with the same physical component).

<img src="./figs/hlc.pdf" width=450>

HLC's logical part is advanced on each request. The phsical part can be increased in two cases: 

A. when the local wall time is greater than HLC's physical part,

B. or the logical part overflows.

In either cases, the physical part will be updated, and the logical part will be set to 0.

Keep the physical part close to local wall time may face non-monotonic problems such as updates to POSIX time that could turn time backward. HLC avoids such problems, since if 'local wall time < HLC's physical part' holds, only case B is satisfied, thus montonicity is guaranteed.

Milvus does not support transaction, but it should gurantee the deterministic execution of the multi-way WAL. The timestamp attached to each request should

- have its physical part close to wall time (has an acceptable bounded error, a.k.a. uncertainty interval in transaction senarios),
- and be globally unique.

HLC leverages on physical clocks at nodes that are synchronized using the NTP. NTP usually maintain time to within tens of milliseconds over local networks in datacenter. Asymmetric routes and network congestion occasionally cause errors of hundreds of milliseconds. Both the normal time error and the spike are acceptable for Milvus use cases. 

The interface of Timestamp is as follows.

```
type timestamp struct {
  physical uint64 // 18-63 bits
  logical uint64  // 0-17 bits
}

type Timestamp uint64
```



#### 4.2 Timestamp Oracle

```
type timestampOracle struct {
  client *etcd.Client // client of a reliable meta service, i.e. etcd client
  rootPath string // this timestampOracle's working root path on the reliable kv service
  saveInterval uint64
  lastSavedTime uint64
  tso Timestamp // monotonically increasing timestamp
}

func (tso *timestampOracle) GetTimestamp(count uint32) ([]Timestamp, Status)

func (tso *timestampOracle) saveTimestamp() Status
func (tso *timestampOracle) loadTimestamp() Status
```



#### 4.2 Timestamp Allocator

###### 4.2.1 Batch Allocation of Timestamps

###### 4.2.2 Expiration of Timestamps



#### 4.5 T_safe





## 5. Basic Components

#### 5.1 Watchdog

``` go
type ActiveComponent interface {
  Id() string
  Status() Status
  Clean() Status
  Restart() Status
}

type ComponentHeartbeat interface {
  Id() string
  Status() Status
  Serialize() string
}

type Watchdog struct {
  targets [] *ActiveComponent
  heartbeats ComponentHeartbeat chan
}

// register ActiveComponent
func (dog *Watchdog) Register(target *ActiveComponent)

// called by ActiveComponents
func (dog *Watchdog) PutHeartbeat(heartbeat *ComponentHeartbeat)

// dump heatbeats as log stream
func (dog *Watchdog) dumpHeartbeat(heartbeat *ComponentHeartbeat)
```



#### 5.2 Global Parameter Table

``` go
type GlobalParamsTable struct {
  params memoryKV
}

func (gparams *GlobalParamsTable) Save(key, value string) Status
func (gparams *GlobalParamsTable) Load(key string) (string, Status)
func (gparams *GlobalParamsTable) LoadRange(key, endKey string, limit int) ([]string, []string, Status)
func (gparams *GlobalParamsTable) Remove(key string) Status
```



#### 5.3 Message Stream

``` go
type MsgType uint32
const {
  USER_REQUEST MsgType = 1
  TIME_TICK = 2
}

type TsMsg interface {
  SetTs(ts Timestamp)
  Ts() Timestamp
  Type() MsgType
}

type TsMsgMarshaler interface {
  Marshal(input *TsMsg) ([]byte, Status)
  Unmarshal(input []byte) (*TsMsg, Status)
}

type MsgPack struct {
  BeginTs Timestamp
  EndTs Timestamp
  Msgs []*TsMsg
}

type MsgStream interface {
  SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
  Produce(*MsgPack) Status
  Consume() *MsgPack // message can be consumed exactly once
}

type PulsarMsgStream struct {
  client *pulsar.Client
  produceChannels []string
  consumeChannels []string
  
  msgMarshaler *TsMsgMarshaler
  msgUnmarshaler *TsMsgMarshaler
}

func (ms *PulsarMsgStream) SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
func (ms *PulsarMsgStream) Produce(*MsgPack) Status
func (ms *PulsarMsgStream) Consume() *MsgPack //return messages in one time tick

type PulsarTtMsgStream struct {
  client *pulsar.Client
  produceChannels []string
  consumeChannels []string
  
  msgMarshaler *TsMsgMarshaler
  msgUnmarshaler *TsMsgMarshaler
  inputBuf []*TsMsg
  unsolvedBuf []*TsMsg
  msgPacks []*MsgPack
}

func (ms *PulsarTtMsgStream) SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
func (ms *PulsarTtMsgStream) Produce(*MsgPack) Status
func (ms *PulsarTtMsgStream) Consume() *MsgPack //return messages in one time tick
```





## 6. Proxy

#### 6.1 Overview

#### 3.1 Task

``` go
type task interface {
  PreExecute() Status
  Execute() Status
  PostExecute() Status
  WaitToFinish() Status
  Notify() Status
}
```

* Base Task 

```go
type baseTask struct {
  Type ReqType
  ReqId int64
  Ts Timestamp
  ProxyId int64
}

func (task *baseTask) PreExecute() Status
func (task *baseTask) Execute() Status
func (task *baseTask) PostExecute() Status
func (task *baseTask) WaitToFinish() Status
func (task *baseTask) Notify() Status
```

* Insert Task

  Take insertTask as an example:

```go
type insertTask struct {
  baseTask
  SegIdAssigner *segIdAssigner
  RowIdAllocator *IdAllocator
  rowBatch *RowBatch
}

func (task *InsertTask) Execute() Status
func (task *InsertTask) WaitToFinish() Status
func (task *InsertTask) Notify() Status
```



#### 6.2 Task Scheduler

``` go
type taskScheduler struct {
  // definition tasks
  ddTasks *task chan
  // manipulation tasks
  dmTasks *task chan
  // query tasks
  dqTasks *task chan
  
  tsAllocator *TimestampAllocator
  ReqIdAllocator *IdAllocator
}

func (sched *taskScheduler) EnqueueDDTask(task *task) Status
func (sched *taskScheduler) EnqueueDMTask(task *task) Status
func (sched *taskScheduler) EnqueueDQTask(task *task) Status

func (sched *taskScheduler) TaskDoneTest(ts Timestamp) bool

// ActiveComponent interfaces
func (sched *taskScheduler) Id() String
func (sched *taskScheduler) Status() Status
func (sched *taskScheduler) Clean() Status
func (sched *taskScheduler) Restart() Status
func (sched *taskScheduler) heartbeat()

// protobuf
message taskSchedulerHeartbeat {
  string id
  uint64 dd_queue_length
  uint64 dm_queue_length
  uint64 dq_queue_length
  uint64 num_dd_done
  uint64 num_dm_done
  uint64 num_dq_done
}
```

* EnqueueDMTask

  If a insertTask is enqueued, *EnqueueDDTask(task \*task)* will set *Ts*, *ReqId*, *ProxyId*, *SegIdAssigner*, *RowIdAllocator*, then push it into queue *dmTasks*. The *SegIdAssigner* and *RowIdAllocator* will later be used in the task's execution phase.

#### 6.3 Time Tick

``` go
type timeTick struct {
  lastTick Timestamp
  currentTick Timestamp
}

func (tt *timeTick) tick() Status

// ActiveComponent interfaces
func (tt *timeTick) ID() String
func (tt *timeTick) Status() Status
func (tt *timeTick) Clean() Status
func (tt *timeTick) Restart() Status
func (tt *timeTick) heartbeat()

// protobuf
message TimeTickHeartbeat {
  string id
  uint64 last_tick
}
```



## 7. Message Stream

#### 7.1 Overview



#### 7.2 Message Stream

``` go
type TsMsg interface {
  SetTs(ts Timestamp)
  Ts() Timestamp
}

type MsgPack struct {
  BeginTs Timestamp
  EndTs Timestamp
  Msgs []*TsMsg
}

type TsMsgMarshaler interface {
  Marshal(input *TsMsg) ([]byte, Status)
  Unmarshal(input []byte) (*TsMsg, Status)
}

type MsgStream interface {
  SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
  Produce(*MsgPack) Status
  Consume() *MsgPack // message can be consumed exactly once
}

type HashFunc func(*MsgPack) map[int32]*MsgPack

type PulsarMsgStream struct {
  client *pulsar.Client
  msgHashFunc HashFunc // return a map from produceChannel idx to *MsgPack
  producers []*pulsar.Producer
  consumers []*pulsar.Consumer
  msgMarshaler *TsMsgMarshaler
  msgUnmarshaler *TsMsgMarshaler
}

func (ms *PulsarMsgStream) SetProducerChannels(channels []string)
func (ms *PulsarMsgStream) SetConsumerChannels(channels []string)
func (ms *PulsarMsgStream) SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
func (ms *PulsarMsgStream) SetMsgHashFunc(hashFunc *HashFunc)
func (ms *PulsarMsgStream) Produce(msgs *MsgPack) Status
func (ms *PulsarMsgStream) Consume() *MsgPack //return messages in one time tick

type PulsarTtMsgStream struct {
  client *pulsar.Client
  msgHashFunc (*MsgPack) map[int32]*MsgPack // return a map from produceChannel idx to *MsgPack
  producers []*pulsar.Producer
  consumers []*pulsar.Consumer
  msgMarshaler *TsMsgMarshaler
  msgUnmarshaler *TsMsgMarshaler
  inputBuf []*TsMsg
  unsolvedBuf []*TsMsg
  msgPacks []*MsgPack
}

func (ms *PulsarMsgStream) SetProducerChannels(channels []string)
func (ms *PulsarMsgStream) SetConsumerChannels(channels []string)
func (ms *PulsarMsgStream) SetMsgMarshaler(marshal *TsMsgMarshaler, unmarshal *TsMsgMarshaler)
func (ms *PulsarMsgStream) SetMsgHashFunc(hashFunc *HashFunc)
func (ms *PulsarMsgStream) Produce(msgs *MsgPack) Status
func (ms *PulsarMsgStream) Consume() *MsgPack //return messages in one time tick
```



## 8. Query Node



#### 8.1 Collection and Segment Meta

###### 8.1.1 Collection

``` go
type Collection struct {
  Name string
  Id uint64
  Fields map[string]FieldMeta
  SegmentsId []uint64
  
  cCollectionSchema C.CCollectionSchema
}
```



###### 8.1.2 Field Meta

```go
type FieldMeta struct {
  Name string
  Id uint64
  IsPrimaryKey bool
  TypeParams map[string]string
  IndexParams map[string]string
}
```



###### 8.1.3 Segment

``` go
type Segment struct {
  Id uint64
  ParitionName string
  CollectionId uint64
  OpenTime Timestamp
  CloseTime Timestamp
  NumRows uint64
  
  cSegment C.CSegmentBase
}
```



#### 8.2 Message Streams

```go
type ManipulationReqUnmarshaler struct {}

// implementations of MsgUnmarshaler interfaces
func (unmarshaler *InsertMsgUnmarshaler) Unmarshal(input *pulsar.Message) (*TsMsg, Status)


type QueryReqUnmarshaler struct {}

// implementations of MsgUnmarshaler interfaces
func (unmarshaler *QueryReqUnmarshaler) Unmarshal(input *pulsar.Message) (*TsMsg, Status)
```



#### 8.3 Query Node









## 4. Storage Engine



#### 4.X Interfaces





## 5. Master



#### 5.1 Interfaces (RPC)

| RPC                | description                                                  |
| :----------------- | ------------------------------------------------------------ |
| CreateCollection   | create a collection base on schema statement                 |
| DropCollection     | drop a collection                                            |
| HasCollection      | whether or not a collection exists                           |
| DescribeCollection | show a collection's schema and its descriptive statistics    |
| ShowCollections    | list all collections                                         |
| CreatePartition    | create a partition                                           |
| DropPartition      | drop a partition                                             |
| HasPartition       | whether or not a partition exists                            |
| DescribePartition  | show a partition's name and its descriptive statistics       |
| ShowPartitions     | list a collection's all partitions                           |
| AllocTimestamp     | allocate a batch of consecutive timestamps                   |
| AllocId            | allocate a batch of consecutive IDs                          |
| AssignSegmentId    | assign segment id to insert rows (master determines which segment these rows belong to) |
|                    |                                                              |
|                    |                                                              |



#### 5.2 Master Instance

```go
type Master interface {
  tso timestampOracle	// timestamp oracle
  ddScheduler ddRequestScheduler // data definition request scheduler
  metaTable metaTable // in-memory system meta
  collManager collectionManager // collection & partition manager
  segManager segmentManager // segment manager
}
```

* Timestamp allocation

Master serves as a centrol clock of the whole system. Other components (i.e. Proxy) allocates timestamps from master via RPC *AllocTimestamp*. All the timestamp allocation requests will be handled by the timestampOracle singleton. See section 4.2 for the details about timestampOracle.

* Request Scheduling

* System Meta

* Collection Management

* Segment Management



#### 5.3 Data definition Request Scheduler

###### 5.2.1 Task

Master receives data definition requests via grpc. Each request (described by a proto) will be wrapped as a task for further scheduling. The task interface is

```go
type task interface {
  Type() ReqType
  Ts() Timestamp
  Execute() Status
  WaitToFinish() Status
  Notify() Status
}
```

A task example is as follows. In this example, we wrap a CreateCollectionRequest (a proto) as a createCollectionTask. The wrapper need to contain task interfaces. 

``` go
type createCollectionTask struct {
  req *CreateCollectionRequest
  cv int chan
}

// Task interfaces
func (task *createCollectionTask) Type() ReqType
func (task *createCollectionTask) Ts() Timestamp
func (task *createCollectionTask) Execute() Status
func (task *createCollectionTask) Notify() Status
func (task *createCollectionTask) WaitToFinish() Status
```



###### 5.2.2 Scheduler

```go
type ddRequestScheduler struct {
  reqQueue *task chan
}

func (rs *ddRequestScheduler) Enqueue(task *task) Status
func (rs *ddRequestScheduler) schedule() *task // implement scheduling policy
```



#### 5.4 Meta Table

```go
type metaTable struct {
  client *etcd.Client // client of a reliable kv service, i.e. etcd client
  rootPath string // this metaTable's working root path on the reliable kv service
  tenantMeta map[int64]TenantMeta // tenant id to tenant meta
  proxyMeta map[int64]ProxyMeta // proxy id to proxy meta
  collMeta map[int64]CollectionMeta // collection id to collection meta
  segMeta map[int64]SegmentMeta // segment id to segment meta
}
```



