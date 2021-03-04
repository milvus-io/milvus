

## 10. Master

<img src="./figs/master.jpeg" width=700>

#### 10.1 Master Interface

```go
type Master interface {
  Service
  GetComponentStates(ctx context.Context) (*internalpb2.ComponentStates, error)
  
  //DDL request
  CreateCollection(ctx context.Context, in *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
  DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
  HasCollection(ctx context.Context, in *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
  DescribeCollection(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
  ShowCollections(ctx context.Context, in *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
  CreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
  DropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
  HasPartition(ctx context.Context, in *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
  ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
  
  //index builder service
  CreateIndex(ctx context.Context, in *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
  DescribeIndex(ctx context.Context, in *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
  DropIndex(ctx context.Context, in *milvuspb.DropIndexRequest) (*commonpb.Status, error)
  
  //global timestamp allocator
  AllocTimestamp(ctx context.Context, in *masterpb.TsoRequest) (*masterpb.TsoResponse, error)
  AllocID(ctx context.Context, in *masterpb.IDRequest) (*masterpb.IDResponse, error)
  
  //receiver time tick from proxy service, and put it into this channel
  GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error)
  
  //receive ddl from rpc and time tick from proxy service, and put them into this channel
  GetDdChannel(ctx context.Context) (*milvuspb.StringResponse, error)
  
  //just define a channel, not used currently
  GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error)
  
  //segment
  DescribeSegment(ctx context.Context, in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)
  ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error)
}
```



* *MsgBase*

```go
type MsgBase struct {
  MsgType   MsgType
  MsgID	    UniqueID
  Timestamp Timestamp
  SourceID  UniqueID
}
```

* *CreateCollection*

```go
type CreateCollectionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  Schema         []byte
}
```

* *DropCollection*

```go
type DropCollectionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
}
```

* *HasCollection*

```go
type HasCollectionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
}
```

* *DescribeCollection*

```go
type DescribeCollectionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  CollectionID   UniqueID
}

type CollectionSchema struct {
  Name        string
  Description string
  AutoID      bool
  Fields      []*FieldSchema
}

type DescribeCollectionResponse struct {
  Status       *commonpb.Status
  Schema       *schemapb.CollectionSchema
  CollectionID int64
}
```

* *ShowCollections*

```go
type ShowCollectionRequest struct {
  Base   *commonpb.MsgBase
  DbName string
}

type ShowCollectionResponse struct {
  Status          *commonpb.Status
  CollectionNames []string
}
```

* *CreatePartition*

```go
type CreatePartitionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  PartitionName  string
}
```

* *DropPartition*

```go
type DropPartitionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  PartitionName  string
}
```

* *HasPartition*

```go
type HasPartitionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  PartitionName  string
}
```

* *ShowPartitions*

```go
type ShowPartitionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  CollectionID   UniqueID
}

type ShowPartitionResponse struct {
  Status         *commonpb.Status
  PartitionNames []string
  PartitionIDs   []UniqueID
}
```

* DescribeSegment

```go
type DescribeSegmentRequest struct {
  Base         *commonpb.MsgBase
  CollectionID UniqueID
  SegmentID    UniqueID
}

type DescribeSegmentResponse struct {
  Status  *commonpb.Status
  IndexID UniqueID
  BuildID UniqueID
}
```

* ShowSegments

```go
type ShowSegmentRequest struct {
  Base         *commonpb.MsgBase
  CollectionID UniqueID
  PartitionID  UniqueID
}

type ShowSegmentResponse struct {
  Status     *commonpb.Status
  SegmentIDs []UniqueID
}
```

* *CreateIndex*

```go
type CreateIndexRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  FieldName      string
  ExtraParams    []*commonpb.KeyValuePair
}
```

* *DescribeIndex*

```go
type DescribeIndexRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  FieldName      string
  IndexName      string
}

type IndexDescription struct {
  IndexName string
  IndexID   UniqueID
  params    []*commonpb.KeyValuePair
}

type DescribeIndexResponse struct {
  Status            *commonpb.Status
  IndexDescriptions []*IndexDescription
}
```

* *DropIndex*

```go
type DropIndexRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  FieldName      string
  IndexName      string
}
```

* *AllocTimestamp*

```go
type TsoRequest struct {
  Base  *commonpb.MsgBase
  Count uint32
}

type TsoResponse struct {
  Status    *commonpb.Status
  Timestamp uint64
  Count     uint32
}
```

* *AllocID*

```go
type IDRequest struct {
  Base  *commonpb.MsgBase
  Count uint32
}

type IDResponse struct {
  Status *commonpb.Status
  ID     UniqueID
  Count  uint32
}
```



#### 10.2 Dd (Data definitions) Channel

* *CreateCollection*

```go
type CreateCollectionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  
  DbID         UniqueID
  CollectionID UniqueID
  Schema       []byte
}
```

* *DropCollection*

```go
type DropCollectionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  DbID           UniqueID
  CollectionID   UniqueID
}
```

* *CreatePartition*

```go
type CreatePartitionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  PartitionName  string
  DbID           UniqueID
  CollectionID   UniqueID
  PartitionID    UniqueID
}
```

* *DropPartition*

```go
type DropPartitionRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  PartitionName  string
  DbID           UniqueID
  CollectionID   UniqueID
  PartitionID    UniqueID
}
```

* *CreateIndex*

```go
type CreateIndexRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  FieldName      string
  DbID           UniqueID
  CollectionID   UniqueID
  FieldID        UniqueID
  ExtraParams    []*commonpb.KeyValuePair
}
```



#### 10.2 Master Instance

```go
type Master interface {
  MetaTable *metaTable
  //id allocator
  idAllocator *allocator.GlobalIDAllocator
  //tso allocator
  tsoAllocator *tso.GlobalTSOAllocator
  
  //inner members
  ctx     context.Context
  cancel  context.CancelFunc
  etcdCli *clientv3.Client
  kvBase  *etcdkv.EtcdKV
  metaKV  *etcdkv.EtcdKV
  
  //setMsgStreams, receive time tick from proxy service time tick channel
  ProxyTimeTickChan chan typeutil.Timestamp
  
  //setMsgStreams, send time tick into dd channel and time tick channel
  SendTimeTick func(t typeutil.Timestamp) error
  
  //setMsgStreams, send create collection into dd channel
  DdCreateCollectionReq func(req *internalpb2.CreateCollectionRequest) error
  
  //setMsgStreams, send drop collection into dd channel, and notify the proxy to delete this collection
  DdDropCollectionReq func(req *internalpb2.DropCollectionRequest) error
  
  //setMsgStreams, send create partition into dd channel
  DdCreatePartitionReq func(req *internalpb2.CreatePartitionRequest) error
  
  //setMsgStreams, send drop partition into dd channel
  DdDropPartitionReq func(req *internalpb2.DropPartitionRequest) error
  
  //setMsgStreams segment channel, receive segment info from data service, if master create segment
  DataServiceSegmentChan chan *datapb.SegmentInfo
  
  //setMsgStreams ,if segment flush completed, data node would put segment id into msg stream
  DataNodeSegmentFlushCompletedChan chan typeutil.UniqueID
  
  //get binlog file path from data service,
  GetBinlogFilePathsFromDataServiceReq func(segID typeutil.UniqueID, fieldID typeutil.UniqueID) ([]string, error)
  
  //call index builder's client to build index, return build id
  BuildIndexReq func(binlog []string, typeParams []*commonpb.KeyValuePair, indexParams []*commonpb.KeyValuePair, indexID typeutil.UniqueID, indexName string) (typeutil.UniqueID, error)
  DropIndexReq  func(indexID typeutil.UniqueID) error
  
  //proxy service interface, notify proxy service to drop collection
  InvalidateCollectionMetaCache func(ts typeutil.Timestamp, dbName string, collectionName string) error
  
  //query service interface, notify query service to release collection
  ReleaseCollection func(ts typeutil.Timestamp, dbID typeutil.UniqueID, collectionID typeutil.UniqueID) error
  
  // put create index task into this chan
  indexTaskQueue chan *CreateIndexTask
  
  //dd request scheduler
  ddReqQueue      chan reqTask //dd request will be push into this chan
  lastDdTimeStamp typeutil.Timestamp
  
  //time tick loop
  lastTimeTick typeutil.Timestamp
  
  //states code
  stateCode atomic.Value
  
  //call once
  initOnce  sync.Once
  startOnce sync.Once
  //isInit    atomic.Value
  
  msFactory ms.Factory
}
```


#### 10.3 Data definition Request Scheduler

###### 10.2.1 Task

Master receives data definition requests via grpc. Each request (described by a proto) will be wrapped as a task for further scheduling. The task interface is

```go
type task interface {
  Type() commonpb.MsgType
  Ts() (typeutil.Timestamp, error)
  IgnoreTimeStamp() bool
  Execute() error
  WaitToFinish() error
  Notify(err error)
}
```



A task example is as follows. In this example, we wrap a CreateCollectionRequest (a proto) as a createCollectionTask. The wrapper need to implement task interfaces. 

``` go
type createCollectionTask struct {
  req *CreateCollectionRequest
  cv int chan
}

// Task interfaces
func (task *createCollectionTask) Type() ReqType
func (task *createCollectionTask) Ts() Timestamp
func (task *createCollectionTask) IgnoreTimeStamp() bool
func (task *createCollectionTask) Execute() error
func (task *createCollectionTask) Notify() error
func (task *createCollectionTask) WaitToFinish() error
```



// TODO
###### 10.2.3 Scheduler

```go
type ddRequestScheduler struct {
  reqQueue *task chan
  ddStream *MsgStream
}

func (rs *ddRequestScheduler) Enqueue(task *task) error
func (rs *ddRequestScheduler) schedule() *task // implement scheduling policy
```

In most cases, a data definition task need to

* update system's meta data (via $metaTable$),
* and synchronize the data definition request to other related system components so that the quest can take effect system wide.

Master 





//TODO
#### 10.4 Meta Table

###### 10.4.1 Meta

* Tenant Meta

```protobuf
message TenantMeta {
  uint64 id = 1;
  uint64 num_query_nodes = 2;
  repeated string insert_channel_names = 3;
  string query_channel_name = 4;
}
```

* Proxy Meta

``` protobuf
message ProxyMeta {
  uint64 id = 1;
  common.Address address = 2;
  repeated string result_channel_names = 3;
}
```

* Collection Meta

```protobuf
message CollectionMeta {
  uint64 id=1;
  schema.CollectionSchema schema=2;
  uint64 create_time=3;
  repeated uint64 segment_ids=4;
  repeated string partition_tags=5;
}
```

* Segment Meta

```protobuf
message SegmentMeta {
  uint64 segment_id=1;
  uint64 collection_id =2;
  string partition_tag=3;
  int32 channel_start=4;
  int32 channel_end=5;
  uint64 open_time=6;
  uint64 close_time=7;
  int64 num_rows=8;
}
```



###### 10.4.2 KV pairs in EtcdKV

```go
"tenant/$tenantId" string -> tenantMetaBlob string
"proxy/$proxyId" string -> proxyMetaBlob string
"collection/$collectionId" string -> collectionMetaBlob string
"segment/$segmentId" string -> segmentMetaBlob string
```

Note that *tenantId*, *proxyId*, *collectionId*, *segmentId* are unique strings converted from int64.

*tenantMeta*, *proxyMeta*, *collectionMeta*, *segmentMeta* are serialized protos. 



###### 10.4.3 Meta Table

```go
type metaTable struct {
  client             kv.TxnBase                                                       // client of a reliable kv service, i.e. etcd client
  tenantID2Meta      map[typeutil.UniqueID]pb.TenantMeta                              // tenant id to tenant meta
  proxyID2Meta       map[typeutil.UniqueID]pb.ProxyMeta                               // proxy id to proxy meta
  collID2Meta        map[typeutil.UniqueID]pb.CollectionInfo                          // collection id to collection meta,
  collName2ID        map[string]typeutil.UniqueID                                     // collection name to collection id
  partitionID2Meta   map[typeutil.UniqueID]pb.PartitionInfo                           // partition id -> partition meta
  segID2IndexMeta    map[typeutil.UniqueID]*map[typeutil.UniqueID]pb.SegmentIndexInfo // segment id -> index id -> segment index meta
  indexID2Meta       map[typeutil.UniqueID]pb.IndexInfo                               // index id ->index meta
  segID2CollID       map[typeutil.UniqueID]typeutil.UniqueID                          // segment id -> collection id
  partitionID2CollID map[typeutil.UniqueID]typeutil.UniqueID                          // partition id -> collection id
  
  tenantLock sync.RWMutex
  proxyLock  sync.RWMutex
  ddLock     sync.RWMutex
}

func (mt *metaTable) AddCollection(coll *pb.CollectionInfo, part *pb.PartitionInfo, idx []*pb.IndexInfo) error
func (mt *metaTable) DeleteCollection(collID typeutil.UniqueID) error
func (mt *metaTable) HasCollection(collID typeutil.UniqueID) bool
func (mt *metaTable) GetCollectionByID(collectionID typeutil.UniqueID) (*pb.CollectionInfo, error)
func (mt *metaTable) GetCollectionByName(collectionName string) (*pb.CollectionInfo, error)
func (mt *metaTable) GetCollectionBySegmentID(segID typeutil.UniqueID) (*pb.CollectionInfo, error)
func (mt *metaTable) ListCollections() ([]string, error)
func (mt *metaTable) AddPartition(collID typeutil.UniqueID, partitionName string, partitionID typeutil.UniqueID) error
func (mt *metaTable) HasPartition(collID typeutil.UniqueID, partitionName string) bool
func (mt *metaTable) DeletePartition(collID typeutil.UniqueID, partitionName string) (typeutil.UniqueID, error)
func (mt *metaTable) GetPartitionByID(partitionID typeutil.UniqueID) (pb.PartitionInfo, error)
func (mt *metaTable) AddSegment(seg *datapb.SegmentInfo) error
func (mt *metaTable) AddIndex(seg *pb.SegmentIndexInfo) error
func (mt *metaTable) DropIndex(collName, fieldName, indexName string) (typeutil.UniqueID, bool, error)
func (mt *metaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, filedID int64, idxName string) (pb.SegmentIndexInfo, error)
func (mt *metaTable) GetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error)
func (mt *metaTable) unlockGetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error)
func (mt *metaTable) IsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool
func (mt *metaTable) unlockIsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool
func (mt *metaTable) GetNotIndexedSegments(collName string, fieldName string, idxInfo *pb.IndexInfo) ([]typeutil.UniqueID, schemapb.FieldSchema, error)
func (mt *metaTable) GetIndexByName(collName string, fieldName string, indexName string) ([]pb.IndexInfo, error)
func (mt *metaTable) GetIndexByID(indexID typeutil.UniqueID) (*pb.IndexInfo, error)

func NewMetaTable(kv kv.TxnBase) (*metaTable, error)
```

*metaTable* maintains meta both in memory and *etcdKV*. It keeps meta's consistency in both sides. All its member functions may be called concurrently.

*  *AddSegment(seg \*SegmentMeta)* first update *CollectionMeta* by adding the segment id, then it adds a new SegmentMeta to *kv*. All the modifications are done transactionally.



#### 10.5 System Time Synchronization



###### 10.5.1 Time Tick Barrier

//TODO
* Soft Time Tick Barrier


<img src="./figs/soft_time_tick_barrier.png" width=500>

```go
type softTimeTickBarrier struct {
  peer2LastTt   map[UniqueID]Timestamp
  minTtInterval Timestamp
  lastTt        int64
  outTt         chan Timestamp
  ttStream      ms.MsgStream
  ctx           context.Context
}

func (ttBarrier *softTimeTickBarrier) GetTimeTick() (Timestamp,error)
func (ttBarrier *softTimeTickBarrier) Start()
func (ttBarrier *softTimeTickBarrier) Close()

func NewSoftTimeTickBarrier(ctx context.Context, ttStream ms.MsgStream, peerIds []UniqueID, minTtInterval Timestamp) *softTimeTickBarrier
```



* Hard Time Tick Barrier

<img src="./figs/hard_time_tick_barrier.png" width=420>

```go
type hardTimeTickBarrier struct {
  peer2Tt    map[UniqueID]Timestamp
  outTt      chan Timestamp
  ttStream   ms.MsgStream
  ctx        context.Context
  wg         sync.WaitGroup
  loopCtx    context.Context
  loopCancel context.CancelFunc
}

func (ttBarrier *hardTimeTickBarrier) GetTimeTick() (Timestamp,error)
func (ttBarrier *hardTimeTickBarrier) Start()
func (ttBarrier *hardTimeTickBarrier) Close()

func NewHardTimeTickBarrier(ctx context.Context, ttStream ms.MsgStream, peerIds []UniqueID) *hardTimeTickBarrier
```



// TODO
###### 10.5.1 Time Synchronization Message Producer

<img src="./figs/time_sync_msg_producer.png" width=700>


 ```go
type TimeTickBarrier interface {
	GetTimeTick() (Timestamp,error)
	Start()
    Close()
}

type timeSyncMsgProducer struct {
  ctx       context.Context
  cancel    context.CancelFunc
  wg        sync.WaitGroup
  ttBarrier TimeTickBarrier
  watchers  []TimeTickWatcher
}

func (syncMsgProducer *timeSyncMsgProducer) SetProxyTtStreams(proxyTt *MsgStream, proxyIds []UniqueId)
func (syncMsgProducer *timeSyncMsgProducer) SetWriteNodeTtStreams(WriteNodeTt *MsgStream, writeNodeIds []UniqueId)

func (syncMsgProducer *timeSyncMsgProducer) SetDmSyncStream(dmSyncStream *MsgStream)
func (syncMsgProducer *timeSyncMsgProducer) SetK2sSyncStream(k2sSyncStream *MsgStream)

func (syncMsgProducer *timeSyncMsgProducer) Start() error
func (syncMsgProducer *timeSyncMsgProducer) Close() error

func newTimeSyncMsgProducer(ctx context.Context) *timeSyncMsgProducer error
 ```



#### 10.6 System Statistics

###### 10.6.1 Query Node Statistics

```protobuf
message SegmentStats {
  int64 segment_id = 1;
  int64 memory_size = 2;
  int64 num_rows = 3;
  bool recently_modified = 4;
}

message QueryNodeStats {
    int64 id = 1;
    uint64 timestamp = 2;
    repeated SegmentStats seg_stats = 3;
}
```



#### 10.7 Segment Management



//TODO
```go
type assignment struct {
	MemSize    int64
	AssignTime time.Time
}

type segmentStatus struct {
  assignments []*assignment
}

type collectionStatus struct {
  openedSegment []UniqueID
}

type SegmentManagement struct {
  segStatus map[UniqueID]*SegmentStatus
  collStatus map[UniqueID]*collectionStatus
}

func NewSegmentManagement(ctx context.Context) *SegmentManagement
```



//TODO
###### 10.7.1 Assign Segment ID to Inserted Rows

Master receives *AssignSegIDRequest* which contains a list of *SegIDRequest(count, channelName, collectionName, partitionName)* from Proxy. Segment Manager will assign the opened segments or open a new segment if there is no enough space, and Segment Manager will record the allocated space which can be reallocated after a expire duration.

```go
func (segMgr *SegmentManager) AssignSegmentID(segIDReq []*internalpb.SegIDRequest) ([]*internalpb.SegIDAssignment, error)

```



#### 10.8 System Config

```protobuf
// examples of keys:
// "/pulsar/ip"
// "/pulsar/port"
// examples of key_prefixes:
// "/proxy"
// "/msg_stream/insert"

message SysConfigRequest {
    MsgType msg_type = 1;
    int64 reqID = 2;
    int64 proxyID = 3;
    uint64 timestamp = 4;
	repeated string keys = 5;
	repeated string key_prefixes = 6;
}

message SysConfigResponse {
    common.Status status = 1;
	repeated string keys = 2;
	repeated string values = 3;
}
```



```go
type SysConfig struct {
	kv *kv.EtcdKV
}

func (conf *SysConfig) InitFromFile(filePath string) (error)
func (conf *SysConfig) GetByPrefix(keyPrefix string) (keys []string, values []string, err error)
func (conf *SysConfig) Get(keys []string) ([]string, error)
```



configuration examples in etcd:

```
key: root_path/config/master/address
value: "localhost"

key: root_path/config/proxy/timezone
value: "UTC+8"
```

