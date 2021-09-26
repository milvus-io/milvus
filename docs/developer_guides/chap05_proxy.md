## 5. Proxy



<img src="./figs/proxy.png" width=700>



#### 5.0 Proxy Service Interface

```go
type ProxyService interface {
	Component
	TimeTickProvider

	RegisterNode(ctx context.Context, request *proxypb.RegisterNodeRequest) (*proxypb.RegisterNodeResponse, error)
	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
}
```

* *MsgBase*

```go

type MsgBase struct {
	MsgType   MsgType
	MsgID     UniqueID
	Timestamp uint64
	SourceID  UniqueID
}
```

* *RegisterNode*

```go
type Address struct {
	Ip   string
	Port int64
}

type RegisterNodeRequest struct {
	Base    *commonpb.MsgBase
	Address string
	Port    int64
}

type InitParams struct {
	NodeID      UniqueID
	StartParams []*commonpb.KeyValuePair
}

type RegisterNodeResponse struct {
	InitParams *internalpb.InitParams
	Status     *commonpb.Status
}
```

* *InvalidateCollectionMetaCache*

```go
type InvalidateCollMetaCacheRequest struct {
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
}
```



#### 5.1 Proxy Node Interface

```go
type Proxy interface {
	Component
	
	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error)
}
```

* *InvalidateCollectionMetaCache*

```go
type InvalidateCollMetaCacheRequest struct {
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
}
```

#### 5.2 Milvus Service Interface

Proxy also implements Milvus Service interface to receive client grpc call.

```go
type MilvusService interface {
	CreateCollection(ctx context.Context, request *milvuspb.CreateCollectionRequest) (*commonpb.Status, error)
	DropCollection(ctx context.Context, request *milvuspb.DropCollectionRequest) (*commonpb.Status, error)
	HasCollection(ctx context.Context, request *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error)
	LoadCollection(ctx context.Context, request *milvuspb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, request *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error)
	DescribeCollection(ctx context.Context, request *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	GetCollectionStatistics(ctx context.Context, request *milvuspb.CollectionStatsRequest) (*milvuspb.CollectionStatsResponse, error)
	ShowCollections(ctx context.Context, request *milvuspb.ShowCollectionRequest) (*milvuspb.ShowCollectionResponse, error)
	
	CreateAlias(ctx context.Context, request *milvuspb.CreateAliasRequest) (*commonpb.Status, error)
	DropAlias(ctx context.Context, request *milvuspb.DropAliasRequest) (*commonpb.Status, error)
	AlterAlias(ctx context.Context, request *milvuspb.AlterAliasRequest) (*commonpb.Status, error)

	CreatePartition(ctx context.Context, request *milvuspb.CreatePartitionRequest) (*commonpb.Status, error)
	DropPartition(ctx context.Context, request *milvuspb.DropPartitionRequest) (*commonpb.Status, error)
	HasPartition(ctx context.Context, request *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error)
	LoadPartitions(ctx context.Context, request *milvuspb.LoadPartitonRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, request *milvuspb.ReleasePartitionRequest) (*commonpb.Status, error)
	GetPartitionStatistics(ctx context.Context, request *milvuspb.PartitionStatsRequest) (*milvuspb.PartitionStatsResponse, error)
	ShowPartitions(ctx context.Context, request *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
	
	CreateIndex(ctx context.Context, request *milvuspb.CreateIndexRequest) (*commonpb.Status, error)
	DescribeIndex(ctx context.Context, request *milvuspb.DescribeIndexRequest) (*milvuspb.DescribeIndexResponse, error)
	GetIndexState(ctx context.Context, request *milvuspb.IndexStateRequest) (*milvuspb.IndexStateResponse, error)
	DropIndex(ctx context.Context, request *milvuspb.DropIndexRequest) (*commonpb.Status, error)
	
	Insert(ctx context.Context, request *milvuspb.InsertRequest) (*milvuspb.InsertResponse, error)
	Search(ctx context.Context, request *milvuspb.SearchRequest) (*milvuspb.SearchResults, error)
	Flush(ctx context.Context, request *milvuspb.FlushRequest) (*commonpb.Status, error)
	
	GetDdChannel(ctx context.Context, request *commonpb.Empty) (*milvuspb.StringResponse, error)
	
	GetQuerySegmentInfo(ctx context.Context, req *milvuspb.QuerySegmentInfoRequest) (*milvuspb.QuerySegmentInfoResponse, error)
	GetPersistentSegmentInfo(ctx context.Context, req *milvuspb.PersistentSegmentInfoRequest) (*milvuspb.PersistentSegmentInfoResponse, error)
}
}
```

* *CreateCollection*

See *Master API* for detailed definitions.

* *DropCollection*

See *Master API* for detailed definitions.

* *HasCollection*

See *Master API* for detailed definitions.

* *LoadCollection*

```go
type LoadCollectionRequest struct {
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
}
```

* *ReleaseCollection*

```go
type ReleaseCollectionRequest struct {
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
}
```

* *DescribeCollection*

See *Master API* for detailed definitions.

* *GetCollectionStatistics*

See *Master API* for detailed definitions.

* *ShowCollections*

See *Master API* for detailed definitions.

* *CreateAlias*

See *Master API* for detailed definitions.

* *DropAlias*

See *Master API* for detailed definitions.

* *AlterAlias*

See *Master API* for detailed definitions.

* *CreatePartition*

See *Master API* for detailed definitions.

* *DropPartition*

See *Master API* for detailed definitions.

* *HasPartition*

See *Master API* for detailed definitions.

* *LoadPartitions*

```go
type CollectionSchema struct {
	Name        string
	Description string
	AutoID      bool
	Fields      []*FieldSchema
}

type LoadPartitonRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	PartitionIDs []UniqueID
	Schema       *schemapb.CollectionSchema
}
```

* *ReleasePartitions*

```go
type ReleasePartitionRequest struct {
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
	PartitionNames []string
}
```

* *GetPartitionStatistics*

See *Master API* for detailed definitions.

* *ShowPartitions*

See *Master API* for detailed definitions.

* *CreateIndex*

See *Master API* for detailed definitions.

* *DescribeIndex*

See *Master API* for detailed definitions.

* *DropIndex*

See *Master API* for detailed definitions.

* *Insert*

```go
type InsertRequest struct {
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
	PartitionName  string
	RowData        []Blob
	HashKeys       []uint32
}

type InsertResponse struct {
	Status     *commonpb.Status
	RowIDBegin int64
	RowIDEnd   int64
}
```

* *Search*

```go
type SearchRequest struct {
	Base             *commonpb.MsgBase
	DbName           string
	CollectionName   string
	PartitionNames   []string
	Dsl              string
	PlaceholderGroup []byte
}

type SearchResults struct {
	Status commonpb.Status
	Hits   byte
}
```

* *Flush*

```go
type FlushRequest struct {
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
}
```


* *GetPersistentSegmentInfo*

```go
type PersistentSegmentInfoRequest  struct{
	Base           *commonpb.MsgBase
	DbName         string
	CollectionName string
}

type SegmentState int32

const (
	SegmentState_SegmentNone     SegmentState = 0
	SegmentState_SegmentNotExist SegmentState = 1
	SegmentState_SegmentGrowing  SegmentState = 2
	SegmentState_SegmentSealed   SegmentState = 3
	SegmentState_SegmentFlushed  SegmentState = 4
)

type PersistentSegmentInfo struct {
	SegmentID    UniqueID
	CollectionID UniqueID
	PartitionID  UniqueID
	OpenTime     Timestamp
	SealedTime   Timestamp
	FlushedTime  Timestamp
	NumRows      int64
	MemSize      int64
	State        SegmentState
}

type PersistentSegmentInfoResponse  struct{
	infos []*milvuspb.SegmentInfo
}

```

#### 5.3 Proxy Instance

```go
type Proxy struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup
	
	initParams *internalpb.InitParams
	ip         string
	port       int
	
	stateCode internalpb.StateCode
	
	rootCoordClient  RootCoordClient
	indexCoordClient IndexCoordClient
	dataCoordClient  DataCoordClient
	queryCoordClient QueryCoordClient
	
	sched *TaskScheduler
	tick  *timeTick
	
	idAllocator  *allocator.IDAllocator
	tsoAllocator *allocator.TimestampAllocator
	segAssigner  *SegIDAssigner
	
	manipulationMsgStream msgstream.MsgStream
	queryMsgStream        msgstream.MsgStream
	msFactory             msgstream.Factory
	
	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

func (node *NodeImpl) Init() error
func (node *NodeImpl) Start() error
func (node *NodeImpl) Stop() error
func (node *NodeImpl) AddStartCallback(callbacks ...func())
func (node *NodeImpl) waitForServiceReady(ctx context.Context, service Component, serviceName string) error
func (node *NodeImpl) lastTick() Timestamp
func (node *NodeImpl) AddCloseCallback(callbacks ...func())
func (node *NodeImpl) SetRootCoordClient(cli RootCoordClient)
func (node *NodeImpl) SetIndexCoordClient(cli IndexCoordClient)
func (node *NodeImpl) SetDataCoordClient(cli DataCoordClient)
func (node *NodeImpl) SetProxyCoordClient(cli ProxyCoordClient)
func (node *NodeImpl) SetQueryCoordClient(cli QueryCoordClient)

func NewProxyImpl(ctx context.Context, factory msgstream.Factory) (*NodeImpl, error)
```

#### Global Parameter Table

```go
type GlobalParamsTable struct {
	paramtable.BaseTable
	
	NetworkPort    int
	IP             string
	NetworkAddress string
	
	MasterAddress string
	PulsarAddress string
	RocksmqPath   string

	RocksmqRetentionTimeInMinutes int64
	RocksmqRetentionSizeInMB 	  int64
	
	ProxyID                            UniqueID
	TimeTickInterval                   time.Duration
	InsertChannelNames                 []string
	DeleteChannelNames                 []string
	K2SChannelNames                    []string
	SearchChannelNames                 []string
	SearchResultChannelNames           []string
	ProxySubName                       string
	ProxyTimeTickChannelNames          []string
	DataDefinitionChannelNames         []string
	MsgStreamTimeTickBufSize           int64
	MaxNameLength                      int64
	MaxFieldNum                        int64
	MaxDimension                       int64
	DefaultPartitionName               string
	DefaultIndexName                   string
}

var Params ParamTable
```


#### 5.4 Task

``` go
type task interface {
	TraceCtx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	Type() commonpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	OnEnqueue() error
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
}
```

#### 5.5 Task Scheduler

* Base Task Queue

```go
type TaskQueue interface {
	utChan() <-chan int
	UTEmpty() bool
	utFull() bool
	addUnissuedTask(t task) error
	FrontUnissuedTask() task
	PopUnissuedTask() task
	AddActiveTask(t task)
	PopActiveTask(ts Timestamp) task
	getTaskByReqID(reqID UniqueID) task
	TaskDoneTest(ts Timestamp) bool
	Enqueue(t task) error
}

type baseTaskQueue struct {
	unissuedTasks *list.List
	activeTasks   map[Timestamp]task
	utLock        sync.Mutex
	atLock        sync.Mutex
	
	maxTaskNum int64
	
	utBufChan chan int
	
	sched *TaskScheduler
}
```

*AddUnissuedTask(task \*task)* will put a new task into *unissuedTasks*, while maintaining the list by timestamp order.

*TaskDoneTest(ts Timestamp)* will check both *unissuedTasks* and *unissuedTasks*. If no task found before *ts*, then the function returns *true*, indicates that all the tasks before *ts* are completed.



* Data Definition Task Queue

```go
type ddTaskQueue struct {
	baseTaskQueue
	lock sync.Mutex
}
func (queue *ddTaskQueue) Enqueue(task *task) error

func newDdTaskQueue() *ddTaskQueue
```

Data definition tasks (i.e. *CreateCollectionTask*) will be put into *DdTaskQueue*. If a task is enqueued, *Enqueue(task \*task)* will set *Ts*, *ReqId*, *ProxyId*, then push it into *queue*. The timestamps of the enqueued tasks should be strictly monotonically increasing. As *Enqueue(task \*task)* will be called in parallel, setting timestamp and queue insertion need to be done atomically.



* Data Manipulation Task Queue

```go
type dmTaskQueue struct {
	baseTaskQueue
}
func (queue *dmTaskQueue) Enqueue(task *task) error

func newDmTaskQueue() *dmTaskQueue
```

Insert tasks and delete tasks will be put into *DmTaskQueue*.

If a *insertTask* is enqueued, *Enqueue(task \*task)* will set *Ts*, *ReqId*, *ProxyId*, *SegIdAssigner*, *RowIdAllocator*, then push it into *queue*. The *SegIdAssigner* and *RowIdAllocator* will later be used in the task's execution phase.



* Data Query Task Queue

```go
type dqTaskQueue struct {
	baseTaskQueue
}
func (queue *dqTaskQueue) Enqueue(task *task) error

func newDqTaskQueue() *dqTaskQueue
```

Queries will be put into *DqTaskQueue*.



* Task Scheduler

``` go
type taskScheduler struct {
	DdQueue TaskQueue
	DmQueue TaskQueue
	DqQueue TaskQueue
	
	idAllocator  *allocator.IDAllocator
	tsoAllocator *allocator.TimestampAllocator
	
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	
	msFactory msgstream.Factory
}

func (sched *taskScheduler) scheduleDdTask() *task
func (sched *taskScheduler) scheduleDmTask() *task
func (sched *taskScheduler) scheduleDqTask() *task
func (sched *TaskScheduler) getTaskByReqID(collMeta UniqueID) task
func (sched *TaskScheduler) processTask(t task, q TaskQueue)

func (sched *taskScheduler) Start() error
func (sched *taskScheduler) TaskDoneTest(ts Timestamp) bool

func NewTaskScheduler(ctx context.Context, idAllocator *allocator.IDAllocator, tsoAllocator *allocator.TimestampAllocator,
	factory msgstream.Factory) (*TaskScheduler, error)
```

*scheduleDdTask()* selects tasks in a FIFO manner, thus time order is garanteed.

The policy of *scheduleDmTask()* should target on throughput, not tasks' time order.  Note that the time order of the tasks' execution will later be garanteed by the timestamp & time tick mechanism.

The policy of *scheduleDqTask()* should target on throughput. It should also take visibility into consideration. For example, if an insert task and a query arrive in a same time tick and the query comes after insert, the query should be scheduled in the next tick thus the query can see the insert.

*TaskDoneTest(ts Timestamp)* will check all the three task queues. If no task found before *ts*, then the function returns *true*, indicates that all the tasks before *ts* are completed.



* Statistics

// TODO
```go
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



// TODO
#### 5.6 Time Tick

* Time Tick

``` go
type timeTick struct {
	lastTick Timestamp
	currentTick Timestamp
	wallTick Timestamp
	tickStep Timestamp
	syncInterval Timestamp
	
	tsAllocator *TimestampAllocator
	scheduler *taskScheduler
	ttStream *MessageStream
	
	ctx context.Context
}

func (tt *timeTick) Start() error
func (tt *timeTick) synchronize() error

func newTimeTick(ctx context.Context, tickStep Timestamp, syncInterval Timestamp, tsAllocator *TimestampAllocator, scheduler *taskScheduler, ttStream *MessageStream) *timeTick
```

*Start()* will enter a loop. On each *tickStep*, it tries to send a *TIME_TICK* typed *TsMsg* into *ttStream*. After each *syncInterval*, it sychronizes its *wallTick* with *tsAllocator* by calling *synchronize()*. When *currentTick + tickStep < wallTick* holds, it will update *currentTick* with *wallTick* on next tick. Otherwise, it will update *currentTick* with *currentTick + tickStep*.


* Statistics

```go
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



