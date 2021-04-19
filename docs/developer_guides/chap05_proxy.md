

## 6. Proxy

#### 6.0 Proxy Service API

```protobuf
message Credential {
	string address
	//TODO: we should add keys/tokens here
}

message ProxyInfo {
	common.Status
	string address
	int32 port
}

service ProxyService {
	rpc RegisterLink(Credential) returns (ProxyInfo){}	//TODO: call IAM
}
```





#### 6.1 Proxy Instance

```go
type Proxy struct {
  servicepb.UnimplementedMilvusServiceServer
  masterClient mpb.MasterClient
  
  timeTick *timeTick
  ttStream *MessageStream
  scheduler *taskScheduler
  tsAllocator *TimestampAllocator
  ReqIdAllocator *IdAllocator
  RowIdAllocator *IdAllocator
  SegIdAssigner *segIdAssigner
}

func (proxy *Proxy) Start() error
func NewProxy(ctx context.Context) *Proxy
```

#### Global Parameter Table

```go
type GlobalParamsTable struct {
}
func (*paramTable GlobalParamsTable) ProxyId() int64
func (*paramTable GlobalParamsTable) ProxyAddress() string
func (*paramTable GlobalParamsTable) MasterAddress() string
func (*paramTable GlobalParamsTable) PulsarAddress() string
func (*paramTable GlobalParamsTable) TimeTickTopic() string
func (*paramTable GlobalParamsTable) InsertTopics() []string
func (*paramTable GlobalParamsTable) QueryTopic() string
func (*paramTable GlobalParamsTable) QueryResultTopics() []string
func (*paramTable GlobalParamsTable) Init() error

var ProxyParamTable GlobalParamsTable
```





#### 6.2 Task

``` go
type task interface {
  Id() int64	// return ReqId
  PreExecute() error
  Execute() error
  PostExecute() error
  WaitToFinish() error
  Notify() error
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

func (task *baseTask) PreExecute() error
func (task *baseTask) Execute() error
func (task *baseTask) PostExecute() error
func (task *baseTask) WaitToFinish() error
func (task *baseTask) Notify() error
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

func (task *InsertTask) Execute() error
func (task *InsertTask) WaitToFinish() error
func (task *InsertTask) Notify() error
```



#### 6.2 Task Scheduler

* Base Task Queue

```go
type baseTaskQueue struct {
  unissuedTasks *List
  activeTasks map[int64]*task
  utLock sync.Mutex	// lock for UnissuedTasks
  atLock sync.Mutex	// lock for ActiveTasks
}
func (queue *baseTaskQueue) AddUnissuedTask(task *task)
func (queue *baseTaskQueue) FrontUnissuedTask() *task
func (queue *baseTaskQueue) PopUnissuedTask(id int64) *task
func (queue *baseTaskQueue) AddActiveTask(task *task)
func (queue *baseTaskQueue) PopActiveTask(id int64) *task
func (queue *baseTaskQueue) TaskDoneTest(ts Timestamp) bool
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
  DdQueue *ddTaskQueue
  DmQueue *dmTaskQueue
  DqQueue *dqTaskQueue
  
  tsAllocator *TimestampAllocator
  ReqIdAllocator *IdAllocator
}

func (sched *taskScheduler) scheduleDdTask() *task
func (sched *taskScheduler) scheduleDmTask() *task
func (sched *taskScheduler) scheduleDqTask() *task

func (sched *taskScheduler) Start() error
func (sched *taskScheduler) TaskDoneTest(ts Timestamp) bool

func newTaskScheduler(ctx context.Context, tsAllocator *TimestampAllocator, ReqIdAllocator *IdAllocator) *taskScheduler
```

*scheduleDdTask()* selects tasks in a FIFO manner, thus time order is garanteed.

The policy of *scheduleDmTask()* should target on throughput, not tasks' time order.  Note that the time order of the tasks' execution will later be garanteed by the timestamp & time tick mechanism.

The policy of *scheduleDqTask()* should target on throughput. It should also take visibility into consideration. For example, if an insert task and a query arrive in a same time tick and the query comes after insert, the query should be scheduled in the next tick thus the query can see the insert.

*TaskDoneTest(ts Timestamp)* will check all the three task queues. If no task found before *ts*, then the function returns *true*, indicates that all the tasks before *ts* are completed.



* Statistics

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



#### 6.3 Time Tick

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



