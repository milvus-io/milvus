

## Appendix A. Basic Components

// TODO
#### A.1 Watchdog

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



#### A.2 Global Parameter Table

``` go
type GlobalParamsTable struct {
  params memoryKV
}

func (gparams *GlobalParamsTable) Save(key, value string) error
func (gparams *GlobalParamsTable) Load(key string) (string, error)
func (gparams *GlobalParamsTable) LoadRange(key, endKey string, limit int) ([]string, []string, error)
func (gparams *GlobalParamsTable) Remove(key string) error
func (gparams *GlobalParamsTable) LoadYaml(filePath string) error
```



* *LoadYaml(filePath string)* turns a YAML file into multiple key-value pairs. For example, given the following YAML

```yaml
etcd:
  address: localhost
  port: 12379
  rootpath: milvus/etcd
```

*GlobalParamsTable.LoadYaml* will insert three key-value pairs into *params*

```go
"etcd.address" -> "localhost"
"etcd.port" -> "12379"
"etcd.rootpath" -> "milvus/etcd"
```



#### A.4 Time Ticked Flow Graph

//TODO
###### A.4.1 Flow Graph States

```go
type flowGraphStates struct {
  startTick Timestamp
  numActiveTasks map[string]int32
  numCompletedTasks map[string]int64
}
```

###### A.4.2 Message

```go
type Msg interface {
  TimeTick() Timestamp
}
```

###### A.4.3 Node

```go
type Node interface {
  Name() string
  MaxQueueLength() int32
  MaxParallelism() int32
  Operate(ctx context.Context, in []Msg) ([]Msg, context.Context)
  IsInputNode() bool
}
```



```go
type baseNode struct {
  maxQueueLength int32
  maxParallelism int32
}
func (node *baseNode) MaxQueueLength() int32
func (node *baseNode) MaxParallelism() int32
func (node *baseNode) SetMaxQueueLength(n int32)
func (node *baseNode) SetMaxParallelism(n int32)
func (node *BaseNode) IsInputNode() bool
```

###### A.4.4 Flow Graph

```go
type nodeCtx struct {
  node                   Node
  inputChannels          []chan *MsgWithCtx
  inputMessages          []Msg
  downstream             []*nodeCtx
  downstreamInputChanIdx map[string]int
  
  NumActiveTasks    int64
  NumCompletedTasks int64
}

func (nodeCtx *nodeCtx) Start(ctx context.Context) error
```

*Start()* will enter a loop. In each iteration, it tries to collect input messges from *inputChan*, then prepare node's input. When input is ready, it will trigger *node.Operate*. When *node.Operate* returns, it sends the returned *Msg* to *outputChans*, which connects to the downstreams' *inputChans*.

```go
type TimeTickedFlowGraph struct {
  ctx     context.Context
  nodeCtx map[NodeName]*nodeCtx
}

func (*pipeline TimeTickedFlowGraph) AddNode(node Node)
func (*pipeline TimeTickedFlowGraph) SetEdges(nodeName string, in []string, out []string)
func (*pipeline TimeTickedFlowGraph) Start() error
func (*pipeline TimeTickedFlowGraph) Close() error

func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph
```



#### A.5 ID Allocator

```go
type IDAllocator struct {
  Allocator
  
  masterAddress string
  masterConn    *grpc.ClientConn
  masterClient  masterpb.MasterServiceClient
  
  countPerRPC uint32
  
  idStart UniqueID
  idEnd   UniqueID
  
  PeerID UniqueID
}

func (ia *IDAllocator) Start() error
func (ia *IDAllocator) connectMaster() error
func (ia *IDAllocator) syncID() bool
func (ia *IDAllocator) checkSyncFunc(timeout bool) bool
func (ia *IDAllocator) pickCanDoFunc()
func (ia *IDAllocator) processFunc(req Request) error
func (ia *IDAllocator) AllocOne() (UniqueID, error)
func (ia *IDAllocator) Alloc(count uint32) (UniqueID, UniqueID, error)

func NewIDAllocator(ctx context.Context, masterAddr string) (*IDAllocator, error)
```





#### A.6 Timestamp Allocator

###### A.6.1 Timestamp

Let's take a brief review of Hybrid Logical Clock (HLC). HLC uses 64bits timestamps which are composed of a 46-bits physical component (thought of as and always close to local wall time) and a 18-bits logical component (used to distinguish between events with the same physical component).

<img src="./figs/hlc.png" width=400>

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



###### A.6.2 Timestamp Oracle

```go
type timestampOracle struct {
  key    string
  kvBase kv.TxnBase
  
  saveInterval  time.Duration
  maxResetTSGap func() time.Duration
  
  TSO           unsafe.Pointer
  lastSavedTime atomic.Value
}

func (t *timestampOracle) InitTimestamp() error
func (t *timestampOracle) ResetUserTimestamp(tso uint64) error
func (t *timestampOracle) saveTimestamp(ts time.time) error
func (t *timestampOracle) loadTimestamp() (time.time, error)
func (t *timestampOracle) UpdateTimestamp() error
func (t *timestampOracle) ResetTimestamp()
```



###### A.6.3 Timestamp Allocator

```go
type TimestampAllocator struct {
  Allocator
  
  masterAddress string
  masterConn    *grpc.ClientConn
  masterClient  masterpb.MasterServiceClient
  
  countPerRPC uint32
  lastTsBegin Timestamp
  lastTsEnd   Timestamp
  PeerID      UniqueID
}

func (ta *TimestampAllocator) Start() error
func (ta *TimestampAllocator) connectMaster() error
func (ta *TimestampAllocator) syncID() bool
func (ta *TimestampAllocator) checkSyncFunc(timeout bool) bool
func (ta *TimestampAllocator) pickCanDoFunc()
func (ta *TimestampAllocator) processFunc(req Request) error
func (ta *TimestampAllocator) AllocOne() (UniqueID, error)
func (ta *TimestampAllocator) Alloc(count uint32) (UniqueID, UniqueID, error)
func (ta *TimestampAllocator) ClearCache()

func NewTimestampAllocator(ctx context.Context, masterAddr string) (*TimestampAllocator, error)
```



* Batch Allocation of Timestamps

* Expiration of Timestamps





#### A.7 KV

###### A.7.1 KV Base

```go
type Base interface {
  Load(key string) (string, error)
  MultiLoad(keys []string) ([]string, error)
  LoadWithPrefix(key string) ([]string, []string, error)
  Save(key, value string) error
  MultiSave(kvs map[string]string) error
  Remove(key string) error
  MultiRemove(keys []string) error
  
  Close()
}
```

###### A.7.2 Txn Base

```go
type TxnBase interface {
  Base
  MultiSaveAndRemove(saves map[string]string, removals []string) error
}
```



###### A.7.3 Etcd KV

```go
type EtcdKV struct {
	client   *clientv3.Client
	rootPath string
}

func (kv *EtcdKV) Close()
func (kv *EtcdKV) GetPath(key string) string
func (kv *EtcdKV) LoadWithPrefix(key string) ([]string, []string, error)
func (kv *EtcdKV) Load(key string) (string, error)
func (kv *EtcdKV) GetCount(key string) (int64, error)
func (kv *EtcdKV) MultiLoad(keys []string) ([]string, error)
func (kv *EtcdKV) Save(key, value string) error
func (kv *EtcdKV) MultiSave(kvs map[string]string) error
func (kv *EtcdKV) RemoveWithPrefix(prefix string) error
func (kv *EtcdKV) Remove(key string) error
func (kv *EtcdKV) MultiRemove(keys []string) error
func (kv *EtcdKV) MultiSaveAndRemove(saves map[string]string, removals []string) error
func (kv *EtcdKV) Watch(key string) clientv3.WatchChan
func (kv *EtcdKV) WatchWithPrefix(key string) clientv3.WatchChan

func NewEtcdKV(etcdAddr string, rootPath string) *EtcdKV
```

EtcdKV implements all *TxnBase* interfaces.

