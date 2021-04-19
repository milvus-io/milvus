

## Appendix A. Basic Components

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



#### A.3 Message Stream

``` go
type MsgType uint32
const {
  kInsert MsgType = 400
  kDelete MsgType = 401
  kSearch MsgType = 500
  KSearchResult MsgType = 1000
  
  kSegStatistics MsgType = 1100
  
  kTimeTick MsgType = 1200
  kTimeSync MsgType = 1201
}

type TsMsg interface {
  SetTs(ts Timestamp)
  BeginTs() Timestamp
  EndTs() Timestamp
  Type() MsgType
  Marshal(*TsMsg) []byte
  Unmarshal([]byte) *TsMsg
}

type MsgPack struct {
  BeginTs Timestamp
  EndTs Timestamp
  Msgs []*TsMsg
}


type MsgStream interface {
  Produce(*MsgPack) error
  Broadcast(*MsgPack) error
  Consume() *MsgPack // message can be consumed exactly once
}

type RepackFunc(msgs []* TsMsg, hashKeys [][]int32) map[int32] *MsgPack

type PulsarMsgStream struct {
  client *pulsar.Client
  repackFunc RepackFunc
  producers []*pulsar.Producer
  consumers []*pulsar.Consumer
  unmarshal *UnmarshalDispatcher
}

func (ms *PulsarMsgStream) CreatePulsarProducers(topics []string)
func (ms *PulsarMsgStream) CreatePulsarConsumers(subname string, topics []string, unmarshal *UnmarshalDispatcher)
func (ms *PulsarMsgStream) SetRepackFunc(repackFunc RepackFunc)
func (ms *PulsarMsgStream) Produce(msgs *MsgPack) error
func (ms *PulsarMsgStream) Broadcast(msgs *MsgPack) error
func (ms *PulsarMsgStream) Consume() (*MsgPack, error)
func (ms *PulsarMsgStream) Start() error
func (ms *PulsarMsgStream) Close() error

func NewPulsarMsgStream(ctx context.Context, pulsarAddr string) *PulsarMsgStream


type PulsarTtMsgStream struct {
  client *pulsar.Client
  repackFunc RepackFunc
  producers []*pulsar.Producer
  consumers []*pulsar.Consumer
  unmarshal *UnmarshalDispatcher
  inputBuf []*TsMsg
  unsolvedBuf []*TsMsg
  msgPacks []*MsgPack
}

func (ms *PulsarTtMsgStream) CreatePulsarProducers(topics []string)
func (ms *PulsarTtMsgStream) CreatePulsarConsumers(subname string, topics []string, unmarshal *UnmarshalDispatcher)
func (ms *PulsarTtMsgStream) SetRepackFunc(repackFunc RepackFunc)
func (ms *PulsarTtMsgStream) Produce(msgs *MsgPack) error
func (ms *PulsarTtMsgStream) Broadcast(msgs *MsgPack) error
func (ms *PulsarTtMsgStream) Consume() *MsgPack //return messages in one time tick
func (ms *PulsarTtMsgStream) Start() error
func (ms *PulsarTtMsgStream) Close() error

func NewPulsarTtMsgStream(ctx context.Context, pulsarAddr string) *PulsarTtMsgStream
```



```go
type MarshalFunc func(*TsMsg) []byte
type UnmarshalFunc func([]byte) *TsMsg


type UnmarshalDispatcher struct {
	tempMap map[ReqType]UnmarshalFunc 
}

func (dispatcher *MarshalDispatcher) Unmarshal([]byte) *TsMsg
func (dispatcher *MarshalDispatcher) AddMsgTemplate(msgType MsgType, marshal MarshalFunc)
func (dispatcher *MarshalDispatcher) addDefaultMsgTemplates()

func NewUnmarshalDispatcher() *UnmarshalDispatcher
```

#### A.4 Time Ticked Flow Graph

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
  SkipThisTick() bool
  DownStreamNodeIdx() int32
}
```

```go
type SkipTickMsg struct {}
func (msg *SkipTickMsg) SkipThisTick() bool // always return true
```

###### A.4.3 Node

```go
type Node interface {
  Name() string
  MaxQueueLength() int32
  MaxParallelism() int32
  SetPipelineStates(states *flowGraphStates)
  Operate([]*Msg) []*Msg
}
```



```go
type baseNode struct {
  Name string
  maxQueueLength int32
  maxParallelism int32
  graphStates *flowGraphStates
}
func (node *baseNode) MaxQueueLength() int32
func (node *baseNode) MaxParallelism() int32
func (node *baseNode) SetMaxQueueLength(n int32)
func (node *baseNode) SetMaxParallelism(n int32)
func (node *baseNode) SetPipelineStates(states *flowGraphStates)
```

###### A.4.4 Flow Graph

```go
type nodeCtx struct {
  node *Node
  inputChans [](*chan *Msg)
  outputChans [](*chan *Msg)
  inputMsgs [](*Msg List)
  downstreams []*nodeCtx
  skippedTick Timestamp
}

func (nodeCtx *nodeCtx) Start(ctx context.Context) error
```

*Start()* will enter a loop. In each iteration, it tries to collect input messges from *inputChan*, then prepare node's input. When input is ready, it will trigger *node.Operate*. When *node.Operate* returns, it sends the returned *Msg* to *outputChans*, which connects to the downstreams' *inputChans*.

```go
type TimeTickedFlowGraph struct {
  states *flowGraphStates
  nodeCtx map[string]*nodeCtx
}

func (*pipeline TimeTickedFlowGraph) AddNode(node *Node)
func (*pipeline TimeTickedFlowGraph) SetEdges(nodeName string, in []string, out []string)
func (*pipeline TimeTickedFlowGraph) Start() error
func (*pipeline TimeTickedFlowGraph) Close() error

func NewTimeTickedFlowGraph(ctx context.Context) *TimeTickedFlowGraph
```



#### A.5 ID Allocator

```go
type IdAllocator struct {
}

func (allocator *IdAllocator) Start() error
func (allocator *IdAllocator) Close() error
func (allocator *IdAllocator) Alloc(count uint32) ([]int64, error)

func NewIdAllocator(ctx context.Context) *IdAllocator
```



#### A.6 KV

###### A.6.1 KV Base

```go
type KVBase interface {
	Load(key string) (string, error)
    MultiLoad(keys []string) ([]string, error)
	Save(key, value string) error
    MultiSave(kvs map[string]string) error
	Remove(key string) error

    MultiRemove(keys []string) error
    MultiSaveAndRemove(saves map[string]string, removals []string) error

	Watch(key string) clientv3.WatchChan
	WatchWithPrefix(key string) clientv3.WatchChan
	LoadWithPrefix(key string) ( []string, []string, error)
}
```

* *MultiLoad(keys []string)* Load multiple kv pairs. Loads are done transactional.
* *MultiSave(kvs map[string]string)* Save multiple kv pairs. Saves are done transactional.
* *MultiRemove(keys []string)* Remove multiple kv pairs. Removals are done transactional.



###### A.6.2 Etcd KV

```go
type EtcdKV struct {
	client   *clientv3.Client
	rootPath string
}

func NewEtcdKV(etcdAddr string, rootPath string) *EtcdKV
```

EtcdKV implements all *KVBase* interfaces.

