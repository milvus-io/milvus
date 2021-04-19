## 8. Message Stream





#### 8.2 Message Stream Service API

```go
type Client interface {
  CreateChannels(req CreateChannelRequest) (CreateChannelResponse, error)
  DestoryChannels(req DestoryChannelRequest) error
  DescribeChannels(req DescribeChannelRequest) (DescribeChannelResponse, error)
}
```

* *CreateChannels*

```go
type OwnerDescription struct {
  Role string
  Address string
  //Token string
  DescriptionText string
}

type CreateChannelRequest struct {
  OwnerDescription OwnerDescription
  NumChannels int
}

type CreateChannelResponse struct {
  ChannelNames []string
}
```

* *DestoryChannels*

```go
type DestoryChannelRequest struct {
	ChannelNames []string
}
```



* *DescribeChannels*

```go
type DescribeChannelRequest struct {
	ChannelNames []string
}

type ChannelDescription struct {
  ChannelName string
  Owner OwnerDescription
}

type DescribeChannelResponse struct {
  Descriptions []ChannelDescription
}
```



#### A.3 Message Stream

* Overview



<img src="./figs/msg_stream_input_output.jpeg" width=700>

* Interface

``` go
// Msg

type MsgType uint32
const {
  kInsert MsgType = 400
  kDelete MsgType = 401
  kSearch MsgType = 500
  kSearchResult MsgType = 1000
  
  kSegStatistics MsgType = 1100
  
  kTimeTick MsgType = 1200
  kTimeSync MsgType = 1201
}

type TsMsg interface {
    ID() UniqueID
    SetTs(ts Timestamp)
    BeginTs() Timestamp
    EndTs() Timestamp
    Type() MsgType
    Marshal(*TsMsg) interface{}
    Unmarshal(interface{}) *TsMsg
}

type MsgPosition {
  ChannelName string
  MsgID string
  TimestampFilter Timestamp
}

type MsgPack struct {
  BeginTs Timestamp
  EndTs Timestamp
  Msgs []TsMsg
  StartPositions []MsgPosition
  EndPositions []MsgPosition
}

type RepackFunc(msgs []* TsMsg, hashKeys [][]int32) map[int32] *MsgPack
```



```go
// Unmarshal

// Interface
type UnmarshalFunc func(interface{}) *TsMsg

type UnmarshalDispatcher interface {
    Unmarshal(interface{}, msgType commonpb.MsgType) (msgstream.TsMsg, error)
}

type UnmarshalDispatcherFactory interface {
    NewUnmarshalDispatcher() *UnmarshalDispatcher
}

// Proto & Mem Implementation
type ProtoUDFactory struct {}
func (pudf *ProtoUDFactory) NewUnmarshalDispatcher() *UnmarshalDispatcher

type MemUDFactory struct {}
func (mudf *MemUDFactory) NewUnmarshalDispatcher() *UnmarshalDispatcher
```




```go
// MsgStream

// Interface
type MsgStream interface {
    Start()
    Close()
    AsProducer(channels []string)
    AsConsumer(channels []string, subName string)
    Produce(*MsgPack) error
    Broadcast(*MsgPack) error
    Consume() *MsgPack // message can be consumed exactly once
    Seek(mp *MsgPosition) error
}

type MsgStreamFactory interface {
    NewMsgStream() *MsgStream
    NewTtMsgStream() *MsgStream
}

// Pulsar
type PulsarMsgStreamFactory interface {}
func (pmsf *PulsarMsgStreamFactory) NewMsgStream() *MsgStream
func (pmsf *PulsarMsgStreamFactory) NewTtMsgStream() *MsgStream

// RockMQ
type RmqMsgStreamFactory interface {}
func (rmsf *RmqMsgStreamFactory) NewMsgStream() *MsgStream
func (rmsf *RmqMsgStreamFactory) NewTtMsgStream() *MsgStream
```



```go
// PulsarMsgStream

type PulsarMsgStream struct {
  client *pulsar.Client
  repackFunc RepackFunc
  producers []*pulsar.Producer
  consumers []*pulsar.Consumer
  unmarshal *UnmarshalDispatcher
}

func (ms *PulsarMsgStream) Start() error
func (ms *PulsarMsgStream) Close() error
func (ms *PulsarMsgStream) AsProducer(channels []string)
func (ms *PulsarMsgStream) AsConsumer(channels []string, subName string)
func (ms *PulsarMsgStream) Produce(msgs *MsgPack) error
func (ms *PulsarMsgStream) Broadcast(msgs *MsgPack) error
func (ms *PulsarMsgStream) Consume() (*MsgPack, error)
func (ms *PulsarMsgStream) Seek(mp *MsgPosition) error
func (ms *PulsarMsgStream) SetRepackFunc(repackFunc RepackFunc)

func NewPulsarMsgStream(ctx context.Context, pulsarAddr string, bufferSize int64) *PulsarMsgStream


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

func (ms *PulsarTtMsgStream) Start() error
func (ms *PulsarTtMsgStream) Close() error
func (ms *PulsarTtMsgStream) AsProducer(channels []string)
func (ms *PulsarTtMsgStream) AsConsumer(channels []string, subName string)
func (ms *PulsarTtMsgStream) Produce(msgs *MsgPack) error
func (ms *PulsarTtMsgStream) Broadcast(msgs *MsgPack) error
func (ms *PulsarTtMsgStream) Consume() *MsgPack //return messages in one time tick
func (ms *PulsarTtMsgStream) Seek(mp *MsgPosition) error
func (ms *PulsarTtMsgStream) SetRepackFunc(repackFunc RepackFunc)

func NewPulsarTtMsgStream(ctx context.Context, pulsarAddr string, bufferSize int64) *PulsarTtMsgStream

// RmqMsgStream

type RmqMsgStream struct {
    client *rockermq.RocksMQ
    repackFunc RepackFunc
    producers []string
    consumers []string
    subName string
    unmarshal *UnmarshalDispatcher
}

func (ms *RmqMsgStream) Start() error
func (ms *RmqMsgStream) Close() error
func (ms *RmqMsgStream) AsProducer(channels []string)
func (ms *RmqMsgStream) AsConsumer(channels []string, subName string)
func (ms *RmqMsgStream) Produce(msgs *MsgPack) error
func (ms *RmqMsgStream) Broadcast(msgs *MsgPack) error
func (ms *RmqMsgStream) Consume() (*MsgPack, error)
func (ms *RmqMsgStream) Seek(mp *MsgPosition) error
func (ms *RmqMsgStream) SetRepackFunc(repackFunc RepackFunc)

func NewRmqMsgStream(ctx context.Context) *RmqMsgStream

type RmqTtMsgStream struct {
    client *rockermq.RocksMQ
    repackFunc RepackFunc
    producers []string
    consumers []string
    subName string
    unmarshal *UnmarshalDispatcher
}

func (ms *RmqTtMsgStream) Start() error
func (ms *RmqTtMsgStream) Close() error
func (ms *RmqTtMsgStream) AsProducer(channels []string)
func (ms *RmqTtMsgStream) AsConsumer(channels []string, subName string)
func (ms *RmqTtMsgStream) Produce(msgs *MsgPack) error
func (ms *RmqTtMsgStream) Broadcast(msgs *MsgPack) error
func (ms *RmqTtMsgStream) Consume() (*MsgPack, error)
func (ms *RmqTtMsgStream) Seek(mp *MsgPosition) error
func (ms *RmqTtMsgStream) SetRepackFunc(repackFunc RepackFunc)

func NewRmqTtMsgStream(ctx context.Context) *RmqTtMsgStream
```





#### A.4 RocksMQ

RocksMQ is a RocksDB-based messaging/streaming library.

```GO
// All the following UniqueIDs are 64-bit integer, which is combined with timestamp and increasing number

type ProducerMessage struct {
  payload []byte
} 

type ConsumerMessage struct {
  msgID UniqueID
  payload []byte
} 

type IDAllocator interface {
	Alloc(count uint32) (UniqueID, UniqueID, error)
	AllocOne() (UniqueID, error)
	UpdateID() error
}

// Every collection has its RocksMQ
type RocksMQ struct {
    store       *gorocksdb.DB
	kv          kv.Base
	idAllocator IDAllocator
	produceMu   sync.Mutex
	consumeMu   sync.Mutex
}

func (rmq *RocksMQ) CreateChannel(channelName string) error
func (rmq *RocksMQ) DestroyChannel(channelName string) error
func (rmq *RocksMQ) CreateConsumerGroup(groupName string) error
func (rmq *RocksMQ) DestroyConsumerGroup(groupName string) error
func (rmq *RocksMQ) Produce(channelName string, messages []ProducerMessage) error
func (rmq *RocksMQ) Consume(groupName string, channelName string, n int) ([]ConsumerMessage, error)
func (rmq *RocksMQ) Seek(groupName string, channelName string, msgID MessageID) error

func NewRocksMQ(name string, idAllocator IDAllocator) (*RocksMQ, error)
```



##### A.4.1 Meta (stored in Etcd)

```go
// channel meta
"$(channel_name)/begin_id", UniqueID
"$(channel_name)/end_id", UniqueID

// consumer group meta
"$(group_name)/$(channel_name)/current_id", UniqueID
```



##### A.4.2 Data (stored in RocksDB)

- data

```go
"$(channel_name)/$(unique_id)", []byte
```