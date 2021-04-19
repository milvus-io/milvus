

## 8. Message Stream Service



#### 8.1 Overview



#### 8.2 API

```go
type Client interface {
  CreateChannels(req CreateChannelRequest) (ChannelID []string, error)
  DestoryChannels(channelID []string) error
  DescribeChannels(channelID []string) (ChannelDescriptions, error)
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
  numChannels int
}
```



* *DescribeChannels*

```go
type ChannelDescription struct {
  Owner OwnerDescription
}

type ChannelDescriptions struct {
  Descriptions []ChannelDescription
}
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
  Msgs []TsMsg
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


