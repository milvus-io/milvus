

## 8. Query Service

#### 8.1 Overview

<img src="./figs/query_service.jpeg" width=700>



#### 8.2 Query Service Interface

```go
type QueryService interface {
  Service
  
  RegisterNode(req RegisterNodeRequest) (RegisterNodeResponse, error)
  
  ShowCollections(req ShowCollectionRequest) (ShowCollectionResponse, error)
  LoadCollection(req LoadCollectionRequest) error
  ReleaseCollection(req ReleaseCollectionRequest) error
  
  ShowPartitions(req ShowPartitionRequest) (ShowPartitionResponse, error)
  GetPartitionStates(req PartitionStatesRequest) (PartitionStatesResponse, error)
  LoadPartitions(req LoadPartitonRequest) error
  ReleasePartitions(req ReleasePartitionRequest) error
  
  CreateQueryChannel() (CreateQueryChannelResponse, error)
}
```



* *MsgBase*

```go
type MsgBase struct {
  MsgType MsgType
  MsgID	UniqueID
  Timestamp Timestamp
  SourceID UniqueID
}
```

* *RegisterNode*

```go
type RegisterNodeRequest struct {
  MsgBase
  Address string
  Port int64
}

type RegisterNodeResponse struct {
  //InitParams
}
```

* *ShowCollections*

```go
type ShowCollectionRequest struct {
  MsgBase
  DbID UniqueID
}

type ShowCollectionResponse struct {
  CollectionIDs []UniqueID
}
```

* *LoadCollection*

```go
type LoadCollectionRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
}
```

* *ReleaseCollection*

```go
type ReleaseCollectionRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
}
```

* *ShowPartitions*

```go
type ShowPartitionRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
}

type ShowPartitionResponse struct {
  PartitionIDs []UniqueID
}
```

* *GetPartitionStates*

```go
type PartitionState = int

const (
  NOT_EXIST PartitionState = 0
  NOT_PRESENT PartitionState = 1
  ON_DISK PartitionState = 2
  PARTIAL_IN_MEMORY PartitionState = 3
	IN_MEMORY PartitionState = 4
  PARTIAL_IN_GPU PartitionState = 5
  IN_GPU PartitionState = 6
)

type PartitionStatesRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}

type PartitionStates struct {
  PartitionID UniqueID
  State PartitionState
}

type PartitionStatesResponse struct {
  States []PartitionStates
}
```

* *LoadPartitions*

```go
type LoadPartitonRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}
```

* *ReleasePartitions*

```go
type ReleasePartitionRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}
```

* *CreateQueryChannel*

```go
type CreateQueryChannelResponse struct {
  RequestChannelName string
  ResultChannelName string
}
```



#### 8.2 Query Channel

```go
type SearchRequest struct {
  RequestBase
  DbName string
  CollectionName string
  PartitionNames []string
  DbID UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
  Dsl string
  PlaceholderGroup []byte
}
```



#### 8.2 Query Node Interface

```go
type QueryNode interface {
  Service
  
  AddQueryChannel(req AddQueryChannelRequest) error
  RemoveQueryChannel(req RemoveQueryChannelRequest) error
  WatchDmChannels(req WatchDmChannelRequest) error
  //SetTimeTickChannel(channelName string) error
  //SetStatsChannel(channelName string) error
  
  LoadSegments(req LoadSegmentRequest) error
  ReleaseSegments(req ReleaseSegmentRequest) error
  //DescribeParition(req DescribeParitionRequest) (PartitionDescriptions, error)
}
```



* *AddQueryChannel*

```go
type AddQueryChannelRequest struct {
  MsgBase
  RequestChannelName string
  ResultChannelName string
}
```

* *RemoveQueryChannel*

```go
type RemoveQueryChannelRequest struct {
  RequestChannelName string
  ResultChannelName string
}
```

* *WatchDmChannels*

```go
type WatchDmChannelRequest struct {
  InsertChannelNames []string
}
```

* *LoadSegments*

```go
type LoadSegmentRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
  PartitionID UniqueID
  SegmentIDs []UniqueID
  FieldIDs []int64
}
```

* *ReleaseSegments*

```go
type ReleaseSegmentRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
  PartitionID UniqueID
  SegmentIDs []UniqueID
}
```



#### 8.2 Collection Replica

$collectionReplica$ contains a in-memory local copy of persistent collections. In common cases, the system has multiple query nodes. Data of a collection will be distributed across all the available query nodes, and each query node's $collectionReplica$ will maintain its own share (only part of the collection).
Every replica tracks a value called tSafe which is the maximum timestamp that the replica is up-to-date.

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



#### 8.3 Data Sync Service

```go
type dataSyncService struct {
	ctx context.Context
	pulsarURL string
	fg *flowgraph.TimeTickedFlowGraph
	msgStream *msgstream.PulsarMsgStream
	dataReplica Replica
}
```



