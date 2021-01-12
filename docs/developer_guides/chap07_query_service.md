

## 8. Query Service

#### 8.1 Overview

<img src="./figs/query_service.jpeg" width=700>



#### 8.2 Query Service API

```go
type Client interface {
  RegisterNode(req NodeInfo) (InitParams, error)
  GetServiceStates() (ServiceStatesResponse, error)
  ShowCollections(req ShowCollectionRequest) (ShowCollectionResponse, error)
  ShowPartitions(req ShowPartitionRequest) (ShowPartitionResponse, error)
  GetPartitionStates(req PartitionStatesRequest) (PartitionStatesResponse, error)
  LoadPartitions(req LoadPartitonRequest) error
  ReleasePartitions(req ReleasePartitionRequest) error
  CreateQueryChannel() (QueryChannels, error)
  GetTimeTickChannel() (string, error)
  GetStatsChannel() (string, error)
}
```



* *RegisterNode*

```go
type NodeInfo struct {}

type InitParams struct {}
```

* *GetServiceStates*

```go
type NodeState = int

const (
  INITIALIZING NodeState = 0
  HEALTHY NodeState = 1
  ABNORMAL NodeState = 2
)

//type ResourceCost struct {
//  MemUsage int64
//  CpuUsage float32
//}

type QueryNodeStates struct {
  NodeState NodeState
  //ResourceCost ResourceCost 
}

type ServiceStatesResponse struct {
  ServiceState NodeState
}
```

* *ShowCollections*

```go
type ShowCollectionRequest struct {
  DbID UniqueID
}

type ShowCollectionResponse struct {
  CollectionIDs []UniqueID
}
```

* *ShowPartitions*

```go
type ShowPartitionRequest struct {
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
	DbID UniqueID
  CollectionID UniqueID
  partitionIDs []UniqueID
}

type PartitionStates struct {
  PartitionID UniqueID
  State PartitionState
}

type PartitionStatesResponse struct {
  States []PartitionStates
}
```



* *CreateQueryChannel*

```go
type QueryChannels struct {
  RequestChannel string
  ResultChannel string
}
```



* *LoadPartitions*

```go
type LoadPartitonRequest struct {
  DbID UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}
```



* *ReleasePartitions*

```go
type ReleasePartitionRequest struct {
  DbID UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}
```



* *LoadSegments*

```go
type LoadSegmentRequest struct {
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
  DbID UniqueID
  CollectionID UniqueID
  PartitionID UniqueID
  SegmentIDs []UniqueID
}
```



#### 8.2 Query Node

```go
type QueryNode interface {
  Start() error
  Close() error
  
  AddQueryChannel(channelIDs QueryChannels) error
  RemoveQueryChannel(channelIDs QueryChannels) error
  WatchDmChannels(insertChannelIDs []string) error
  //SetTimeTickChannel(channelID string) error
  //SetStatsChannel(channelID string) error
  
  LoadSegments(req LoadSegmentRequest) error
  ReleaseSegments(req ReleaseSegmentRequest) error
  DescribeParition(req DescribeParitionRequest) (PartitionDescriptions, error)
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



#### 8.3 Data Manipulation Service



```go
type manipulationService struct {
	ctx context.Context
	pulsarURL string
	fg *flowgraph.TimeTickedFlowGraph
	msgStream *msgstream.PulsarMsgStream
	node *QueryNode
}
```



