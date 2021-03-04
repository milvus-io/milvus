

## 8. Query Service

#### 8.1 Overview

<img src="./figs/query_service.png" width=500>



#### 8.2 Query Service Interface

```go
type QueryService interface {
  Service
  Component
  
  RegisterNode(ctx context.Context, req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error)
  ShowCollections(ctx context.Context, req *querypb.ShowCollectionRequest) (*querypb.ShowCollectionResponse, error)
  LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error)
  ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
  ShowPartitions(ctx context.Context, req *querypb.ShowPartitionRequest) (*querypb.ShowPartitionResponse, error)
  LoadPartitions(ctx context.Context, req *querypb.LoadPartitionRequest) (*commonpb.Status, error)
  ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionRequest) (*commonpb.Status, error)
  CreateQueryChannel(ctx context.Context) (*querypb.CreateQueryChannelResponse, error)
  GetPartitionStates(ctx context.Context, req *querypb.PartitionStatesRequest) (*querypb.PartitionStatesResponse, error)
  GetSegmentInfo(ctx context.Context, req *querypb.SegmentInfoRequest) (*querypb.SegmentInfoResponse, error)
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
tyoe Address struct {
  Ip   string
  port int64
}

type RegisterNodeRequest struct {
  Base    *commonpb.MsgBase
  Address *commonpb.Address
}

type RegisterNodeResponse struct {
  Status     *commonpb.Status
  InitParams *internalpb2.InitParams
}
```

* *ShowCollections*

```go
type ShowCollectionRequest struct {
  Base *commonpb.MsgBase
  DbID UniqueID
}

type ShowCollectionResponse struct {
  Status        *commonpb.Status
  CollectionIDs []UniqueID
}
```

* *LoadCollection*

```go
type LoadCollectionRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
  schema       *schemapb.CollectionSchema
}
```

* *ReleaseCollection*

```go
type ReleaseCollectionRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
}
```

* *ShowPartitions*

```go
type ShowPartitionRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
}

type ShowPartitionResponse struct {
  Status       *commonpb.Status
  PartitionIDs []UniqueID
}
```

* *GetPartitionStates*

```go
type PartitionState = int

const (
  PartitionState_NotExist        PartitionState = 0
  PartitionState_NotPresent      PartitionState = 1
  PartitionState_OnDisk          PartitionState = 2
  PartitionState_PartialInMemory PartitionState = 3
  PartitionState_InMemory        PartitionState = 4
  PartitionState_PartialInGPU    PartitionState = 5
  PartitionState_InGPU           PartitionState = 6
)

type PartitionStatesRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}

type PartitionStates struct {
  PartitionID UniqueID
  State       PartitionState
}

type PartitionStatesResponse struct {
  Status                *commonpb.Status
  PartitionDescriptions []*PartitionStates
}
```

* *LoadPartitions*

```go
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
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}
```

* *CreateQueryChannel*

```go
type CreateQueryChannelResponse struct {
  Status             *commonpb.Status
  RequestChannelName string
  ResultChannelName  string
}
```

* *GetSegmentInfo* *

```go
type SegmentInfoRequest struct {
  Base       *commonpb.MsgBase
  SegmentIDs []UniqueID
}

type SegmentInfo struct {
  SegmentID    UniqueID
  CollectionID UniqueID
  PartitionID  UniqueID
  MemSize      UniqueID
  NumRows      UniqueID
  IndexName    string
  IndexID      UniqueID
}

type SegmentInfoResponse struct {
  Status *commonpb.Status
  Infos  []*SegmentInfo
}
```

//TODO
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
  typeutil.Component
  
  AddQueryChannel(ctx context.Context, in *queryPb.AddQueryChannelsRequest) (*commonpb.Status, error)
  RemoveQueryChannel(ctx context.Context, in *queryPb.RemoveQueryChannelsRequest) (*commonpb.Status, error)
  WatchDmChannels(ctx context.Context, in *queryPb.WatchDmChannelsRequest) (*commonpb.Status, error)
  LoadSegments(ctx context.Context, in *queryPb.LoadSegmentRequest) (*commonpb.Status, error)
  ReleaseCollection(ctx context.Context, in *queryPb.ReleaseCollectionRequest) (*commonpb.Status, error)
  ReleasePartitions(ctx context.Context, in *queryPb.ReleasePartitionRequest) (*commonpb.Status, error)
  ReleaseSegments(ctx context.Context, in *queryPb.ReleaseSegmentRequest) (*commonpb.Status, error)
  GetSegmentInfo(ctx context.Context, in *queryPb.SegmentInfoRequest) (*queryPb.SegmentInfoResponse, error)
}
```



* *AddQueryChannel*

```go
type AddQueryChannelRequest struct {
  Base             *commonpb.MsgBase
  RequestChannelID string
  ResultChannelID  string
}
```

* *RemoveQueryChannel*

```go
type RemoveQueryChannelRequest struct {
  Status           *commonpb.Status
  Base             *commonpb.MsgBase
  RequestChannelID string
  ResultChannelID  string
}
```

* *WatchDmChannels*

```go
type WatchDmChannelRequest struct {
  Base       *commonpb.MsgBase
  ChannelIDs []string
}
```

* *LoadSegments*

```go
type LoadSegmentRequest struct {
  Base          *commonpb.MsgBase
  DbID          UniqueID
  CollectionID  UniqueID
  PartitionID   UniqueID
  SegmentIDs    []UniqueID
  FieldIDs      []UniqueID
  SegmentStates []*datapb.SegmentStateInfo
  Schema        *schemapb.CollectionSchema
}
```
* *ReleaseCollection*

```go
type ReleaseCollectionRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
}
```

* *ReleasePartitions*

```go
type ReleasePartitionRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
}
```

* *ReleaseSegments*

```go
type ReleaseSegmentRequest struct {
  Base         *commonpb.MsgBas
  DbID         UniqueID
  CollectionID UniqueID
  PartitionIDs []UniqueID
  SegmentIDs   []UniqueID
  FieldIDs     []UniqueID
}
```

* *GetSegmentInfo*

```go
type SegmentInfoRequest struct {
  Base       *commonpb.MsgBase
  SegmentIDs []Unique
}

type SegmentInfoResponse struct {
  Status *commonpb.Status
  Infos  []*SegmentInfo

}
```


//TODO
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



