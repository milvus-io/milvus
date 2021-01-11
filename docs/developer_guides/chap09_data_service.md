

## 8. Data Service



#### 8.1 Overview

<img src="./figs/data_service.jpeg" width=700>

#### 8.2 Data Service API

```go
type Client interface {
  RegisterNode(req NodeInfo) (InitParams, error)
  AssignSegmentID(req AssignSegIDRequest) (AssignSegIDResponse, error)
  Flush(req FlushRequest) error
  ShowSegments(req ShowSegmentRequest) (ShowSegmentResponse, error)
  GetSegmentStates(req SegmentStatesRequest) (SegmentStatesResponse, error)
  GetInsertBinlogPaths(req InsertBinlogPathRequest) (InsertBinlogPathsResponse, error)
  
  GetInsertChannels(req InsertChannelRequest) ([]string, error)
  GetTimeTickChannel() (string, error)
  GetStatsChannel() (string, error)
}
```



* *RegisterNode*

```go
type NodeInfo struct {}

type InitParams struct {}
```

* *AssignSegmentID*

```go
type SegIDRequest struct {
  Count uint32
  ChannelID string
	CollectionID UniqueID
  PartitionID UniqueID
}

type AssignSegIDRequest struct {
  PerChannelRequest []SegIDRequest
}

type SegIDAssignment struct {
  SegID UniqueID
  ChannelID string
  Count uint32
	CollectionID UniqueID
  PartitionID UniqueID
  ExpireTime Timestamp
}

type AssignSegIDResponse struct {
  PerChannelResponse []SegIDAssignment
}
```



* *Flush*

```go
type FlushRequest struct {
  DbID UniqueID
  CollectionID UniqueID
}
```



* *ShowSegments*

```go
type ShowSegmentRequest struct {
  CollectionID UniqueID
  PartitionID UniqueID
}

type ShowSegmentResponse struct {
  SegmentIDs []UniqueID
}
```



* *GetSegmentStates*

```go
enum SegmentState {
    NONE = 0;
    NOT_EXIST = 1;
    GROWING = 2;
    SEALED = 3;
}

type SegmentStatesRequest struct {
  SegmentID UniqueID
}

type SegmentStatesResponse struct {
  State SegmentState
  CreateTime Timestamp
  SealedTime Timestamp
}
```



* *GetInsertBinlogPaths*

```go
type InsertBinlogPathRequest struct {
  SegmentID UniqueID
}

type InsertBinlogPathsResponse struct {
  FieldIDToPaths map[int64][]string
}
```



* *GetInsertChannels*

```go
type InsertChannelRequest struct {
  DbID UniqueID
  CollectionID UniqueID
}
```



#### 8.2 Data Node API

```go
type DataNode interface {
  Start() error
  Close() error
  
  WatchDmChannels(channelIDs []string) error
  WatchDdChannel(channelID string) error
  SetTimeTickChannel(channelID string) error
  SetStatsChannel(channelID string) error
  
  Flush(req FlushRequest) error
}
```

