

## 8. Data Service



#### 8.1 Overview

<img src="./figs/data_service.png" width=700>

#### 8.2 Data Service Interface

```go
type DataService interface {
  Service
  
  RegisterNode(req RegisterNodeRequest) (RegisterNodeResponse, error)
  Flush(req FlushRequest) error
  
  AssignSegmentID(req AssignSegIDRequest) (AssignSegIDResponse, error)
  ShowSegments(req ShowSegmentRequest) (ShowSegmentResponse, error)
  GetSegmentStates(req SegmentStatesRequest) (SegmentStatesResponse, error)
  GetInsertBinlogPaths(req InsertBinlogPathRequest) (InsertBinlogPathsResponse, error)
  
  GetInsertChannels(req InsertChannelRequest) ([]string, error)
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

* *AssignSegmentID*

```go
type SegIDRequest struct {
  Count uint32
  ChannelName string
	CollectionID UniqueID
  PartitionID UniqueID
}

type AssignSegIDRequest struct {
  MsgBase
  PerChannelRequest []SegIDRequest
}

type SegIDAssignment struct {
  SegmentID UniqueID
  ChannelName string
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
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
}
```



* *ShowSegments*

```go
type ShowSegmentRequest struct {
  MsgBase
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
  MsgBase
  SegmentID UniqueID
}

type SegmentStatesResponse struct {
  State SegmentState
  OpenTime Timestamp
  SealedTime Timestamp
  MsgStartPositions []msgstream.MsgPosition
  MsgEndPositions []msgstream.MsgPosition
}
```



* *GetInsertBinlogPaths*

```go
type InsertBinlogPathRequest struct {
  MsgBase
  SegmentID UniqueID
}

type InsertBinlogPathsResponse struct {
  FieldIDToPaths map[int64][]string
}
```



* *GetInsertChannels*

```go
type InsertChannelRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
}
```



#### 8.2 Insert Channel

```go
type InsertRequest struct {
  MsgBase
  DbName string
  CollectionName string
  PartitionName string
  DbID UniqueID
  CollectionID UniqueID
  PartitionID UniqueID
  RowData []Blob
  HashKeys []uint32
}
```



#### 8.2 Data Node Interface

```go
type DataNode interface {
  Service
  
  WatchDmChannels(req WatchDmChannelRequest) error
  //WatchDdChannel(channelName string) error
  //SetTimeTickChannel(channelName string) error
  //SetStatisticsChannel(channelName string) error
  
  FlushSegments(req FlushSegRequest) error
}
```



* *WatchDmChannels*

```go
type WatchDmChannelRequest struct {
  MsgBase
  InsertChannelNames []string
}
```

* *FlushSegments*

```go
type FlushSegRequest struct {
  MsgBase
  DbID UniqueID
  CollectionID UniqueID
  SegmentID []UniqueID
}
```

* *SegmentStatistics*

```go
type SegmentStatisticsUpdates struct {
    SegmentID UniqueID
    MemorySize int64
    NumRows int64
}

type SegmentStatistics struct{
    MsgBase
    SegStats []*SegmentStatisticsUpdates
}
```

