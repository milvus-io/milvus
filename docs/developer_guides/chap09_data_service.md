

## 8. Data Service



#### 8.1 Overview

<img src="./figs/data_service.png" width=700>

#### 8.2 Data Service Interface

```go
type DataService interface {
  typeutil.Service
  typeutil.Component
  RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error)
  Flush(ctx context.Context, req *datapb.FlushRequest) (*commonpb.Status, error)
  
  AssignSegmentID(ctx context.Context, req *datapb.AssignSegIDRequest) (*datapb.AssignSegIDResponse, error)
  ShowSegments(ctx context.Context, req *datapb.ShowSegmentRequest) (*datapb.ShowSegmentResponse, error)
  GetSegmentStates(ctx context.Context, req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error)
  GetInsertBinlogPaths(ctx context.Context, req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error)
  GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error)
  GetInsertChannels(ctx context.Context, req *datapb.InsertChannelRequest) (*internalpb2.StringList, error)
  GetCollectionStatistics(ctx context.Context, req *datapb.CollectionStatsRequest) (*datapb.CollectionStatsResponse, error)
  GetPartitionStatistics(ctx context.Context, req *datapb.PartitionStatsRequest) (*datapb.PartitionStatsResponse, error)
  GetCount(ctx context.Context, req *datapb.CollectionCountRequest) (*datapb.CollectionCountResponse, error)
  GetSegmentInfo(ctx context.Context, req *datapb.SegmentInfoRequest) (*datapb.SegmentInfoResponse, error)
}
```



* *MsgBase*

```go
type MsgBase struct {
  MsgType   MsgType
  MsgID	    UniqueID
  Timestamp Timestamp
  SourceID  UniqueID
}
```

* *RegisterNode*

```go
type RegisterNodeRequest struct {
  Base    *commonpb.MsgBase
  Address *commonpb.Address
}

type RegisterNodeResponse struct {
  InitParams *internalpb2.InitParams
  Status     *commonpb.Status
}
```

* *Flush*

```go
type FlushRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
}
```

* *AssignSegmentID*

```go
type SegIDRequest struct {
  Count         uint32
  ChannelName   string
  CollectionID  UniqueID
  PartitionID   UniqueID
  CollName      string
  PartitionName string
}

type AssignSegIDRequest struct {
  NodeID        int64
  PeerRole      string
  SegIDRequests []*SegIDRequest
}

type SegIDAssignment struct {
  SegID         UniqueID
  ChannelName   string
  Count         uint32
  CollectionID  UniqueID
  PartitionID   UniqueID
  ExpireTime    uint64
  Status        *commonpb.Status
  CollName      string
  PartitionName string
}

type AssignSegIDResponse struct {
  SegIDAssignments []*SegIDAssignment
  Status           *commonpb.Status
}
```

* *ShowSegments*

```go
type ShowSegmentRequest struct {
  Base         *commonpb.MsgBase
  CollectionID UniqueID
  PartitionID  UniqueID
}

type ShowSegmentResponse struct {
  SegmentIDs []UniqueID
  Status     *commonpb.Status
}
```



* *GetSegmentStates*

```go
type SegmentStatesRequest struct {
  Base      *commonpb.MsgBase
  SegmentID UniqueID
}

enum SegmentState {
  NONE      = 0;
  NOT_EXIST = 1;
  GROWING   = 2;
  SEALED    = 3;
}

type SegmentStateInfo struct {
  SegmentID     UniqueID 
  State         commonpb.SegmentState
  CreateTime    uint64
  SealedTime    uint64
  FlushedTime   uint64
  StartPosition *internalpb2.MsgPosition
  EndPosition   *internalpb2.MsgPosition
  Status        *commonpb.Status
}

type SegmentStatesResponse struct {
  Status               *commonpb.Status    `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
  States               []*SegmentStateInfo `protobuf:"bytes,2,rep,name=states,proto3" json:"states,omitempty"`
}
```

* *GetInsertBinlogPaths*

```go
type InsertBinlogPathRequest struct {
  Base      *commonpb.MsgBase
  SegmentID UniqueID
}

type InsertBinlogPathsResponse struct {
  FieldIDs             []int64
  Paths                []*internalpb2.StringList
  Status               *commonpb.Status
}
```

* *GetInsertChannels*

```go
type InsertChannelRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
}
```

* *GetCollectionStatistics*

```go
type CollectionStatsRequest struct {
  Base         *commonpb.MsgBase
  DbID         int64
  CollectionID int64
}

type CollectionStatsResponse struct {
  Stats  []*commonpb.KeyValuePair
  Status *commonpb.Status
}
```

* *GetPartitionStatistics*

```go
type PartitionStatsRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID 
  CollectionID UniqueID
  PartitionID  UniqueID
}

type PartitionStatsResponse struct {
  Stats  []*commonpb.KeyValuePair
  Status *commonpb.Status
}
```

* *GetCount*

```go
type CollectionCountRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
}

type CollectionCountResponse struct {
  Status *commonpb.Status
  Count  int64
}
```

* *GetSegmentInfo*

```go
type SegmentInfoRequest  struct{
  Base       *commonpb.MsgBase
  SegmentIDs []UniqueID
}

type SegmentInfo struct {
  SegmentID     UniqueID
  CollectionID  UniqueID
  PartitionID   UniqueID
  InsertChannel string
  OpenTime      Timestamp
  SealedTime    Timestamp
  FlushedTime   Timestamp
  NumRows       int64
  MemSize       int64
  State         SegmentState
  StartPosition []*internalpb2.MsgPosition
  EndPosition   []*internalpb2.MsgPosition
}

type SegmentInfoResponse  struct{
  Status *commonpb.Status
  infos  []SegmentInfo
}
```





#### 8.2 Insert Channel

```go
type InsertRequest struct {
  Base           *commonpb.MsgBase
  DbName         string
  CollectionName string
  PartitionName  string
  DbID           UniqueID
  CollectionID   UniqueID
  PartitionID    UniqueID
  SegmentID      UniqueID
  ChannelID      string
  Timestamps     []uint64
  RowIDs         []int64
  RowData        []*commonpb.Blob
}
```



#### 8.2 Data Node Interface

```go
type DataNode interface {
  Service
  Component
  
  WatchDmChannels(ctx context.Context, in *datapb.WatchDmChannelRequest) (*commonpb.Status, error)
  FlushSegments(ctx context.Context, in *datapb.FlushSegRequest) error
  
  SetMasterServiceInterface(ctx context.Context, ms MasterServiceInterface) error
  SetDataServiceInterface(ctx context.Context, ds DataServiceInterface) error
}
```

* *WatchDmChannels*

```go
type WatchDmChannelRequest struct {
  Base         *commonpb.MsgBase
  ChannelNames []string
}
```

* *FlushSegments*

```go
type FlushSegRequest struct {
  Base         *commonpb.MsgBase
  DbID         UniqueID
  CollectionID UniqueID
  SegmentIDs   []int64
}
```


#### 8.2 SegmentStatistics Update Channel

* *SegmentStatistics*

```go
type SegmentStatisticsUpdates struct {
  SegmentID     UniqueID
  MemorySize    int64
  NumRows       int64
  CreateTime    uint64
  EndTime       uint64
  StartPosition *internalpb2.MsgPosition
  EndPosition   *internalpb2.MsgPosition
  IsNewSegment  bool
}

type SegmentStatistics struct{
    Base     *commonpb.MsgBase
    SegStats []*SegmentStatisticsUpdates
}
```

