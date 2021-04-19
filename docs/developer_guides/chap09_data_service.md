

## 8. Data Service



#### 8.1 Overview

<img src="./figs/data_service.png" width=700>

#### 8.2 Data Service Interface

```go
type DataService interface {
	Component
	TimeTickProvider

	RegisterNode(ctx context.Context, req *datapb.RegisterNodeRequest) (*datapb.RegisterNodeResponse, error)
	Flush(ctx context.Context, req *datapb.FlushRequest) (*commonpb.Status, error)

	AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error)
	ShowSegments(ctx context.Context, req *datapb.ShowSegmentsRequest) (*datapb.ShowSegmentsResponse, error)
	GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error)
	GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error)
	GetSegmentInfoChannel(ctx context.Context) (*milvuspb.StringResponse, error)
	GetInsertChannels(ctx context.Context, req *datapb.GetInsertChannelsRequest) (*internalpb.StringList, error)
	GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error)
	GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error)
	GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error)
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
	InitParams *internalpb.InitParams
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
type SegmentIDRequest struct {
	Count         uint32
	ChannelName   string
	CollectionID  UniqueID
	PartitionID   UniqueID
}

type AssignSegmentIDRequest struct {
	NodeID        int64
	PeerRole      string
	SegIDRequests []*SegmentIDRequest
}

type SegIDAssignment struct {
	SegID         UniqueID
	ChannelName   string
	Count         uint32
	CollectionID  UniqueID
	PartitionID   UniqueID
	ExpireTime    uint64
	Status        *commonpb.Status
}

type AssignSegmentIDResponse struct {
	SegIDAssignments []*SegmentIDAssignment
	Status           *commonpb.Status
}
```

* *ShowSegments*

```go
type ShowSegmentsRequest struct {
	Base         *commonpb.MsgBase
	CollectionID UniqueID
	PartitionID  UniqueID
	DbID         UniqueID
}

type ShowSegmentsResponse struct {
	SegmentIDs []UniqueID
	Status     *commonpb.Status
}
```



* *GetSegmentStates*

```go
type GetSegmentStatesRequest struct {
	Base      *commonpb.MsgBase
	SegmentID UniqueID
}

type SegmentState int32

const (
	SegmentState_SegmentStateNone SegmentState = 0
	SegmentState_NotExist         SegmentState = 1
	SegmentState_Growing          SegmentState = 2
	SegmentState_Sealed           SegmentState = 3
	SegmentState_Flushed          SegmentState = 4
)

type SegmentStateInfo struct {
	SegmentID     UniqueID
	State         commonpb.SegmentState
	CreateTime    uint64
	SealedTime    uint64
	FlushedTime   uint64
	StartPosition *internalpb.MsgPosition
	EndPosition   *internalpb.MsgPosition
	Status        *commonpb.Status
}

type GetSegmentStatesResponse struct {
	Status *commonpb.Status
	States []*SegmentStateInfo
}
```

* *GetInsertBinlogPaths*

```go
type GetInsertBinlogPathsRequest struct {
	Base      *commonpb.MsgBase
	SegmentID UniqueID
}

type GetInsertBinlogPathsResponse struct {
	FieldIDs []int64
	Paths    []*internalpb.StringList
	Status   *commonpb.Status
}
```

* *GetInsertChannels*

```go
type GetInsertChannelsRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
}
```

* *GetCollectionStatistics*

```go
type GetCollectionStatisticsRequest struct {
	Base         *commonpb.MsgBase
	DbID         int64
	CollectionID int64
}

type GetCollectionStatisticsResponse struct {
	Stats  []*commonpb.KeyValuePair
	Status *commonpb.Status
}
```

* *GetPartitionStatistics*

```go
type GetPartitionStatisticsRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	PartitionID  UniqueID
}

type GetPartitionStatisticsResponse struct {
	Stats  []*commonpb.KeyValuePair
	Status *commonpb.Status
}
```

* *GetSegmentInfo*

```go
type GetSegmentInfoRequest  struct{
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
	StartPosition []*internalpb.MsgPosition
	EndPosition   []*internalpb.MsgPosition
}

type GetSegmentInfoResponse  struct{
	Status *commonpb.Status
	infos  []SegmentInfo
}
```





#### 8.2 Insert Channel

* *InsertMsg*

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

type InsertMsg struct {
	BaseMsg
	InsertRequest
}
```



#### 8.2 Data Node Interface

```go
type DataNode interface {
	Component

	WatchDmChannels(ctx context.Context, req *datapb.WatchDmChannelsRequest) (*commonpb.Status, error)
	FlushSegments(ctx context.Context, req *datapb.FlushSegmentsRequest) (*commonpb.Status, error)
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
type FlushSegmentsRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	SegmentIDs   []int64
}
```


#### 8.2 SegmentStatistics Update Channel

* *SegmentStatisticsMsg*

```go
type SegmentStatisticsUpdates struct {
	SegmentID     UniqueID
	MemorySize    int64
	NumRows       int64
	CreateTime    uint64
	EndTime       uint64
	StartPosition *internalpb.MsgPosition
	EndPosition   *internalpb.MsgPosition
}

type SegmentStatistics struct {
	Base                 *commonpb.MsgBase
	SegStats             []*SegmentStatisticsUpdates
}

type SegmentStatisticsMsg struct {
	BaseMsg
	SegmentStatistics
}
```

