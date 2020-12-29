

## 8. Data Service



#### 8.1 Overview



#### 8.2 API

```go
type Client interface {
  AssignSegmentID(req AssignSegIDRequest) (AssignSegIDResponse, error)
  Flush(req FlushRequest) (error)
  GetInsertBinlogPaths(req InsertBinlogPathRequest) (InsertBinlogPathsResponse, error)
  GetInsertChannels(req InsertChannelRequest) ([]string, error)
  GetTimeTickChannel() (string, error)
  GetStatsChannel() (string, error)
}
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



* *GetInsertBinlogPaths*

```go
type InsertBinlogPathRequest struct {
  segmentID UniqueID
}

type InsertBinlogPathsResponse struct {
  FieldIdxToPaths map[int32][]string
}
```



* *GetInsertChannels*

```go
type InsertChannelRequest struct {
  DbID UniqueID
  CollectionID UniqueID
}
```

