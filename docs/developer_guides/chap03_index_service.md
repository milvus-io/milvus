

## 3. Index Service



#### 3.1 Overview

<img src="./figs/index_coord.png" width=700>

#### 3.2 Index Service Interface

```go
type IndexCoord interface {
	Component
	TimeTickProvider

	BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error)
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
	GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error)
	GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error)
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}
```



* *RegisterNode*

```go
type MsgBase struct {
	MsgType   MsgType
	MsgID     UniqueID
	Timestamp uint64
	SourceID  UniqueID
}

type Address struct {
	Ip   string
	Port int64
}

type RegisterNodeRequest struct {
	Base    *commonpb.MsgBase
	Address *commonpb.Address
}

type InitParams struct {
	NodeID      UniqueID
	StartParams []*commonpb.KeyValuePair
}

type RegisterNodeResponse struct {
	InitParams *internalpb.InitParams
	Status     *commonpb.Status
}
```

* *BuildIndex*

```go
type KeyValuePair struct {
	Key   string
	Value string
}

type BuildIndexRequest struct {
	IndexBuildID UniqueID
	IndexName    string
	IndexID      UniqueID
	DataPaths    []string
	TypeParams   []*commonpb.KeyValuePair
	IndexParams  []*commonpb.KeyValuePair
}

type BuildIndexResponse struct {
	Status       *commonpb.Status
	IndexBuildID UniqueID
}
```

* *DropIndex*

```go
type DropIndexRequest struct {
	IndexID      UniqueID
}
```

* *GetIndexStates*

```go
type GetIndexStatesRequest struct {
	IndexBuildIDs []UniqueID
}

const (
	IndexState_IndexStateNone IndexState = 0
	IndexState_Unissued       IndexState = 1
	IndexState_InProgress     IndexState = 2
	IndexState_Finished       IndexState = 3
	IndexState_Failed         IndexState = 4
	IndexState_Deleted        IndexState = 5
)

type IndexInfo struct {
	State        commonpb.IndexState
	IndexBuildID UniqueID
	IndexID      UniqueID
	IndexName    string
	Reason       string
}

type GetIndexStatesResponse struct {
	Status *commonpb.Status
	States []*IndexInfo
}
```

* *GetIndexFilePaths*

```go
type GetIndexFilePathsRequest struct {
	IndexBuildIDs []UniqueID
}

type IndexFilePathInfo struct {
	Status         *commonpb.Status
	IndexBuildID   UniqueID
	IndexFilePaths []string
}

type GetIndexFilePathsResponse struct {
	Status    *commonpb.Status
	FilePaths []*IndexFilePathInfo
}

```

* *NotifyBuildIndex*

```go
type NotifyBuildIndexRequest struct {
	Status         *commonpb.Status
	IndexBuildID   UniqueID
	IndexFilePaths []string
	NodeID         UniqueID
}
```



#### 3.3 Index Node Interface

```go
type IndexNode interface {
	Component
	TimeTickProvider

	BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*commonpb.Status, error)
	DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error)
}
```

* *BuildIndex*

```go

type KeyValuePair struct {
	Key   string
	Value string
}

type BuildIndexRequest struct {
	IndexBuildID UniqueID
	IndexName    string
	IndexID      UniqueID
	DataPaths    []string
	TypeParams   []*commonpb.KeyValuePair
	IndexParams  []*commonpb.KeyValuePair
}
```

* *DropIndex*

```go
type DropIndexRequest struct {
	IndexID UniqueID
}
```
