

## 8. Index Service



#### 8.1 Overview

<img src="./figs/index_service.jpeg" width=700>

#### 8.2 Index Service Interface

```go
type IndexService interface {
  Service
  RegisterNode(req RegisterNodeRequest) (RegisterNodeResponse, error)
  BuildIndex(req BuildIndexRequest) (BuildIndexResponse, error)
	GetIndexStates(req IndexStatesRequest) (IndexStatesResponse, error)
  GetIndexFilePaths(req IndexFilePathRequest) (IndexFilePathsResponse, error)
  NotifyTaskState(TaskStateNotification) error

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

* *BuildIndex*

```go
type BuildIndexRequest struct {
  DataPaths []string
  TypeParams map[string]string
  IndexParams map[string]string
}

type BuildIndexResponse struct {
  IndexID UniqueID
}
```


```go
type BuildIndexCmd struct {
  IndexID UniqueID
  Req BuildIndexRequest
}

type TaskStateNotification struct {
  IndexID UniqueID
  IndexState IndexState
  IndexFilePaths []string
  FailReason  string
}
```

* *GetIndexStates*

```go
type IndexStatesRequest struct {
	IndexID UniqueID 
}

enum IndexState {
    NONE = 0;
    UNISSUED = 1;
    INPROGRESS = 2;
    FINISHED = 3;
}

type IndexStatesResponse struct {
	ID                UniqueID
	State            IndexState
	//EnqueueTime       time.Time
	//ScheduleTime      time.Time
	//BuildCompleteTime time.Time
}
```

* *GetIndexFilePaths*

```go
type IndexFilePathRequest struct {
  IndexID UniqueID
}

type IndexFilePathsResponse struct {
  FilePaths []string
}
```



#### 8.3 Index Node Interface

```go
type IndexNode interface {
  Service
//  SetTimeTickChannel(channelName string) error
//  SetStatsChannel(channelName string) error
  
  BuildIndex(req BuildIndexCmd) error
}
```

