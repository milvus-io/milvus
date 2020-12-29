

## 8. Index Service



#### 8.1 Overview



#### 8.2 API

```go
type Client interface {
  BuildIndex(req BuildIndexRequest) (BuildIndexResponse, error)
	DescribeIndex(indexID UniqueID) (IndexDescription, error)
	GetIndexFilePaths(indexID UniqueID) (IndexFilePaths, error)
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



* *DescribeIndex*

```go
enum IndexStatus {
    NONE = 0;
    UNISSUED = 1;
    INPROGRESS = 2;
    FINISHED = 3;
}

type IndexDescription struct {
	ID                UniqueID
	Status            IndexStatus
	EnqueueTime       time.Time
	ScheduleTime      time.Time
	BuildCompleteTime time.Time
}
```



* *GetIndexFilePaths*

```go
type IndexFilePaths struct {
  FilePaths []string
}
```

