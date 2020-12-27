

## 8. Index Service



#### 8.1 Overview



#### 8.2 API

```protobuf
enum IndexStatus {
    NONE = 0;
    UNISSUED = 1;
    INPROGRESS = 2;
    FINISHED = 3;
}
```



```go
type IndexDescription struct {
	ID                UniqueID
	Status            IndexStatus
	EnqueueTime       time.Time
	ScheduleTime      time.Time
	BuildCompleteTime time.Time
}


type Client interface {
  BuildIndex(dataPaths []string, typeParams map[string]string, indexParams map[string]string) (UniqueID, error)
	DescribeIndex(indexID UniqueID) (*IndexDescription, error)
	GetIndexFilePaths(indexID UniqueID) ([]string, error)
}
```

