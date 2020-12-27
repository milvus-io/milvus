

## 8. Query Service



#### 8.1 Overview



#### 8.2 API





```go
type Client interface {
  CreateQueryNodeGroup(nodeInstanceType string, numInstances int) (groupID UniqueID, error)
  DestoryQueryNodeGroup(groupID UniqueID) error
  DescribeQueryNodeGroup(groupID UniqueID) (QueryNodeGroupDescription, error)
  DescribeParition(groupID UniqueID, dbID UniqueID, collID UniqueID, partitionIDs []UniqueID) ([]PartitionDescription, error)
  CreateQueryChannel(groupID UniqueID) (QueryChannelInfo, error)
  LoadPartitions(groupID UniqueID, dbID UniqueID, collID UniqueID, partitionIDs []UniqueID) error
  ReleasePartitions(groupID UniqueID, dbID UniqueID, collID UniqueID, PartitionIDs []UniqueID) error
}
```

#### 

```go
// examples of node instance type (nodeInstanceType)
defaultInstanceType = "default"
userDefinedInstanceType = "custom.instance.type"
ec2StandardInstanceType = "c4.2xlarge"
```



```go
type QueryChannelInfo struct {
  RequestChannel string
  ResultChannel string
}
```



```go
type ResourceCost struct {
  MemUsage int64
  CpuUsage float32
}

type QueryNodeDescription struct {
  ResourceCost ResourceCost 
}

type CollectionDescription struct {
  ParitionIDs []UniqueID
}

type DbDescription struct {
  CollectionDescriptions []CollectionDescription
}

type QueryNodeGroupDescription struct {
  DbDescriptions map[UniqueID]DbDescription
  NodeDescriptions map[UniqueID]QueryNodeDescription
}
```



```go
type PartitionState = int

const (
  NOT_EXIST PartitionState = 0
  ON_DISK PartitionState = 1
  PARTIAL_IN_MEMORY PartitionState = 2
	IN_MEMORY PartitionState = 3
  PARTIAL_IN_GPU PartitionState = 4
  IN_GPU PartitionState = 5
)

type PartitionDescription struct {
  ID UniqueID
  State PartitionState
  ResourceCost ResourceCost
}
```





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



#### 8.3 Data Manipulation Service



```go
type manipulationService struct {
	ctx context.Context
	pulsarURL string
	fg *flowgraph.TimeTickedFlowGraph
	msgStream *msgstream.PulsarMsgStream
	node *QueryNode
}
```



