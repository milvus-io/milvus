# Flush Collection 
`Flush` operation is used to make sure that the data has been writen into the persistent storage, this document introduce how `Flush` operation works in `Milvus 2.0`. The following figure shows the execution flow of `Flush`

![flush_collections](./graphs/flush_data_coord.png)

1. Firstly, `SDK` starts a `Flush` request to `Proxy` via `Grpc`, the `proto` is defined as follows:
```proto
service MilvusService {
  ...

  rpc Flush(FlushRequest) returns (FlushResponse) {}

  ...
}

message FlushRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  repeated string collection_names = 3;
}

message FlushResponse{
  common.Status status = 1;
  string db_name = 2;
  map<string, schema.LongArray> coll_segIDs = 3;
}
```


2. When received the `Flush` request, the `Proxy` would wraps this request into `FlushTask`, and pushs this task into `DdTaskQueue` queue. After that, `Proxy` would call method of `WatiToFinish` to wait until the task finished.
```go
type task interface {
	TraceCtx() context.Context
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Name() string
	Type() commonpb.MsgType
	BeginTs() Timestamp
	EndTs() Timestamp
	SetTs(ts Timestamp)
	OnEnqueue() error
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
}

type FlushTask struct {
	Condition
	*milvuspb.FlushRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.FlushResponse
}
```

3. There is a backgroud service in `Proxy`, this service would get the `FlushTask` from `DdTaskQueue`, and executes it in three phases.
    - `PreExecute`,`FlushTask` does nothing at this phase, and return directly
    - `Execute`, at this phase, `Proxy` would send `Flush` request to `DataCoord` via `Grpc`,and wait for the reponse, the `proto` is defined as follow:
    ```proto
        service DataCoord {
          ...

          rpc Flush(FlushRequest) returns (FlushResponse) {}

          ...
        }

        message FlushRequest {
          common.MsgBase base = 1;
          int64 dbID = 2;
          int64 collectionID = 4;
        }

        message FlushResponse {
          common.Status status = 1;
          int64 dbID = 2;
          int64 collectionID = 3;
          repeated int64 segmentIDs = 4;
    ```
    - `PostExecute`, `FlushTask` does nothing at this phase, and return directly

4. After receiving `Flush` request from `Proxy`, `DataCoord` would call `SealAllSegments` to seal all the growing segments that belong to this `Collection`, and no longer allocate new `ID`s for these segments. After that, `DataCoord` would send response to `Proxy`, and the response should contain all the sealed segment ID.

5. In `Milvus 2.0`, the `Flush` is an asynchronous operation. So when `SDK` receives the response of `Flush`, it only means that the `DataCoord` has sealed these segments, and there are 2 problem that we have to soluved.
    - The sealed segments might still in the memory, and not have been writen into persistent storage yet.
    - `DataCoord` would no longer allocate new `ID`s for these sealed segments, but how to make sure all the allocated `ID`s have been consumed by `DataNode`.


6. For the first problem, `SDK` should send `GetSegmentInfo` request to `DataCoord` periodically, until all the sealed segment are in state of `Flushed`. the `proto` is defined as following.
```proto
service DataCoord {
  ...

  rpc GetSegmentInfo(GetSegmentInfoRequest) returns (GetSegmentInfoResponse) {}

  ...
}

message GetSegmentInfoRequest {
  common.MsgBase base = 1;
  repeated int64 segmentIDs = 2;
}

message GetSegmentInfoResponse {
  common.Status status = 1;
  repeated SegmentInfo infos = 2;
}

message SegmentInfo {
  int64 ID = 1;
  int64 collectionID = 2;
  int64 partitionID = 3;
  string insert_channel = 4;
  int64 num_of_rows = 5;
  common.SegmentState state = 6;
  internal.MsgPosition dml_position = 7;
  int64 max_row_num = 8;
  uint64 last_expire_time = 9;
  internal.MsgPosition start_position = 10;
}

enum SegmentState {
    SegmentStateNone = 0;
    NotExist = 1;
    Growing = 2;
    Sealed = 3;
    Flushed = 4;
    Flushing = 5;
}

```

7. For second problem, `DataNode` would report a timestamp to `DataCoord` every time it consumes a package from `MsgStream`,the Proto is define as follow.

 ```proto
 message DataNodeTtMsg {
    common.MsgBase base =1;
    string channel_name = 2;
    uint64 timestamp = 3;
}
```

8. There is a backgroud service, `startDataNodeTsLoop`, in `DataCoord` to process the message of `DataNodeTtMsg`.
    - Firstly, `DataCoord` would extract `channel_name` from `DataNodeTtMsg`, and filter out all the sealed segments that attached on this `channel_name`
    - Compare the timestamp when the segment enters into state of `Sealed` with the `DataNodeTtMsg.timestamp`, if `DataNodeTtMsg.timestamp` is greater, it means that all the `ID`s belong to that segment have been consumed by `DataNode`,so it's safe to notify `DataNode` to write that segment into persistent storage. The `proto` is defined as follow.
```proto
service DataNode {
  ...

  rpc FlushSegments(FlushSegmentsRequest) returns(common.Status) {}

  ...
}

message FlushSegmentsRequest {
  common.MsgBase base = 1;
  int64 dbID = 2;
  int64 collectionID = 3;
  repeated int64 segmentIDs = 4;
}

```