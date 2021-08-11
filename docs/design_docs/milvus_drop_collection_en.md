# Drop Collection
`Milvus 2.0` use `Collection` to represent a set of data, like `Table` in traditional database. Users can create or drop `Collection`. Altering the `Schema` of `Collection` is not supported yet. This article introduces the execution path of `Drop Collection`, at the end of this article, you should know which components are involved in `Drop Collection`.

The execution flow of `Drop Collection` is shown in the following figure:

![drop_collection](./graphs/dml_drop_collection.png)

1. Firstly, `SDK` starts a `DropCollection` request to `Proxy` via `Grpc`, the `proto` is defined as follows:
```proto
service MilvusService {
    ...

    rpc DropCollection(DropCollectionRequest) returns (common.Status) {}

    ...
}

message DropCollectionRequest {
  common.MsgBase base = 1; // must
  string db_name = 2;
  string collection_name = 3; // must
}
```

2. When received the `DropCollection` request, the `Proxy` would wraps this request into `DropCollectionTask`, and pushs this task into `DdTaskQueue` queue. After that, `Proxy` would call method of `WatiToFinish` to wait until the task finished.
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

type DropCollectionTask struct {
	Condition
	*milvuspb.DropCollectionRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
	chMgr     channelsMgr
	chTicker  channelsTimeTicker
}
```

3. There is a backgroud service in `Proxy`, this service would get the `DropCollectionTask` from `DdTaskQueue`, and executes it in three phases.
    - `PreExecute`, do some static checking at this phase, such as check if `Collection Name` is legal etc.
    - `Execute`, at thie phase, `Proxy` would send `DropCollection` request to `RootCoord` via `Grpc`,and wait the reponse, the `proto` is defined as follow:
    ```proto
        service RootCoord {
          ...

           rpc DropCollection(milvus.DropCollectionRequest) returns (common.Status) {}

          ...
        }
    ```
    - `PostExecute`, `Proxy` would delete the `Collection`'s meta from global meta table at this phase. 

4. `RootCoord` would wraps the `DropCollection` request into `DropCollectionReqTask`, and then call function `executeTask`. `executeTask` would return until the `context` is done or `DropCollectionReqTask.Execute` returned.
```go
type reqTask interface {
	Ctx() context.Context
	Type() commonpb.MsgType
	Execute(ctx context.Context) error
	Core() *Core
}

type DropCollectionReqTask struct {
	baseReqTask
	Req *milvuspb.DropCollectionRequest
}
```

5. Firstly, `RootCoord` would delete `Collection`'s meta from `metaTable`, including `schema`,`partition`, `segment`,`index`, all of these delete operations are committed in one transaction.

6. After `Collection`'s meta has been deleted from `metaTable`, `Milvus` would consider this collection has been deleted successfully.

7. `RootCoord` would alloc a timestamp from `TSO` before deleting `Collection`'s meta from `metaTable`, and this timestamp is considered as the point when the collection was deleted. 

8. `RoooCoord` would send a message of `DropCollectionRequest` into `MsgStream`, and other components, who has subscribe to the `MsgStream`, would be notified. The `Proto` of `DropCollectionRequest` is defined as follow:
```proto
message DropCollectionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collectionName = 3;
  int64 dbID = 4;
  int64 collectionID = 5;
}

```

9. After these operations, `RootCoord` would update internal timestamp.

10. Then `RootCoord` would start a `ReleaseCollection` request to `QueryCoord` via `Grpc` , notify `QueryCoord` to release all the resouces that related to this `Collection`. This `Grpc` request is done in another `goroutine`, so it would not block the main thread. The `proto` is defined as follow:
```proto
service QueryCoord {
    ...

    rpc ReleaseCollection(ReleaseCollectionRequest) returns (common.Status) {}

    ...
}

message ReleaseCollectionRequest {
  common.MsgBase base = 1;
  int64 dbID = 2;
  int64 collectionID = 3;
  int64 nodeID = 4;
}
```

11. At last, `RootCoord` would send `InvalidateCollectionMetaCache` request to each `Proxy`, notify `Proxy` to remove `Collection`'s meta, the `proto` is defined as follow: 
```proto
service Proxy {
    ...

    rpc InvalidateCollectionMetaCache(InvalidateCollMetaCacheRequest) returns (common.Status) {}

    ...
}

message InvalidateCollMetaCacheRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
}
```
 
12. The execution flow of `QueryCoord.ReleaseCollection` is shown in the follwing figure.

![releae_collection](./graphs/dml_release_collection.png)

13. `QueryCoord` would wraps `ReleaseCollection` into `ReleaseCollectionTask`, and push the task into `TaskScheduler`

14. There is a backgroud service in `QueryCoord`, this service would get the `ReleaseCollectionTask` from `TaskScheduler`, and executes it in three phases.
    - `PreExecute`, `ReleaseCollectionTask` would only print debug log at this phase.
    - `Execute`, there are two jobs at this phase:
        - send a `ReleaseDQLMessageStream` request to `RootCoord` via `Grpc`, `RootCoord` would redirect the `ReleaseDQLMessageStream` request to each `Proxy`, notify the `Proxy` that not processing any message of this `Collection` anymore. The `proto` is defined as follow: 
        ```proto
            message ReleaseDQLMessageStreamRequest {
                common.MsgBase base = 1;
                int64 dbID = 2;
                int64 collectionID = 3;
            }
        ```
        - send a `ReleaseCollection` request to each `QueryNode` via `Grpc`, notify the `QueryNode` to release all the resources related to this `Collection`, including `Index`, `Segment`, `FlowGraph`, etc. `QueryNode` would no longer read any message from this `Collection`'s `MsgStream` anymore  
        ```proto
            service QueryNode {
                ...

                rpc ReleaseCollection(ReleaseCollectionRequest) returns (common.Status) {}

                ...
            }

            message ReleaseCollectionRequest {
                common.MsgBase base = 1;
                int64 dbID = 2;
                int64 collectionID = 3;
                int64 nodeID = 4;
            }
        ```
    - `PostExecute`, `ReleaseCollectionTask` would only print debug log at this phase.

15. After these operations, `QueryCoord` would send `ReleaseCollection`'s reponse to `RootCoord

16. At `Step 8`, `RoooCoord` has sent a message of `DropCollectionRequest` into `MsgStream`, `DataNode` would subscribe this `MsgStream`, so `DataNode` would be notified to released the resources. The execution flow is shown in the following figure.

![releae_collection](./graphs/dml_release_flow_graph_on_data_node.png)

17. In `DataNode`, each `MsgStream` will have a `FlowGraph`, all the messages are processed by that `FlowGraph`. When the `DataNode` receives the message of `DropCollectionRequest`, `DataNode` would notify `BackouGroundGC`, which is a background service on `DataNode`, to release the resouces. 

*Notes*:
1. Currently, the `DataCoord` has not response to the `DropCollection`, so the `Collection`'s `segment meta` are still exist in the `DataCoord`'s `metaTable`, and the `Binlog` files belongs to this `Collection` are still exist on the persistent storage. 
2. Currently, the `IndexCoord` has not response to the `DropCollection`, so the `Collection`'s `index file` are still exist on the persistent storage. 
 



