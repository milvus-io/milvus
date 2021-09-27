## Root Coordinator recovery on power failure

## 1. Basic idea
1. `RC(Root Coordinator`) reads meta from etcd when it starts
2. `RC` needs to store the `position` of the msgstream into etcd every time it consumes the msgstream.
3. `RC` reads the `position` of msgstream from etcd when it starts up, then seek to the specified `position` and re-consume the msgstream
4. Ensure that all messages from the msgstream are processed in idempotent fashion, so that repeated consumption of the same message does not cause system inconsistencies 
5. `RC`  registers itself in etcd and finds out if the dependent `DC(Data Coordinator)` and `IC(Index Coordinator)` are online via etcd

## 2. Specific tasks

### 2.1 Read meta from etcd
1. `RC` needs to load meta from etcd when it starts, this part is already done

### 2.2 `dd requests` from grpc
1. The `dd requests`, such as create_collection, create_partition, etc., from grpc are marked as done only if the related meta have been writen into etcd.
2. The `dd requests` should be send to `dd msgstream` when the operation is done. 
3. There may be a fault here, that is, the `dd request` has been written to etcd, but it has not been sent to `dd msgstream` yet, then the `RC` has crashed.
4. For the scenarios mentioned in item 3, `RC` needs to check if all `dd requests` are sent to `dd msgstream` when it starts up.
5. `RC`'s built-in scheduler ensures that all grpc requests are executed serially, so it only needs to check whether the most recent `dd requests` are sent to the `dd msgstream`, and resend them if not.
6. Take `create_collection` as an example to illustrate the process
    - When `create collection` is written to etcd, 2 additional keys are updated, `dd_msg` and `dd_type`
    - `dd_msg` is the serialization of the `dd_msg`
    - `dd_type` is the message type of `dd_msg`, such as `create_collection`, `create_partition`, `drop_collection,` etc. It's used to deserializes `dd_msg`.
    - Update the meta of `create_collection`, `dd_msg` and `dd_type` at the same time in a transactional manner.
    - When `dd_msg`  has been sent to `dd msgstream`, delete `dd_msg` and `dd_type` from etcd.
    - When the `RC` starts, first check whether there are `dd_msg` and `dd_type` in etcd, if so, then deserialize `dd_msg` according to `dd_type`, and then send it to the `dd msgstream`, otherwise no processing will be done
    - There may be a failure here, that is, `dd_msg` has been sent to the `dd msgstream` , but has not been deleted from etcd yet, then the `RC` crashed, at this case, the `dd_msg`  would be sent to `dd msgstream` repeatedly, so the receiver needs to count this case. 

### 2.3 `create index` requests from grpc
1. In the processing of `create index`, `RC` calls  `metaTable`'s `GetNotIndexedSegments` to get all segment ids that are not indexed
2. After getting the segment ids, `RC` call `IC` create the index on these segment ids.
3. In the current implementation, the `create index` requests will return after the segment ids are put into a go channel.
4. The `RC` starts a background task that keeps reading the segment ids from the go channel, and then calls the `IC` to create the index.
5. There is a fault here, the segment ids have been put into the go channel in the processing function of the grpc request, and then the grpc returns, but the `RC`'s background task has not yet read them from the go channel, theb `RC` crashes. At this time, the client thinks that the index is created, but the `RC` does not call `IC` to create the index.
6. The solution for the fault mentioned in item 5
    - Remove the go channel and `RC`'s background task
    - In the request processing function of `create index`, the call will return only when all segment ids have been send `IC`
    - Some segment ids may be send to `IC` repeatedly, and `IC` needs to handle such requests  

### 2.4 New segment from `DC`
1. Each time a new segment is created, the `DC` sends the segment id to the `RC` via msgstream
2. `RC` needs to update the segment id to the collection meta and record the position of the msgstream in etcd
3. Step 2 is transactional and the operation will be successful only if the collection meta in etcd is updated
4. So the `RC` only needs to restore the msgstream to the position when recovering from a power failure

### 2.5 Flushed segment from `data node`
1. Each time the `data node` finishes flushing a segment, it sends the segment id to the `RC` via msgstream.
2. `RC` needs to fetch binlog from `DC` by id and send request to `IC` to create index on this segment
3. When the `IC` is called successfully, it will return a build id, and then `RC` will update the build id to the `collection meta` and record the position of the msgstream in etcd.
4. Step 3 is transactional and the operation will be successful only if the `collection meta` in etcd is updated
5. So the `RC` only needs to restore the msgstream to the position when recovering from a power failure

### 2.6 Failed to call external grpc service
1. `RC` depends on `DC` and `IC`, if the grpc call failed, it needs to reconnect.
2. `RC` does not listen to the status of the `DC` and `IC` in real time

### 2.7 Add virtual channel assignment when creating collection
1. Add a new field, "number of shards" in the `create collection` request, the "num of shards" tell the `RC` to create the number of virtual channel for this collection.
2. In the current implementation, virtual channels and physical channels have a one-to-one relationship, and the total number of physical channels increases as the number of virtual channels increases; later, the total number of physical channels needs to be fixed, and multiple virtual channels share one physical channel
3. The name of the virtual channel is globally unique, and the `collection meta` records the correspondence between the virtual channel and the physical channel


### Add processing of time synchronization signals from proxy node
1. A virtual channel can be inserted by multiple proxies, so the timestamp in the virtual channel is not increase monotonically
2. All proxies report the timestamp of all the virtual channels to the `RC` periodically
3. The `RC` collects the timestamps from the proxies on each virtual channel and gets the minimum one as the timestamp of that virtual channel, and then inserts the timestamp into the virtual channel  
4. The proxy reports the timestamp to the `RC` via grpc
5. The proxy needs to register itself in etcd when it starts, `RC` will listen to the corresponding key to determine how many active proxies there are, and thus determine if all of them have sent timestamps to `RC` 
6. If a proxy is not registered in etcd but sends a timestamp or any other grpc request to `RC`, `RC` will ignore the grpc request

### 2.9 Register service in etcd
1. `RC` needs to register itself with etcd when it starts
2. The registration should include ip address, port, its own id, global incremental timestamp 

### 2.10 Remove the code related to proxy service
1. The `proxy service` related code  will be removed
2. The the job of time synchronization which done by the `proxy service` is partially simplified and handed over to the `RC` (subsection 2.8)

### 2.11 Query collection meta based on timeline
1. Add a new field of `timestamp` to the grpc request of `describe collection`
2. `RC` should provide snapshot on the `collection mate`
3. Return the `collection meta` at the point of timestamp mentioned in the request

### 2.12 Timestamp of `dd operations`
1. `RC` response to set the timestamp of `dd operations`, create collection, create partition, drop collection, drop partition, and send this timestamp into `dml msgstream`
