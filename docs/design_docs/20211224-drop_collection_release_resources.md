# DropCollection release resources

## Before this enhancement

**When dropping a collection**

1. DataNode releases the flowgraph of this collection and drops all the data in a buffer.
2. DataCoord has no idea whether a collection is dropped or not.
    - DataCoord will make DataNode watch DmChannels of dropped collections.
    - Blob files will never be removed even if the collection is dropped.

**For not in used binlogs on blob storage: Why are there such binlogs**
- A failure flush.
- A failure compaction.
- Dropped and out-of timetravel collection binlogs.

This enhancement is focused on solving these 2 problems.

## Object1 DropCollection

DataNode ignites Flush&Drop
    receive drop collection msg ->
    cancel compaction ->
    flush all insert buffer and delete buffer ->
    release the flowgraph

**Plan 1: Picked**

Add a `dropped` flag in `SaveBinlogPathRequest` proto.

DataNode
- Flush all segments in this vChannel, When Flush&Drop, set the `dropped` flag true.
    - If fails, retry at most 10 times and restart.

DataCoord
- DataCoord marks segmentInfo as `dropped`, doesn't remove segmentInfos from etcd.
- When recovery, check if the segments in the vchannel are all dropped.
    - if not, recover before the drop.
    - if so, no need to recover the vchannel.

Pros:
    1. The easiest approach in both DataNode and DataCoord.
    2. DataNode can reuse the current flush manager procedure.
Cons:
    1. The No. rpc call is equal to the No. segments in a collection, expensive.

---

**Plan 2: Enhance later**

Add a new rpc `FlushAndDrop`, it's a vchannel scope rpc.

Pros:
    1. much lesser rpc calls, equal to shard-numbers.
    2. More clarity of flush procedure in DataNode.
Cons:
    1. More efforts in DataNode and DataCoord.

```
message FlushAndDropRequest {
  common.MsgBase base = 1;
  string channelID = 2;
  int64 collectionID = 3;
  repeated SegmentBinlogPaths segment_binlog_paths = 6;
}

message SegmentBinlogPaths {
  int64 segmentID = 1;
  CheckPoint checkPoint = 2;
  repeated FieldBinlog field2BinlogPaths = 2;
  repeated FieldBinlog field2StatslogPaths = 3;
  repeated DeltaLogInfo deltalogs = 4;
}
```

---

## Object2: DataCoord Garbage Collection (GC) for not in used binlogs

### How to clear unknown binlogs?
DataCoord runs a background GC goroutine, triggers every 1 day:
1. Get all minIO/S3 paths(keys).
2. Filter out keys not in segmentInfo.
3. According to the meta of blobs from minIO/S3, remove binlogs that exist more than 1 day.
    - **Why 1 day: **Maybe there are newly uploaded binlogs from flush/compaction

### How to clear dropped-collection's binlogs?
- DataCoord checks all dropped-segments, removes the binlogs recorded if they've been dropped by 1 day.
- DataCoord keeps the etcd segmentInfo meta.
