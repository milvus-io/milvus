# DataNode Flowgraph Recovery Design

update: 6.4.2021, by [Goose](https://github.com/XuanYang-cn)
update: 6.21.2021, by [Goose](https://github.com/XuanYang-cn)

## 1. Common Sense
A. 1 message stream to 1 vchannel, so there are 1 start position and 1 end position in 1 message pack
B. Only when datanode flushes, datanode will update every segment's position
An optimization: update position of
  - a. Current flushing segment 
  - b. StartPosition of segments never been flushed.
C. DataNode auto-flush is a valid flush.
D. DDL messages are now in DML Vchannels.

## 2. Segments in Flowgraph

![segments](graphs/segments.png)

## 3. Flowgraph Recovery
### A. Save checkpoints
When a flowgraph flushes a segment, we need to save these things:
- current segment's binlog paths,
- current segment positions,
- all other segments' current positions from replica (If a segment hasn't been flushed, save the position when datanode first meet it.)

Whether save successfully:
- If successed, flowgraph updates all segments' position to replica
- If not
    - For a grpc failure( this failure will appear after many times retry internally), crush itself.
    - For a normal failure, retry save 10 times, if fail still, crush itself. 

### B. Recovery from a set of checkpoints
1. We need all positions of all segments in this vchannel `p1, p2, ... pn`

A design of WatchDmChannelReq
``` proto
message VchannelInfo {
  int64 collectionID = 1;
  string channelName = 2;
  internal.MsgPosition seek_position = 3;
  repeated SegmentInfo unflushedSegments = 4;
  repeated int64 flushedSegments = 5;
}

message WatchDmChannelsRequest {
  common.MsgBase base = 1;
  repeated VchannelInfo vchannels = 2;
}
```

2. We want to filter msgPacks based on these positions.

![recovery](graphs/flowgraph_recovery_design.png)

Supposing we have segment `s1, s2, s3`, corresponding position `p1, p2, p3`
  - Sort positions in reverse order `p3, p2, p1`
  - Get segments dup range time: `s3 ( p3 > mp_px > p1)`, `s2 (p2 > mp_px > p1)`, `s1(zero)`
  - Seek from the earliest, in this example `p1`
  - Then for every msgPack after seeking `p1`,  the pseudocode:

```go
const filter_threshold = recovery_time
// mp means msgPack
for mp := seeking(p1) {
    if mp.position.endtime < filter_threshod {  
        if mp.position < p3 {
            filter s3
        }
        if mp.position < p2 {
            filter s2
        }
    }
}
```
