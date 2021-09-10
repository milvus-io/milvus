# DataNode DDL Flush Design

update: 5.19.2021, by [Goose](https://github.com/XuanYang-cn)
update: 5.21.2021, by [Goose](https://github.com/XuanYang-cn)
update: 6.04.2021, by [Goose](https://github.com/XuanYang-cn)

**THIS IS OUTDATE**

## Background

Data Definition Language (DDL) is a language used to define data structures and modify data<sup>[1](#techterms1)</sup>.
In Milvus terminology, for instance, `CreateCollection` and `DropPartition` etc. are DDL. In order to recover
or redo DD operations, DataNode flushes DDLs into persistent storages.

Before this design, DataNode buffers DDL chunks by collection, flushes all buffered data in manul/auto flush.

Now in [DataNode Recovery Design](datanode_recover_design_0513_2021.md), flowgraph : vchannel = 1 : 1, and insert
data of one segment is always in one vchannel. So each flowgraph concerns only about ONE specific collection. For
DDL channels, one flowgraph only cares about DDL operations of one collection. In this case,
I don't think it's necessary to auto-flush ddl anymore.

## Goals

-  Flowgraph knows about which segment/collection to concern.
-  DDNode update masPositions once it buffers ddl about the collection.
-  DDNode won't auto flush.
-  In manul-flush, a background flush-complete goroutinue waits for DDNode and InsertBufferNode both done
flushing, waiting for both binlog paths.

## Detailed design

1. Redesign of DDL binlog paths and etcd paths for these binlog paths


DDL flushes based on a manul flush of a segment.

**Former design**
```
# minIO/S3 ddl binlog paths
${tenant}/data_definition_log/${collection_id}/ts/${log_idx}
${tenant}/data_definition_log/${collection_id}/ddl/${log_idx}

# etcd paths for ddl binlog paths
${prefix}/${collectionID}/${idx}
```

The minIO/S3 ddl binlog paths seems ok, but etcd paths aren't clear, especially when we want to relate a ddl flush
to a certain segment flush.

**Redesign**
```
# etcd paths for ddl binlog paths
${prefix}/${collectionID}/${segmentID}/${idx}
```

```
message PositionPair {
  internal.MsgPosition start_position = 1;
  internal.MsgPosition end_position = 2;
}

message SaveBinlogPathsRequest {
    common.MsgBase base = 1;
    int64 segmentID = 2;
    int64 collectionID = 3;
    ID2PathList field2BinlogPaths = 4;
    repeated DDLBinlogMeta = 5;
    PositionPair dml_position = 6;
    PositionPair ddl_position =7;
 }
```

## TODOs

1. Refactor auto-flush of ddNode
3. Refactor etcd paths

<a name="techterms1">[1]</a>: *[techterms.com](https://techterms.com/definition/ddl#:~:text=Stands%20for%20%22Data%20Definition%20Language,SQL%2C%20the%20Structured%20Query%20Language)*
