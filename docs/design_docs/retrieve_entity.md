# Support to retrieve the specified entity from a collection

## Background

Milvus supports one entity containing multiple vector fields and multiple scalar fields.

When creating a collection, you can specify using the primary key generated automatically or the user-provided primary key. If the user sets to use the user-provided primary key, each entity inserted must contain the primary key field, otherwise, the insertion will fail. The primary keys will be returned after the insertion request is successful.

Milvus currently only supports primary keys of int64 type.

QueryNode subscribes to the insert channel and will determine whether to use the data extracted from insert channel or data processed by DataNode to provide services according to the status of a segment.

## Goals

- Support to retrieve one or more entities from a collection through primary keys
- Support to retrieve only some fields of an entity
- Consider backward file format compatibility if a new file is defined

## Non-Goals

- How to deal with duplicate primary keys
- How to retrieve entity by non-primary key

## Detailed design

When the DataNode processes each inserted entity, it updates the bloomfilter of the Segment to which the entity belongs. If it does not exist, it creates a bloomfilter in memory and updates it.

Once DataNode receives a Flush command from DataCoord, it sorts the data in the segment in ascending order of primary key, records the maximum and minimum values of primary key, and writes the segment, statistics and bloomfilter to storage system.

- Key of binlog file: `${tenant}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/_${log_idx}`
- Key of statistics file: `${tenant}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/stats_${log_idx}`
- Key of bloom filter file: `${tenant}/insert_log/${collection_id}/${partition_id}/${segment_id}/${field_id}/bf_${log_idx}`

QueryNode maintains mapping from primary key to entities in each segment. This mapping updates every time an insert request is processed.

After receiving the Get request from the client, the Proxy writes the request to the `search` channel and waits for the result returned from the `searchResult` channel.

The processing flow after QueryNode reads the Get request from `search` channel:

1. Search for the existence of the requested primary key in the mapping of the `Growing` status segment, and return directly if found;
2. If the primary key exists in any mapping of `Growing` segments, return the results;
3. Load statistics and bloomfilter of all `Sealed` segments;
4. Convert the statistics into an inverted index from Range to SegmentID for each `Sealed` segment;
5. Check whether the requested primary key exists in any inverted index of `Sealed` segment, return empty if not found;
6. [optional] Use the bloomfilter to filter out segments where the primary key does not exist;
7. Use binary search to find specified entity in each segment where the primary key may exist;

### APIs

```go
// pseudo-code
func get(collection_name string,
         ids list[string],
         output_fields list[string],
         partition_names list[string]) (list[entity], error)
// Example
// entities = get("collection1", ["103"], ["_id", "age"], nil)
```

When the primary key does not exist in specified collection( and partitions), Milvus will return an empty result, which is not considered as an error.

### Storage

Both bloomfilter files and statistical information files belong to Binlog files and follow the Binlog file format.

https://github.com/milvus-io/milvus/blob/master/docs/developer_guides/chap08_binlog.md

Two new types of Binlog are added: BFBinlog and StatsBinlog.

BFBinlog Payload: Refer to https://github.com/milvus-io/milvus/blob/1.1/core/src/segment/SegmentWriter.h for storage methods

StatsBinlog Payload: Json format string, currently only contains the keys `max`, `min`.

## Impact

### API

- A new Get API
- DropCollection / ReleaseCollection / DropPartition / ReleasePartition requests need to clear the corresponding statistics files and bloomfilter files in the memory

### Storage

- The name of the binlog file has been changed from `${log_idx}` to `_${log_idx}`
- Each binlog adds a stats file
- Each binlog adds a bloomfilter file

## Test Plan

### Testcase 1

In the newly created collection, insert an entity with a primary key of 107, call the Get interface to query the entity with a primary key of 107, and each field of the retrieved entity is exactly the same as the inserted entity.

### Testcase 2

In the newly created collection, insert a record with a primary key of 107, call the Get interface to query the record with a primary key of 106, and the retrieved record is empty.

### Testcase 3

In the newly created collection, insert the records with the primary keys of 105, 106, 107, call the Get interface to query the records with the primary keys of 101, 102, 103, 104, 105, 106, 107, the retrieved result only contains the primary keys of 105, 106 , 107 records.

### Testcase 4

In the newly created collection, insert a record with a primary key of 107, call the Flush interface, and check whether there are stats and bloomfilter files on MinIO.
