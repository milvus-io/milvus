# Brief

This document will help SDK developers to understand each API of Milvus RPC proto.

# Table of contents
=================
- [Brief](#brief)
- [# Table of contents](#-table-of-contents)
- [milvus.proto](#milvusproto)
  - [CreateCollection](#createcollection)
    - [CreateCollectionRequest](#createcollectionrequest)
  - [DropCollection](#dropcollection)
    - [DropCollectionRequest](#dropcollectionrequest)
  - [HasCollection](#hascollection)
    - [HasCollectionRequest](#hascollectionrequest)
  - [LoadCollection](#loadcollection)
    - [LoadCollectionRequest](#loadcollectionrequest)
  - [ReleaseCollection](#releasecollection)
    - [ReleaseCollectionRequest](#releasecollectionrequest)
  - [DescribeCollection](#describecollection)
    - [DescribeCollectionRequest](#describecollectionrequest)
    - [DescribeCollectionResponse](#describecollectionresponse)
  - [GetCollectionStatistics](#getcollectionstatistics)
    - [GetCollectionStatisticsRequest](#getcollectionstatisticsrequest)
    - [GetCollectionStatisticsResponse](#getcollectionstatisticsresponse)
  - [ShowCollections](#showcollections)
    - [ShowCollectionsRequest](#showcollectionsrequest)
    - [ShowCollectionsResponse](#showcollectionsresponse)
  - [CreatePartition](#createpartition)
    - [CreatePartitionRequest](#createpartitionrequest)
  - [DropPartition](#droppartition)
    - [DropPartitionRequest](#droppartitionrequest)
  - [HasPartition](#haspartition)
    - [HasPartitionRequest](#haspartitionrequest)
  - [LoadPartitions](#loadpartitions)
    - [LoadPartitionsRequest](#loadpartitionsrequest)
  - [ReleasePartitions](#releasepartitions)
    - [ReleasePartitionsRequest](#releasepartitionsrequest)
  - [GetPartitionStatistics](#getpartitionstatistics)
    - [GetPartitionStatisticsRequest](#getpartitionstatisticsrequest)
    - [GetPartitionStatisticsResponse](#getpartitionstatisticsresponse)
  - [ShowPartitions](#showpartitions)
    - [ShowPartitionsRequest](#showpartitionsrequest)
    - [ShowPartitionsResponse](#showpartitionsresponse)
  - [CreateAlias](#createalias)
    - [CreateAliasRequest](#createaliasrequest)
  - [DropAlias](#dropalias)
    - [DropAliasRequest](#dropaliasrequest)
  - [AlterAlias](#alteralias)
    - [AlterAliasRequest](#alteraliasrequest)
  - [CreateIndex](#createindex)
    - [CreateIndexRequest](#createindexrequest)
  - [DescribeIndex](#describeindex)
    - [DescribeIndexRequest](#describeindexrequest)
    - [DescribeIndexResponse](#describeindexresponse)
  - [GetIndexState](#getindexstate)
    - [GetIndexStateRequest](#getindexstaterequest)
    - [GetIndexStateResponse](#getindexstateresponse)
  - [GetIndexBuildProgress](#getindexbuildprogress)
    - [GetIndexBuildProgressRequest](#getindexbuildprogressrequest)
    - [GetIndexBuildProgressResponse](#getindexbuildprogressresponse)
  - [DropIndex](#dropindex)
    - [DropIndexRequest](#dropindexrequest)
  - [Insert](#insert)
    - [InsertRequest](#insertrequest)
  - [Delete](#delete)
    - [DeleteRequest](#deleterequest)
  - [Search](#search)
    - [SearchRequest](#searchrequest)
    - [SearchResults](#searchresults)
  - [Flush](#flush)
    - [FlushRequest](#flushrequest)
    - [FlushResponse](#flushresponse)
  - [Query](#query)
    - [QueryRequest](#queryrequest)
    - [QueryResults](#queryresults)
  - [CalcDistance](#calcdistance)
    - [VectorIDs](#vectorids)
    - [VectorsArray](#vectorsarray)
    - [CalcDistanceRequest](#calcdistancerequest)
    - [CalcDistanceResults](#calcdistanceresults)
  - [GetPersistentSegmentInfo](#getpersistentsegmentinfo)
    - [GetPersistentSegmentInfoRequest](#getpersistentsegmentinforequest)
    - [PersistentSegmentInfo](#persistentsegmentinfo)
    - [GetPersistentSegmentInfoResponse](#getpersistentsegmentinforesponse)
  - [GetQuerySegmentInfo](#getquerysegmentinfo)
    - [GetQuerySegmentInfoRequest](#getquerysegmentinforequest)
    - [QuerySegmentInfo](#querysegmentinfo)
    - [GetQuerySegmentInfoResponse](#getquerysegmentinforesponse)
  - [GetMetrics](#getmetrics)
    - [GetMetricsRequest](#getmetricsrequest)
    - [GetMetricsResponse](#getmetricsresponse)
  - [LoadBalance](#loadbalance)
    - [LoadBalanceRequest](#loadbalancerequest)
  - [GetCompactionState](#getcompactionstate)
    - [GetCompactionStateRequest](#getcompactionstaterequest)
    - [GetCompactionStateResponse](#getcompactionstateresponse)
  - [ManualCompaction](#manualcompaction)
    - [ManualCompactionRequest](#manualcompactionrequest)
    - [ManualCompactionResponse](#manualcompactionresponse)
  - [GetCompactionStateWithPlans](#getcompactionstatewithplans)
    - [GetCompactionPlansRequest](#getcompactionplansrequest)
    - [GetCompactionPlansResponse](#getcompactionplansresponse)
    - [CompactionMergeInfo](#compactionmergeinfo)
  - [ShowType](#showtype)
  - [BoolResponse](#boolresponse)
  - [MutationResult](#mutationresult)
- [common.proto](#commonproto)
  - [ErrorCode](#errorcode)
  - [IndexState](#indexstate)
  - [SegmentState](#segmentstate)
  - [Status](#status)
  - [KeyValuePair](#keyvaluepair)
  - [KeyDataPair](#keydatapair)
  - [DslType](#dsltype)
  - [CompactionState](#compactionstate)
- [schema.proto](#schemaproto)
  - [DataType](#datatype)
  - [FieldSchema](#fieldschema)
  - [CollectionSchema](#collectionschema)
  - [BoolArray](#boolarray)
  - [IntArray](#intarray)
  - [LongArray](#longarray)
  - [FloatArray](#floatarray)
  - [DoubleArray](#doublearray)
  - [BytesArray](#bytesarray)
  - [StringArray](#stringarray)
  - [ScalarField](#scalarfield)
  - [VectorField](#vectorfield)
  - [FieldData](#fielddata)
  - [IDs](#ids)
  - [SearchResultData](#searchresultdata)


# milvus.proto

## CreateCollection
Create a collection with schema. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc CreateCollection(CreateCollectionRequest) returns (common.Status) {}
```
[back to top](#brief)

### CreateCollectionRequest
```
message CreateCollectionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The unique collection name in milvus.(Required)
  string collection_name = 3;

  // The serialized `schema.CollectionSchema`(Required)
  bytes schema = 4;

  // Once set, no modification is allowed (Optional)
  // https://github.com/milvus-io/milvus/issues/6690
  int32 shards_num = 5;
}
```

## DropCollection
Drop a collection by name. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc DropCollection(DropCollectionRequest) returns (common.Status) {}
```
[back to top](#brief)

### DropCollectionRequest
```
message DropCollectionRequest {
  // Not useful for now
  common.MsgBase base = 1; 

  // Not useful for now
  string db_name = 2;

  // The name of the collection to be dropped. (Required)
  string collection_name = 3;
}
```

## HasCollection
To test the existence of a collection by name. Return [BoolResponse](#boolresponse) to tell client whether the collection is exist.
```
rpc HasCollection(HasCollectionRequest) returns (BoolResponse) {}
```
[back to top](#brief)

### HasCollectionRequest
```
message HasCollectionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The name of the collection which you want to check. (Required)
  string collection_name = 3; 

  // If time_stamp is not zero, will return true when time_stamp >= created collection timestamp, otherwise will return false.
  uint64 time_stamp = 4;
}
```

## LoadCollection
Load a collection data into cache of query node. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
Note: this interface only sends a request to server ask to load collection, it returns at once after the request is consumed. Collection loading progress is asynchronously.
```
rpc LoadCollection(LoadCollectionRequest) returns (common.Status) {}
```
[back to top](#brief)

### LoadCollectionRequest
```
message LoadCollectionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The name of the collection which you want to load into cache. (Required)
  string collection_name = 3;
}
```

## ReleaseCollection
Release a collection data from cache of query node. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc ReleaseCollection(ReleaseCollectionRequest) returns (common.Status) {}
```
[back to top](#brief)

### ReleaseCollectionRequest
```
message ReleaseCollectionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The name of the collection which you want to release. (Required)
  string collection_name = 3;
}
```

## DescribeCollection
Get a collection's information including id, schema, alias, timestamp and internal data transfer channel. (Some returned information is for developers).
```
rpc DescribeCollection(DescribeCollectionRequest) returns (DescribeCollectionResponse) {}
```
[back to top](#brief)

### DescribeCollectionRequest
```
message DescribeCollectionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name you want to describe, you can pass collection_name or collectionID. (Required)
  string collection_name = 3;

  // The collection ID you want to describe
  int64 collectionID = 4;

  // If time_stamp is not zero, will describe collection success when time_stamp >= created collection timestamp, otherwise will throw an error.
  uint64 time_stamp = 5;
}
```

### DescribeCollectionResponse
```
message DescribeCollectionResponse {
  // Contain error_code and reason
  common.Status status = 1;

  // The schema param when you created collection.
  schema.CollectionSchema schema = 2;

  // The collection id
  int64 collectionID = 3;

  // System design related, users should not perceive
  repeated string virtual_channel_names = 4;

  // System design related, users should not perceive
  repeated string physical_channel_names = 5;

  // Hybrid timestamp in milvus
  uint64 created_timestamp = 6;

  // The utc timestamp calculated by created_timestamp
  uint64 created_utc_timestamp = 7;

  // The shards number you set.
  int32 shards_num = 8;

  // The aliases of this collection
  repeated string aliases = 9;

  // The message ID/posititon when collection is created
  repeated common.KeyDataPair start_positions = 10;
}
```

## GetCollectionStatistics
Get collection statistics. Currently only support return row count.
```
rpc GetCollectionStatistics(GetCollectionStatisticsRequest) returns (GetCollectionStatisticsResponse) {}
```
[back to top](#brief)

### GetCollectionStatisticsRequest
```
message GetCollectionStatisticsRequest {
  // Not useful for now
  common.MsgBase base = 1;
  // Not useful for now
  string db_name = 2;
  // The collection name you want get statistics. (Required)
  string collection_name = 3;
}
```

### GetCollectionStatisticsResponse
See [common.KeyValuePair](#keyvaluepair).
```
message GetCollectionStatisticsResponse {
  // Contain error_code and reason
  common.Status status = 1;

  // Collection statistics data in key-value pairs. Currently only return one pair: "row_count":xxx
  repeated common.KeyValuePair stats = 2;
}
```

## ShowCollections
Get all collections name in db, or get in-memory state for each collection.
```
rpc ShowCollections(ShowCollectionsRequest) returns (ShowCollectionsResponse) {}
```
[back to top](#brief)

### ShowCollectionsRequest
See [ShowType](#showtype).
```
message ShowCollectionsRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // Not useful for now
  uint64 time_stamp = 3;

  // Decide return Loaded collections or All collections(Optional)
  ShowType type = 4;

  // When type is InMemory, will return these collection's inMemory_percentages.(Optional)
  repeated string collection_names = 5; 
}
```

### ShowCollectionsResponse
```
message ShowCollectionsResponse {
  // Contain error_code and reason
  common.Status status = 1;

  // Collection name array
  repeated string collection_names = 2;

  // Collection Id array
  repeated int64 collection_ids = 3;

  // Hybrid timestamps in milvus
  repeated uint64 created_timestamps = 4;

  // The utc timestamp calculated by created_timestamp
  repeated uint64 created_utc_timestamps = 5;

  // Load percentage on QueryNode when type is InMemory
  repeated int64 inMemory_percentages = 6; 
}
```

## CreatePartition
Create a partition in a collection. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc CreatePartition(CreatePartitionRequest) returns (common.Status) {}
```
[back to top](#brief)

### CreatePartitionRequest
```
message CreatePartitionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name in milvus. (Required)
  string collection_name = 3;

  // The name of the partition to be created. (Required)
  string partition_name = 4;
}
```

## DropPartition
Drop a partition by name. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc DropPartition(DropPartitionRequest) returns (common.Status) {}
```
[back to top](#brief)

### DropPartitionRequest
```
message DropPartitionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name in milvus. (Required)
  string collection_name = 3;

  // The name of the partition which you want to drop. (Required)
  string partition_name = 4; 
}
```

## HasPartition
To test existence of a partition by name. Return [BoolResponse](#boolresponse) to tell client whether the partition exists
```
rpc HasPartition(HasPartitionRequest) returns (BoolResponse) {}
```
[back to top](#brief)

### HasPartitionRequest
```
message HasPartitionRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name in milvus
  string collection_name = 3;

  // The name of the partition which you want to check
  string partition_name = 4;
}
```

## LoadPartitions
Load multiple partitions data into cache of QueryNode. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
Note: this interface only send a request to server ask to load partitions, it returns at once after the request is consumed. Loading progress is asynchronously.
```
rpc LoadPartitions(LoadPartitionsRequest) returns (common.Status) {}
```
[back to top](#brief)

### LoadPartitionsRequest
```
message LoadPartitionsRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name in milvus. (Required)
  string collection_name = 3;

  // A name array of the partitions which you want to load. (Required)
  repeated string partition_names = 4;
}
```

## ReleasePartitions
Release partitions data from cache of QueryNode. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc ReleasePartitions(ReleasePartitionsRequest) returns (common.Status) {}
```
[back to top](#brief)

### ReleasePartitionsRequest
```
message ReleasePartitionsRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name in milvus. (Required)
  string collection_name = 3;

  // A name array of the partitions which you want to release. (Required)
  repeated string partition_names = 4;
}
```

## GetPartitionStatistics
Get partition statistics. Currently only support return row count.
```
rpc GetPartitionStatistics(GetPartitionStatisticsRequest) returns (GetPartitionStatisticsResponse) {}
```
[back to top](#brief)

### GetPartitionStatisticsRequest
```
message GetPartitionStatisticsRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name in milvus
  string collection_name = 3;

  // The partition name you want to collect statistics
  string partition_name = 4; 
}
```

### GetPartitionStatisticsResponse
```
message GetPartitionStatisticsResponse {
  // Contain error_code and reason
  common.Status status = 1;

  // Collection statistics data in key-value pairs. Currently only return one pair: "row_count":xxx
  repeated common.KeyValuePair stats = 2;
}
```

## ShowPartitions
Get all partitions' names in db, or get in-memory state for each partition.
```
rpc ShowPartitions(ShowPartitionsRequest) returns (ShowPartitionsResponse) {}
```
[back to top](#brief)

### ShowPartitionsRequest
See [ShowType](#showtype).
```
message ShowPartitionsRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The collection name you want to describe, you can pass collection_name or collectionID
  string collection_name = 3;

  // The collection id in milvus
  int64 collectionID = 4;

  // When type is InMemory, will return these patitions's inMemory_percentages.(Optional)
  repeated string partition_names = 5;

  // Decide return Loaded partitions or All partitions(Optional)
  ShowType type = 6;
}
```

### ShowPartitionsResponse
```
message ShowPartitionsResponse {
  // Contain error_code and reason
  common.Status status = 1;

  // All partition names for this collection
  repeated string partition_names = 2;

  // All partition ids for this collection
  repeated int64 partitionIDs = 3;

  // All hybrid timestamps
  repeated uint64 created_timestamps = 4;

  // All utc timestamps calculated by created_timestamps
  repeated uint64 created_utc_timestamps = 5;

  // Load percentage on querynode
  repeated int64 inMemory_percentages = 6;
}
```

## CreateAlias
Create an alias for a collection. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
For more information please refer the [document](https://wiki.lfaidata.foundation/display/MIL/MEP+10+--+Support+Collection+Alias).
```
rpc CreateAlias(CreateAliasRequest) returns (common.Status) {}
```
[back to top](#brief)

### CreateAliasRequest
```
message CreateAliasRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The target collection name. (Required)
  string collection_name = 3;

  // The alias. (Required)
  string alias = 4;
}
```

## DropAlias
Delete an alias. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
For more information please refer the [document](https://wiki.lfaidata.foundation/display/MIL/MEP+10+--+Support+Collection+Alias).
```
rpc DropAlias(DropAliasRequest) returns (common.Status) {}
```
[back to top](#brief)

### DropAliasRequest
```
message DropAliasRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The alias. (Required)
  string alias = 3;
}
```

## AlterAlias
Alter an alias from a collection to another. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
For more information please refer the [document](https://wiki.lfaidata.foundation/display/MIL/MEP+10+--+Support+Collection+Alias).
```
rpc AlterAlias(AlterAliasRequest) returns (common.Status) {}
```
[back to top](#brief)

### AlterAliasRequest
```
message AlterAliasRequest{
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The target collection name. (Required)
  string collection_name = 3;

  // The alias. (Required)
  string alias = 4;
}
```

## CreateIndex
Create an index. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc CreateIndex(CreateIndexRequest) returns (common.Status) {}
```
[back to top](#brief)

### CreateIndexRequest
```
message CreateIndexRequest {
  // Not useful for now
  common.MsgBase base = 1; 

  // Not useful for now
  string db_name = 2;

  // The particular collection name you want to create index. (Required)
  string collection_name = 3;

  // The vector field name in this particular collection. (Required)
  string field_name = 4;

  // Support keys: index_type, metric_type, params. Different index_type may has different params.
  repeated common.KeyValuePair extra_params = 5; 
}
```

## DescribeIndex
Get index information.
```
rpc DescribeIndex(DescribeIndexRequest) returns (DescribeIndexResponse) {}
```
[back to top](#brief)

### DescribeIndexRequest
```
message DescribeIndexRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The particular collection name in Milvus. (Required)
  string collection_name = 3;

  // The vector field name in this particular collection.  (Required)
  string field_name = 4;

  // No need to set up for now @2021.06.30
  string index_name = 5; 
}
```

### DescribeIndexResponse
```
message IndexDescription {
  // Index name
  string index_name = 1;

  // Index id
  int64 indexID = 2;

  // Will return index_type, metric_type, params(like nlist).
  repeated common.KeyValuePair params = 3;

  // The vector field name
  string field_name = 4;
}

message DescribeIndexResponse {
  // Response status
  common.Status status = 1;

  // All index informations, for now only return the latest index you created for the collection.
  repeated IndexDescription index_descriptions = 2;
}
```

## GetIndexState
Get index state .
```
rpc GetIndexState(GetIndexStateRequest) returns (GetIndexStateResponse) {}
```
[back to top](#brief)

### GetIndexStateRequest
```
message GetIndexStateRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2 ;
  
  // The target collection name. (Required)
  string collection_name = 3;

  // The target field name. (Required)
  string field_name = 4;

  // No need to set up for now @2021.06.30
  string index_name = 5;
}
```

### GetIndexStateResponse
See [common.IndexState](#indexstate)
```
message GetIndexStateResponse {
  // Response status
  common.Status status = 1;

  // Index state
  common.IndexState state = 2;

  // If the index build failed, the fail_reason will be returned.
  string fail_reason = 3;
}
```

## GetIndexBuildProgress
Get index build progress state.
```
rpc GetIndexBuildProgress(GetIndexBuildProgressRequest) returns (GetIndexBuildProgressResponse) {}
```
[back to top](#brief)

### GetIndexBuildProgressRequest
```
message GetIndexBuildProgressRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2 ;

  // The collection name in milvus
  string collection_name = 3;

  // The vector field name in this collection
  string field_name = 4;

  // Not useful for now
  string index_name = 5;
}
```

### GetIndexBuildProgressResponse
```
message GetIndexBuildProgressResponse {
  // Response status
  common.Status status = 1;

  // How many rows are indexed
  int64 indexed_rows = 2;

  // Totally how many rows need to be indexed
  int64 total_rows = 3;
}
```

## DropIndex
Drop an index. Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
```
rpc DropIndex(DropIndexRequest) returns (common.Status) {}
```
[back to top](#brief)

### DropIndexRequest
```
message DropIndexRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The target collection name. (Required)
  string collection_name = 3;

  // The target field name. (Required)
  string field_name = 4;

  // No need to set up for now @2021.06.30
  string index_name = 5;
}
```

## Insert
Insert entities into a collection. See [MutationResult](#mutationresult).
```
rpc Insert(InsertRequest) returns (MutationResult) {}
```
[back to top](#brief)

### InsertRequest
See [scheml.FieldData](#fielddata).
```
message InsertRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The target collection name. (Required)
  string collection_name = 3;

  // The target partition name. (Optional)
  string partition_name = 4;

  // The fields data. See schema.FieldData
  repeated schema.FieldData fields_data = 5;

  // Not useful for now
  repeated uint32 hash_keys = 6;

  // Row count inserted
  uint32 num_rows = 7;
}
```


## Delete
Delete entities from a collection by an expression. See [MutationResult](#mutationresult).
```
rpc Delete(DeleteRequest) returns (MutationResult) {}
```
[back to top](#brief)

### DeleteRequest
```
message DeleteRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The target collection name. (Required)
  string collection_name = 3;

  // The target partition name. (Optional)
  string partition_name = 4;

  // The expression to filter out entities. Currently only support primary key as filtering condition. For example: "id in [1, 2, 3]" (Required)
  string expr = 5;

  // Reserved
  repeated uint32 hash_keys = 6;
}
```

## Search
Do ANN search.
```
rpc Search(SearchRequest) returns (SearchResults) {}
```
[back to top](#brief)

### SearchRequest
```
message SearchRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // The target collection name. (Required)
  string collection_name = 3;

  // The target partition names. (Optional)
  repeated string partition_names = 4;

  // The filtering expression. (Optional)
  string dsl = 5;

  // Serialized `PlaceholderGroup`
  bytes placeholder_group = 6;

  // Always be BoolExprV1
  common.DslType dsl_type = 7;

  // Specifiy output fields, the returned result will return other fields data. (Optional) 
  repeated string output_fields = 8;

  // ANN search parameters. For example, if the index is IVF, the search parameters will be: "nprobe":xxx
  repeated common.KeyValuePair search_params = 9;

  // Filtering entities by timestamp. (Optional)
  uint64 travel_timestamp = 10;

  // The time tolerance between entities visibility and search action. Default is 0. In Milvus, each entity has a timestamp. To ensure data consistence, each node(query node and data node) will consume data in a time interval. So entity visibility is a bit later than its timestamp. If this value is 0, Milvus will hold the search action, wait until all entities whose timestamp is earlier that the search action's timestamp to be fully consumed.
  uint64 guarantee_timestamp = 11;
}
```

### SearchResults
See [schema.SearchResultData](#searchresultdata).
```
message SearchResults {
  // Response status
  common.Status status = 1;

  // Search results
  schema.SearchResultData results = 2;
}
```

## Flush
Flush data node's buffer into storage. After flush, all growing segments become sealed, and persisted into storage asynchronously.
```
rpc Flush(FlushRequest) returns (FlushResponse) {}
```
[back to top](#brief)

### FlushRequest
```
message FlushRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // Specify target collection names. (Required)
  repeated string collection_names = 3;
}
```

### FlushResponse
```
message FlushResponse{
  // Not useful for now
  common.Status status = 1;

  // Not useful for now
  string db_name = 2;

  // Return a map mapping collection id and flushed segments.
  map<string, schema.LongArray> coll_segIDs = 3;
}
```

## Query
Retrieve entities by expression.
```
rpc Query(QueryRequest) returns (QueryResults) {}
```
[back to top](#brief)

### QueryRequest
```
message QueryRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string db_name = 2;

  // Target collection name. (Required)
  string collection_name = 3;

  // Filtering expression. (Required) For more information: https://milvus.io/docs/v2.0.0/boolean.md
  string expr = 4;

  // Specifiy output fields, the returned result will return other fields data. (Optional) 
  repeated string output_fields = 5;

  // Target partition names. (Optional)
  repeated string partition_names = 6;

  // Filtering entities by timestamp. (Optional)
  uint64 travel_timestamp = 7;

  // The time tolerance between entities visibility and query action. Default is 0. 
  uint64 guarantee_timestamp = 8; // guarantee_timestamp
}
```

### QueryResults
See [schema.FieldData](#fielddata).
```
message QueryResults {
  // Response status
  common.Status status = 1;

  // See schema.FieldData
  repeated schema.FieldData fields_data = 2;
}
```

## CalcDistance
Calculate distance between two vector arrays.
```
rpc CalcDistance(CalcDistanceRequest) returns (CalcDistanceResults) {}
```
[back to top](#brief)

### VectorIDs
```
message VectorIDs {
  // Target collection name. (Required)
  string collection_name = 1;

  // Target field name. (Required)
  string field_name = 2;

  // Specify id array. See schema.IDs
  schema.IDs id_array = 3;
  repeated string partition_names = 4;
}
```

### VectorsArray
See [VectorIDs](#vectorids), [schema.VectorField](#vectorfield).
```
message VectorsArray {
  // Specify vectors by long id, or input vectors data as input
  // See schema.VectorField
  oneof array {
    VectorIDs id_array = 1; // vector ids
    schema.VectorField data_array = 2; // vectors data
  } 
}
```

### CalcDistanceRequest
See [VectorsArray](#vectorsarray). See [schema.KeyValuePair](#keyvaluepair).
```
message CalcDistanceRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // vectors on the left of operator
  VectorsArray op_left = 2;

  // vectors on the right of operator
  VectorsArray op_right = 3; 

  // Extra parameters in key-value format. Currently only support metric type: "metric":"L2"/"IP"/"HAMMIN"/"TANIMOTO"
  repeated common.KeyValuePair params = 4; 
}
```

### CalcDistanceResults
See [schema.IntArray](#intarray). See [schema.FloatArray](#floatarray).
```
message CalcDistanceResults {
  // Response status
  common.Status status = 1;

  // Distance results array. The array size = num(op_left)*num(op_right)
  // For float vectors, return float distance values.
  // For binary vectors, return integer distance values.
  // For example: 
  //    Assume the vectors_left: L_1, L_2, L_3
  //    Assume the vectors_right: R_a, R_b
  //    Distance between L_n and R_m we called "D_n_m"
  //    The returned distance values are arranged like this:
  //      [D_1_a, D_1_b, D_2_a, D_2_b, D_3_a, D_3_b]
  oneof array {
    schema.IntArray int_dist = 2;
    schema.FloatArray float_dist = 3;
  }
}
```

## GetPersistentSegmentInfo
Get segment persist state from data node.
```
rpc GetPersistentSegmentInfo(GetPersistentSegmentInfoRequest) returns (GetPersistentSegmentInfoResponse) {}
```
[back to top](#brief)

### GetPersistentSegmentInfoRequest
```
message GetPersistentSegmentInfoRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string dbName = 2;

  // Target collection's name. (Required)
  string collectionName = 3;
}
```

### PersistentSegmentInfo
See [common.SegmentState](#segmentstate).
```
message PersistentSegmentInfo {
  // The segment's ID
  int64 segmentID = 1;

  // The collection ID to which the segment belong
  int64 collectionID = 2;

  // The partition ID to which the segment belong
  int64 partitionID = 3;

  // The segment's row count
  int64 num_rows = 4;

  // See common.SegmentState
  common.SegmentState state = 5;
}
```

### GetPersistentSegmentInfoResponse
See [PersistentSegmentInfo](#persistentsegmentinfo).
```
message GetPersistentSegmentInfoResponse {
  // Response status
  common.Status status = 1;

  // Array of multiple segment info. See PersistentSegmentInfo
  repeated PersistentSegmentInfo infos = 2;
}
```

## GetQuerySegmentInfo
Get segments information from query nodes.
```
rpc GetQuerySegmentInfo(GetQuerySegmentInfoRequest) returns (GetQuerySegmentInfoResponse) {}
```
[back to top](#brief)

### GetQuerySegmentInfoRequest
```
message GetQuerySegmentInfoRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Not useful for now
  string dbName = 2;

  // Target collection's name. (Required)
  string collectionName = 3;
}
```

### QuerySegmentInfo
See [common.SegmentState](#segmentstate).
```
message QuerySegmentInfo {
  // The segment id
  int64 segmentID = 1;

  // The collection id to which the segment belong
  int64 collectionID = 2;

  // The partition id to which the segment belong
  int64 partitionID = 3;

  // Memory size cost for the segment
  int64 mem_size = 4;

  // Row count of the segment
  int64 num_rows = 5;

  // The index name of the segment
  string index_name = 6;

  // Index id, mostly for developers
  int64 indexID = 7;

  // The query node id which the segment is loaded
  int64 nodeID = 8;

  // The segment state, see common.SegmentState
  common.SegmentState state = 9;
}
```

### GetQuerySegmentInfoResponse
See [QuerySegmentInfo](#querysegmentinfo).
```
message GetQuerySegmentInfoResponse {
  // Response status
  common.Status status = 1;

  // Array of QuerySegmentInfo. See QuerySegmentInfo.
  repeated QuerySegmentInfo infos = 2;
}
```

## GetMetrics
Get server runtime statistics. For more information refer the [document](https://wiki.lfaidata.foundation/display/MIL/MEP+8+--+Add+metrics+for+proxy).
```
rpc GetMetrics(GetMetricsRequest) returns (GetMetricsResponse) {}
```
[back to top](#brief)

### GetMetricsRequest
```
message GetMetricsRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Request in json format
  string request = 2;
}
```

### GetMetricsResponse
```
message GetMetricsResponse {
  // Response status
  common.Status status = 1;

  // Response in json format
  string response = 2;

  // Metrics from which component
  string component_name = 3;
}
```

## LoadBalance
Handoff segments from one query node to others. Client can use GetPersistentSegmentInfo() to know which segment loaded on which query node, then use LoadBalance() to reassign them into different query node.
Return a `common.Status`(see [common.Status](#status)) to tell client whether the operation is successful.
For more information, refer this [document](https://wiki.lfaidata.foundation/display/MIL/MEP+17+--+Support+handoff+and+load+balance+segment+on+query+nodes).
```
rpc LoadBalance(LoadBalanceRequest) returns (common.Status) {}
```
[back to top](#brief)

### LoadBalanceRequest
```
message LoadBalanceRequest {
  // Not useful for now
  common.MsgBase base = 1;

  // Source query node id. (Required)
  int64 src_nodeID = 2;

  // Target query node id array. (Optional)
  // If this field is not specified, system will automatically balance load these segments.
  repeated int64 dst_nodeIDs = 3;

  // Segment id array to be handoff. (Required)
  repeated int64 sealed_segmentIDs = 4;
}
```

## GetCompactionState
Get a compaction action state.
```
rpc GetCompactionState(GetCompactionStateRequest) returns (GetCompactionStateResponse) {}
```
[back to top](#brief)

### GetCompactionStateRequest
```
message GetCompactionStateRequest {
  // The compaction id which is returned by ManualCompaction().
  int64 compactionID = 1;
}
```

### GetCompactionStateResponse
See [common.CompactionState](#compactionstate)
```
message GetCompactionStateResponse {
  // Response status
  common.Status status = 1;

  // Compaction action state. See common.CompactionState.
  common.CompactionState state = 2;

  // The runtime plan number which is in progress
  int64 executingPlanNo = 3;

  // The time out plan number
  int64 timeoutPlanNo = 4;

  // The completed plan number
  int64 completedPlanNo = 5;
}
```

## ManualCompaction
Manually trigger compaction action.
```
rpc ManualCompaction(ManualCompactionRequest) returns (ManualCompactionResponse) {}
```
[back to top](#brief)

### ManualCompactionRequest
```
message ManualCompactionRequest {
  // Target collection id
  int64 collectionID = 1;

  // Setup a timetravel range, segment blob between the range will be compacted
  uint64 timetravel = 2;
}
```

### ManualCompactionResponse
```
message ManualCompactionResponse {
  // Response status
  common.Status status = 1;

  // Compaction action id
  int64 compactionID = 2;
}
```

## GetCompactionStateWithPlans
Get runtime plans/state for compaction action.
```
rpc GetCompactionStateWithPlans(GetCompactionPlansRequest) returns (GetCompactionPlansResponse) {}
```
[back to top](#brief)

### GetCompactionPlansRequest
```
message GetCompactionPlansRequest {
  // The compaction action id
  int64 compactionID = 1;
}
```

### GetCompactionPlansResponse
See [common.CompactionState](#compactionstate).
See [CompactionMergeInfo](#compactionmergeinfo).
```
message GetCompactionPlansResponse {
  // Response status
  common.Status status = 1;

  // See common.CompactionState
  common.CompactionState state = 2;

  // The runtime plans of the compaction action. See CompactionMergeInfo
  repeated CompactionMergeInfo mergeInfos = 3;
}
```

### CompactionMergeInfo
```
message CompactionMergeInfo {
  // Segment id array to be merged
  repeated int64 sources = 1;

  // New generated segment id after merging
  int64 target = 2;
}
```

## ShowType
Used by [ShowCollections()]](#showcollections) and [ShowPartitions()](#showpartitions).
```
enum ShowType {
  // Will return all colloections/partitions
  All = 0;

  // Will return loaded collections/partitions with their inMemory_percentages
  InMemory = 1;
}
```
[back to top](#brief)

## BoolResponse
Used by [HasCollection()](#hascollection) and [HasPartition()](#haspartition).
```
message BoolResponse {
  // Response status
  common.Status status = 1;

  // bool value
  bool value = 2;
}
```
[back to top](#brief)

## MutationResult
Used by [Insert()](#insert) and [Delete()](#delete).
```
message MutationResult {
  // Response status
  common.Status status = 1;

  // For insert, return the ids of inserted entities.
  // For delete, return the ids of input to be deleted.
  schema.IDs IDs = 2; 

  // Reserved. Indicate succeed indexes.
  repeated uint32 succ_index = 3; 

  // Reserved. Indicate error indexes.
  repeated uint32 err_index = 4;

  // Reserved.
  bool acknowledged = 5;

  // Reserved.
  int64 insert_cnt = 6;

  // Reserved.
  int64 delete_cnt = 7;

  // Reserved.
  int64 upsert_cnt = 8;

  // Reserved.
  uint64 timestamp = 9;
}
```
[back to top](#brief)

# common.proto

## ErrorCode
Error code retrurned by server.
```
enum ErrorCode {
    Success = 0;
    UnexpectedError = 1;
    ConnectFailed = 2;
    PermissionDenied = 3;
    CollectionNotExists = 4;
    IllegalArgument = 5;
    IllegalDimension = 7;
    IllegalIndexType = 8;
    IllegalCollectionName = 9;
    IllegalTOPK = 10;
    IllegalRowRecord = 11;
    IllegalVectorID = 12;
    IllegalSearchResult = 13;
    FileNotFound = 14;
    MetaFailed = 15;
    CacheFailed = 16;
    CannotCreateFolder = 17;
    CannotCreateFile = 18;
    CannotDeleteFolder = 19;
    CannotDeleteFile = 20;
    BuildIndexError = 21;
    IllegalNLIST = 22;
    IllegalMetricType = 23;
    OutOfMemory = 24;
    IndexNotExist = 25;
    EmptyCollection = 26;

    // internal error code.
    DDRequestRace = 1000;
}
```
[back to top](#brief)

## IndexState
Used by [GetIndexState()](#getindexstate).
```
enum IndexState {
    IndexStateNone = 0;

    // Build index not yet assigned or begin
    Unissued = 1;

    // Build index in progress
    InProgress = 2;

    // Build index finished
    Finished = 3;

    // Build index failed
    Failed = 4; 
}
```
[back to top](#brief)

## SegmentState
Used by [GetPersistentSegmentInfo()](#getpersistentsegmentinfo) and [GetQuerySegmentInfo()](#getquerysegmentinfo).
```
enum SegmentState {
    SegmentStateNone = 0;

    // Segment doesn't exist
    NotExist = 1;

    // Segment is in buffer, waiting for insert, not yet persisted
    Growing = 2;

    // Segment is in buffer, but is sealed, not allow insert
    Sealed = 3;

    // Segment was persisted into storage
    Flushed = 4;

    // Segment is to be flushed into storage
    Flushing = 5;

    // Segment has been marked as deleted
    Dropped = 6;
}
```
[back to top](#brief)

## Status
Used by some interfaces to return server error code and message.
See [ErrorCode](#errorcode).
```
message Status {
  // See ErrorCode
  ErrorCode error_code = 1;
  
  // Fail reason if the error_code is not Success
  string reason = 2;
}
```
[back to top](#brief)

## KeyValuePair
Pass key(string)-value(string) paremeters to server, for example "nlist":"1024".
```
message KeyValuePair {
    string key = 1;
    string value = 2;
}
```
[back to top](#brief)

## KeyDataPair
Pass key(string)-value(binary data) paremeters to server.
```
message KeyDataPair {
    string key = 1;
    bytes data = 2;
}
```
[back to top](#brief)

## DslType
Used to support DSL(Domain Specific Language). But now only support expression. Now in [SearchRequest](#searchrequest) the `DslType` always be `BoolExprV1`.
```
enum DslType {
    Dsl = 0;
    BoolExprV1 = 1;
}
```
[back to top](#brief)

## CompactionState
Used by [GetCompactionState()](#getcompactionstate) to get compaction state.
```
enum CompactionState {
  UndefiedState = 0;

  // Compaction action is in progress
  Executing = 1;

  // Compaction action completed
  Completed = 2;
}
```
[back to top](#brief)

# schema.proto

## DataType
To define field data type.
```
enum DataType {
  None = 0;
  Bool = 1;
  Int8 = 2;
  Int16 = 3;
  Int32 = 4;
  Int64 = 5;

  Float = 10;
  Double = 11;

  // Not avaiable in v2.0, plan to support in 2.1
  String = 20;

  // Binary vector, each dimension is one bit
  BinaryVector = 100;

  // Float vector, each dimension is a float
  FloatVector = 101;
}
```
[back to top](#brief)

## FieldSchema
To define field schema. Used by [CreateCollection()](#createcollection).
```
message FieldSchema {
  // Field unique id, typically sdk ought to automatically set this id for user.
  int64 fieldID = 1;

  // Field name. (Required)
  string name = 2; 

  // Is primary key or not, each collection only has one primary key field. Currently only support int64 type field as primary key. Once string type is supported, string type field also can be primary key. Default is false.
  bool is_primary_key = 3;

  // Field description. (Optional)
  string description = 4;

  // Field data type, see DataType.
  DataType data_type = 5;

  // Extra parameters. Reserved.  
  repeated common.KeyValuePair type_params = 6;

  // Deprecated
  repeated common.KeyValuePair index_params = 7;

  // When it is true, Milvus will automatically generate id for each entity. Only primary key field can set autoID as true. Default is false.  
  bool autoID = 8;          
}
```
[back to top](#brief)

## CollectionSchema
To define collection schema. Used by [CreateCollection()](#createcollection).
```
message CollectionSchema {
  // Collection name. (Required)
  string name = 1;

  // Collection description. (Optional)
  string description = 2;

  // Deprecated
  bool autoID = 3;

  // Fields schema, see FieldSchema. (Required)
  repeated FieldSchema fields = 4;
}
```
[back to top](#brief)

## BoolArray
Bool values array.
```
message BoolArray {
  repeated bool data = 1;
}
```
[back to top](#brief)

## IntArray
Integer values array.
```
message IntArray {
  repeated int32 data = 1;
}
```
[back to top](#brief)

## LongArray
Long values array.
```
message LongArray {
  repeated int64 data = 1;
}
```
[back to top](#brief)

## FloatArray
Float values array.
```
message FloatArray {
  repeated float data = 1;
}
```
[back to top](#brief)

## DoubleArray
Double values array.
```
message DoubleArray {
  repeated double data = 1;
}
```
[back to top](#brief)

## BytesArray
Byte values array. For special fields such as binary vector.
Note: each bit is one dimension.
```
message BytesArray {
  repeated bytes data = 1;
}
```
[back to top](#brief)

## StringArray
String values array. String type is planned in v2.1.
```
message StringArray {
  repeated string data = 1;
}
```
[back to top](#brief)

## ScalarField
Store data for non-vector field.
```
message ScalarField {
  oneof data {
    BoolArray bool_data = 1;
    IntArray int_data = 2;
    LongArray long_data = 3;
    FloatArray float_data = 4;
    DoubleArray double_data = 5;
    StringArray string_data = 6;
    BytesArray bytes_data = 7;
  }
}
```
[back to top](#brief)

## VectorField
Store data for vector field.
```
message VectorField {
  // Vector dimension
  int64 dim = 1;

  oneof data {
    // For float vectors. The array size is vector_count*dimension.
    FloatArray float_vector = 2;

    // For binary vectors. The array size is vector_count*dimension/8.
    bytes binary_vector = 3;
  }
}
```
[back to top](#brief)

## FieldData
Represent a field. Can be vector field or scalar field. Used by [Insert()](#insert).
```
message FieldData {
  // Field data type
  DataType type = 1;

  // Field name. (Required)
  string field_name = 2;

  oneof field {
    // For scalar field
    ScalarField scalars = 3;

    // For vector field
    VectorField vectors = 4;
  }
  // Deprecated
  int64 field_id = 5;
}
```
[back to top](#brief)

## IDs
Represent an array of ID. Used by [Insert()](#insert)/[Delete()](#delete)/[Query()](#query), return id array.
```
message IDs {
  oneof id_field {
    // Long id array
    LongArray int_id = 1;

    // String id array. String id is planned in v2.1.
    StringArray str_id = 2;
  }
}
```
[back to top](#brief)

## SearchResultData
Used by [Search()](#search), to return search results.
```
message SearchResultData {
  // Target vectors count
  int64 num_queries = 1;

  // Specified search topK      
  int64 top_k = 2;

  // Return field data specified by output field names. See Search()
  repeated FieldData fields_data = 3;

  // TopK scores array. The array size is num_queries*topK.
  repeated float scores = 4;

  // TopK ids array. The array size is num_queries*topK.
  IDs ids = 5;

  // Sometimes not all target vectors return same topK result. The topks will tell client the topK of each target vector.
  repeated int64 topks = 6;  
}
```
[back to top](#brief)
