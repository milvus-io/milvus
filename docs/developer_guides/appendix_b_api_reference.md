

## Appendix B. API Reference

In this section, we introduce the RPCs of milvus service. A brief description of the RPCs is listed as follows.

| RPC                | description                                                  |
| :----------------- | ------------------------------------------------------------ |
| CreateCollection        | create a collection base on schema statement                 |
| DropCollection          | drop a collection                                            |
| HasCollection           | whether or not a collection exists                           |
| LoadCollection          | load collection to memory for future search                  |
| ReleaseCollection       | release the memory the collection memory                     |
| DescribeCollection      | show a collection's schema and its descriptive statistics    |
| GetCollectionStatistics | show a collection's statistics                               |
| ShowCollections         | list all collections                                         |
| CreatePartition         | create a partition                                           |
| DropPartition           | drop a partition                                             |
| HasPartition            | whether or not a partition exists                            |
| LoadPartition           | load collection to memory for future search                  |
| ReleasePartition        | release the memory the collection memory                     |
| GetPartitionStatistics  | show a collection's statistics                               |
| ShowPartitions          | list a collection's all partitions                           |
| CreateIndex             | create index for a field in the collection                       |
| DescribeIndex           | get index details for a field in the collection                |
| GetIndexStates          | get build index state                                        |
| DropIndex               | drop a specific index for a field in the collection            |
| Insert                  | insert a batch of rows into a collection or a partition      |
| Search                  | query the columns of a collection or a partition with ANNS statements and boolean expressions |
| Flush                   | Perform persistent storage of data in memory                 |

**MsgBase** is a base struct in each request.

```protobuf
message MsgBase {
  MsgType msg_type = 1;
  int64  msgID = 2;
  uint64 timestamp = 3;
  int64 sourceID = 4;
}
```

**MsgType** is the enum to distingush diffrent message types in message queue, such as insert msg, search msg, etc. **msgID** is the unique id identifier of message. **timestamp** is the time when this message was generated. **sourceID** is the unique id identifier of the source.



#### 3.1 Definition Requests

###### 3.1.1 CreateCollection

**Interface:**

```
rpc CreateCollection(CreateCollectionRequest) returns (common.Status){}
```

**Description:**

Create a collection through CreateCollectionRequest.

**Parameters:**

- **CreateCollectionRequest**

CreateCollectionRequest struct is shown as follows:

```protobuf
message CreateCollectionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  // `schema` is the serialized `schema.CollectionSchema`
  bytes schema = 4;
}

message CollectionSchema {
  string name = 1;
  string description = 2;
  bool autoID = 3;
  repeated FieldSchema fields = 4;
}
```

CreateCollectionRequest contains **MsgBase**, **db_name**, **collection_name** and serialized collection schema **schema**. **db_name** contains only a string named **collection_name**. Collection with the same collection_name is going to be created.

Collection schema contains all the base information of a collection including **collection name**, **description**, **autoID** and **fields**. Collection description is defined by database manager to describe the collection. **autoID** determines whether the ID of each row of data is user-defined. If **autoID** is true, our system will generate a unique ID for each data. If **autoID** is false, user need to give each entity a ID when insert.

**Fields** is a list of **FieldSchema**. Each schema should include Field **name**, **description**, **dataType**, **type_params** and **index_params**.

FieldSchema struct is shown as follows:

```protobuf
message FieldSchema {
  int64 fieldID = 1;
  string name = 2;
  bool is_primary_key = 3;
  string description = 4;
  DataType data_type = 5;
  repeated common.KeyValuePair type_params = 6;
  repeated common.KeyValuePair index_params = 7;
}
```

**Field schema** contains all the base information of a field including **fieldID**, **name**, **description**, **data_type**, **type_params** and **index_params**. **data_type** is an enum type to distingush different data types.Total enum is shown in the last of this doc

**type_params** contains the detailed information of data_type. For example, vector data type should include dimension information. You can give a pair of <dim, 8> to let the field store 8-dimension vector.

**index_params**：For fast search, you build index for field. You specify detailed index information for a field. Detailed information about index can be seen in chapter 2.2.3



**Returns:**

- **common.Status**

```protobuf
message Status {
ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contain grpc error but contains the server error code. We can get the executing result in common status. **error_code** is an enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describe the detailed error.



###### 3.1.2 DropCollection

**Interface:**

```
rpc DropCollection(DropCollectionRequest) returns (common.Status) {}
```

**Description:**

This method is used to delete collection.

**Parameters:**

- **DropCollectionRequest**

DropCollectionRequest struct is shown as follows:

```protobuf
message DropCollectionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
}
```

Collection with the same **collection_name** is going to be deleted.

**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contain grpc error but contains the server error code. We can get the executing result in common status. **error_code** is an enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.3 HasCollection

**Interface:**

```
rpc HasCollection(HasCollectionRequest) returns (BoolResponse) {}
```

**Description:**

This method is used to test collection existence.

**Parameters:**

- **HasCollectionRequest**

HasCollectionRequest struct is shown as follows:

```protobuf
message HasCollectionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
}
```

The server finds the collection through **collection_name** and judge whether the collection exists.

**Returns:**

- **BoolResponse**

```protobuf
message BoolResponse {
  common.Status status = 1;
  bool value = 2;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**value** represents whether the collection exists. If collection exists, value will be true. If collection doesn't exist, value will be false.



###### 3.1.4 LoadCollection

**Interface:**

```
rpc LoadCollection(LoadCollectionRequest) returns (common.Status) {}
```

**Description:**

This method is used to load collection.

**Parameters:**

- **LoadCollectionRequest**

LoadCollectionRequest struct is shown as follows:

```protobuf
message LoadCollectionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
}
```

Collection with the same **collection_name** is going to be loaded to memory.

**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.5 ReleaseCollection

**Interface:**

```
rpc ReleaseCollection(ReleaseCollectionRequest) returns (common.Status) {}
```

**Description:**

This method is used to release collection.

**Parameters:**

- **ReleaseCollectionRequest**

ReleaseCollectionRequest struct is shown as follows:

```protobuf
message ReleaseCollectionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
}
```

Collection with the same **collection_name** is going to be released from memory.

**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

###### 3.1.6 DescribeCollection

**Interface:**

```
 rpc DescribeCollection(DescribeCollectionRequest) returns (CollectionDescription) {}
```

**Description:**

This method is used to get collection schema.

**Parameters:**

- **DescribeCollectionRequest**

DescribeCollectionRequest struct is shown as follows:

```protobuf
message DescribeCollectionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  int64 collectionID = 4;
}
```

The server finds the collection through **collection_name** and get detailed collection information. And **collectionID** is for internel component to get collection details.

**Returns:**

- **DescribeCollectionResponse**

```protobuf
message DescribeCollectionResponse {
  common.Status status = 1;
  schema.CollectionSchema schema = 2;
  int64 collectionID = 3;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**schema** is collection schema same as the collection schema in [CreateCollection](#311-createcollection).

###### 3.1.7 GetCollectionStatistics

**Interface:**

```
 rpc GetCollectionStatistics(GetCollectionStatisticsRequest) returns (GetCollectionStatisticsResponse) {}
```

**Description:**

This method is used to get collection statistics.

**Parameters:**

- **GetCollectionStatisticsRequest**

GetCollectionStatisticsRequest struct is shown as follows:

```protobuf
message GetCollectionStatisticsRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
}
```

The server finds the collection through **collection_name** and get detailed collection statistics.

**Returns:**

- **GetCollectionStatisticsResponse**

```protobuf
message GetCollectionStatisticsResponse {
  common.Status status = 1;
  repeated common.KeyValuePair stats = 2;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**stats** is a map saving diffrent statistics. For example, you can get row_count of a collection with key 'row_count'.



###### 3.1.8 ShowCollections

**Interface:**

```
rpc ShowCollections(ShowCollectionsRequest) returns (ShowCollectionsResponse) {}
```

**Description:**

This method is used to get collection schema.

**Parameters:** None

**Returns:**

- **ShowCollectionsResponse**

```protobuf
message ShowCollectionsResponse {
  common.Status status = 1;
  repeated string collection_names = 2;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**collection_names**  is a list contains all collections' name.



###### 3.1.9 CreatePartition

**Interface:**

```
rpc CreatePartition(CreatePartitionRequest) returns (common.Status) {}
```

**Description:**

This method is used to create partition

**Parameters:**

- **CreatePartitionRequest**

CreatePartitionRequest struct is shown as follows:

```protobuf
message CreatePartitionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string partition_name = 4;
}
```

The server creates partition with the **partition_name** in collection with name of **collection_name**

- **Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.10 DropPartition

**Interface:**

```
rpc DropPartition(DropPartitionRequest) returns (common.Status) {}
```

**Description:**

This method is used to drop partition.

**Parameters:**

- **DropPartitionRequest**

DropPartitionRequest struct is shown as follows:

```protobuf
message DropPartitionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string partition_name = 4;
}
```

Drop partition with the same **partition_name** in collection with **collection_name** is going to be deleted.

**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.11 HasPartition

**Interface:**

```
rpc HasPartition(HasPartitionRequest) returns (BoolResponse) {}
```

**Description:**

This method is used to test partition existence.

**Parameters:**

- **HasPartitionRequest**

HasPartitionRequest struct is shown as follows:

```protobuf
message HasPartitionRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string partition_name = 4;
}
```

Partition with the same **partition_name** is going to be tested whether it is in collection with **collection_name**.

**Returns:**

- **BoolResponse**

```protobuf
message BoolResponse {
  common.Status status = 1;
  bool value = 2;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**value** represents whether the partition exists. If partition exists, value will be true. If partition doesn't exist, value will be false.


###### 3.1.12 LoadPartitions

**Interface:**

```
rpc LoadPartitions(LoadPartitionsRequest) returns (common.Status) {}
```

**Description:**

This method is used to load collection.

**Parameters:**

- **LoadPartitionsRequest**

LoadPartitionsRequest struct is shown as follows:

```protobuf
message LoadPartitionsRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  repeated string partition_names = 4;
}
```

**parition_names** is a list of parition_name. These partitions in collection with the **collection_name** is going to be loaded to memory.

**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

###### 3.1.13 ReleasePartitions

**Interface:**

```
rpc ReleasePartitions(ReleasePartitionsRequest) returns (common.Status) {}
```

**Description:**

This method is used to release partition.

**Parameters:**

- **ReleasePartitionsRequest**

ReleasePartitionsRequest struct is shown as follows:

```protobuf
message ReleasePartitionsRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  repeated string partition_names = 4;
}
```

**parition_names** is a list of parition_name. These partitions in collection with the **collection_name** is going to be released from memory.

**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

###### 3.1.14 GetPartitionStatistics

**Interface:**

```
 rpc GetPartitionStatistics(GetPartitionStatisticsRequest) returns (GetPartitionStatisticsResponse) {}
```

**Description:**

This method is used to get partition statistics.

**Parameters:**

- **GetPartitionStatisticsRequest**

GetPartitionStatisticsRequest struct is shown as follows:

```protobuf
message GetPartitionStatisticsRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string partition_name = 4;
}
```

The server finds the partition through **partition_name** in collection with **collection_name** and get detailed partition statistics.

**Returns:**

- **GetCollectionStatisticsResponse**

```protobuf
message GetPartitionStatisticsResponse {
  common.Status status = 1;
  repeated common.KeyValuePair stats = 2;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**stats** is a map saving diffrent statistics. For example, you can get row_count of a partition with key 'row_count'.

###### 3.1.15 ShowPartitions

**Interface:**

```
rpc ShowPartitions(ShowPartitionsRequest) returns (StringListResponse) {}
```

**Description:**

This method is used to get partition description.

**Parameters:**

- **ShowPartitionsRequest**

ShowPartitionsRequest struct is shown as follows:

```protobuf
message ShowPartitionsRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  int64 collectionID = 4;
}
```

Partitions in the collection with **collection_name** is going to be listed.

**Returns:**

- **StringListResponse**

```protobuf
message ShowPartitionsResponse {
  common.Status status = 1;
  repeated string partition_names = 2;
  repeated int64 partitionIDs = 3;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**partition_names**  is a list contains all partitions' name.
**partitionIDs**  is a list contains all partitions' ids. And the index of a parition in **partition_names** and **partitionIDs** are same.


#### 3.2 Manipulation Requsts

###### 3.2.1 Insert


**Interface:**

```
rpc Insert(InsertRequest) returns (InsertResponse){}
```

**Description:**

Insert a batch of rows into a collection or a partition

**Parameters:**

- **InsertRequest**

InsertRequest struct is shown as follows:

```protobuf
message InsertRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string partition_name = 4;
  repeated common.Blob row_data = 5;
  repeated uint32 hash_keys = 6;
}

message Blob {
  bytes value = 1;
}
```

Insert a batch of **row_data** into collection with **collection_name** and partition with **partition_name**. Blob contains bytes of value.

**Returns:**

- **common.Status**

```protobuf
message InsertResponse {
  common.Status status = 1;
  int64 rowID_begin = 2;
  int64 rowID_end = 3;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**rowID_begin** and **rowID_end** are the ID of inserted values.

###### 3.2.2 Delete

* DeleteByID



#### 3.3 Query


#### 3.3 Index
###### 3.3.1 CreateIndex

**Interface:**

```
rpc CreateIndex(CreateIndexRequest) returns (common.Status){}
```

**Description:**

Create a index for a collection.

**Parameters:**

- **CreateIndexRequest**

CreateIndexRequest struct is shown as follows:

```protobuf
message CreateIndexRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string field_name = 4;
  repeated common.KeyValuePair extra_params = 5;
}
```

CreateIndex for the field with **field_name** in collection with **collection_name**.

**extra_params**：For fast search, you build index for field. You specify detailed index information for a field. Detailed information about index can be seen in chapter 2.2.3



**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

###### 3.3.2 DescribeIndex

**Interface:**

```
rpc DescribeIndex(DescribeIndexRequest) returns (common.Status){}
```

**Description:**

Get a index detailed info

**Parameters:**

- **DescribeIndexRequest**

DescribeIndexRequest struct is shown as follows:

```protobuf
message DescribeIndexRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string field_name = 4;
  string index_name = 5;
}
```

Get a index details for the field with **field_name** in collection with **collection_name**.

**index_name**： A field can create multiple indexes. And you can drop specific index through index_name.

**Returns:**

- **common.Status**

```protobuf
message DescribeIndexResponse {
  common.Status status = 1;
  repeated IndexDescription index_descriptions = 2;
}

message IndexDescription {
  string index_name = 1;
  int64 indexID = 2;
  repeated common.KeyValuePair params = 3;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**index_descriptions** is a list of index descriptions. If index_name is specific in request, the list length will be 0. Otherwise if index_name is empty, the response will return all index in the field of a collection.

**params**：For fast search, you build index for field. You specify detailed index information for a field. Detailed information about index can be seen in chapter 2.2.3

###### 3.3.3 GetIndexStates

**Interface:**

```
rpc GetIndexStates(GetIndexStatesRequest) returns (GetIndexStatesRequest){}
```

**Description:**

Get a index build progress info.

**Parameters:**

- **GetIndexStatesRequest**

GetIndexStatesRequest struct is shown as follows:

```protobuf
message GetIndexStatesRequest {
  common.MsgBase base = 1;
  string db_name = 2 ;
  string collection_name = 3;
  string field_name = 4;
  string index_name = 5;
}
```

Get a index build progress info for the field with **field_name** in collection with **collection_name**.

**index_name**： A field can create multiple indexes. And you can get specific index state through index_name.

**Returns:**

- **common.Status**

```protobuf
message GetIndexStatesResponse {
  common.Status status = 1;
  common.IndexState state = 2;
}

enum IndexState {
  IndexStateNone = 0;
  Unissued = 1;
  InProgress = 2;
  Finished = 3;
  Failed = 4;
  Deleted = 5;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**index state** is a enum type to distinguish the different processes in the index building process.


###### 3.3.4 DropIndex

**Interface:**

```
rpc DropIndex(DropIndexRequest) returns (common.Status){}
```

**Description:**

Drop a index for a collection.

**Parameters:**

- **DropIndexRequest**

DropIndexRequest struct is shown as follows:

```protobuf
message DropIndexRequest {
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string field_name = 4;
  string index_name = 5;
}
```

DropIndex for the field with **field_name** in collection with **collection_name**.

**index_name**： A field can create multiple indexes. And you can drop specific index through index_name.

**Returns:**

- **common.Status**

```protobuf
message Status {
  ErrorCode error_code = 1;
  string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

