

## Appendix B. API Reference

In this section, we introduce the RPCs of milvus service. A brief description of the RPCs is listed as follows.

| RPC                | description                                                  |
| :----------------- | ------------------------------------------------------------ |
| CreateCollection   | create a collection base on schema statement                 |
| DropCollection     | drop a collection                                            |
| HasCollection      | whether or not a collection exists                           |
| DescribeCollection | show a collection's schema and its descriptive statistics    |
| ShowCollections    | list all collections                                         |
| CreatePartition    | create a partition                                           |
| DropPartition      | drop a partition                                             |
| HasPartition       | whether or not a partition exists                            |
| DescribePartition  | show a partition's name and its descriptive statistics       |
| ShowPartitions     | list a collection's all partitions                           |
| Insert             | insert a batch of rows into a collection or a partition      |
| Search             | query the columns of a collection or a partition with ANNS statements and boolean expressions |



#### 3.1 Definition Requests

###### 3.1.1 CreateCollection

**Interface:**

```
rpc CreateCollection(schema.CollectionSchema) returns (common.Status){}
```

**Description:**

Create a collection through collection schema.

**Parameters:** 

- **schema.CollectionSchema**

CollectionSchema  struct is shown as follows:

```protobuf
message CollectionSchema {
    string name = 1;
    string description = 2;
    bool autoID = 3;
    repeated FieldSchema fields = 4;
}
```

Collection schema contains all the base information of a collection including **collection name**, **description**, **autoID** and **fields**. Collection description is defined by database manager to describe the collection. **autoID** determines whether the ID of each row of data is user-defined. If **autoID** is true, our system will generate a unique ID for each data. If **autoID** is false, user need to give each entity a ID when insert. 

**Fields** is a list of **FieldSchema**. Each schema should include Field **name**, **description**, **dataType**, **type_params** and **index_params**.

FieldSchema  struct is shown as follows:

```protobuf
message FieldSchema {
    string name = 1;
    string description = 2;
    DataType data_type = 3;
    repeated common.KeyValuePair type_params = 4;
    repeated common.KeyValuePair index_params = 5;
}
```

**Field schema** contains all the base information of a field including field **name**, **description**, **data_type**, **type_params** and **index_params**. **data_type** is a enum type to distingush different data type.Total enum is shown in the last of this doc

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

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.2 DropCollection

**Interface:**

```
rpc DropCollection(CollectionName) returns (common.Status) {}
```

**Description:**

This method is used to delete collection.

**Parameters:** 

- **CollectionName**

CollectionName  struct is shown as follows:

```protobuf
message CollectionName {
    string collection_name = 1;
}
```

**CollectionName** contains only a string named **collection_name**. Collection with the same collection_name is going to be deleted.

**Returns:** 

- **common.Status**

```protobuf
message Status {
    ErrorCode error_code = 1;
    string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.3 HasCollection

**Interface:**

```
rpc HasCollection(CollectionName) returns (BoolResponse) {}
```

**Description:**

This method is used to test collection existence.

**Parameters:** 

- **CollectionName**

CollectionName  struct is shown as follows:

```protobuf
message CollectionName {
    string collection_name = 1;
}
```

**CollectionName** contains only a string named **collection_name**. The server finds the collection through collection_name and judge whether the collection exists.

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



###### 3.1.4 DescribeCollection

**Interface:**

```
 rpc DescribeCollection(CollectionName) returns (CollectionDescription) {}
```

**Description:**

This method is used to get collection schema.

**Parameters:** 

- **CollectionName**

CollectionName  struct is shown as follows:

```protobuf
message CollectionName {
    string collection_name = 1;
}
```

**CollectionName** contains only a string named **collection_name**. The server finds the collection through collection_name and get detailed collection information

**Returns:** 

- **CollectionDescription**

```protobuf
message CollectionDescription {
    common.Status status = 1;
    schema.CollectionSchema schema = 2;
    repeated common.KeyValuePair statistics = 3;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**schema** is collection schema same as the collection schema in [CreateCollection](#311-createcollection).

**statitistics** is a statistic used to count various information, such as the number of segments, how many rows there are, the number of visits in the last hour, etc.



###### 3.1.5 ShowCollections

**Interface:**

```
rpc ShowCollections(common.Empty) returns (StringListResponse) {}
```

**Description:**

This method is used to get collection schema.

**Parameters:** None

**Returns:** 

- **StringListResponse**

```protobuf
message StringListResponse {
    common.Status status = 1;
    repeated string values = 2;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**values**  is a list contains all collections' name.



###### 3.1.6 CreatePartition

**Interface:**

```
rpc CreatePartition(PartitionName) returns (common.Status) {}
```

**Description:**

This method is used to create partition

**Parameters:** 

- **PartitionName**

PartitionName  struct is shown as follows:

```protobuf
message PartitionName {
    string partition_name = 1;
}
```

**PartitionName** contains only a string named **partition_name**. The server creates partition with the partition_name

- **Returns:** 

- **common.Status**

```protobuf
message Status {
    ErrorCode error_code = 1;
    string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.7 DropPartition

**Interface:**

```
rpc DropPartition(PartitionName) returns (common.Status) {}
```

**Description:**

This method is used to drop partition.

**Parameters:** 

- **PartitionName**

PartitionName  struct is shown as follows:

```protobuf
message PartitionName {
    string partition_name = 1;
}
```

**PartitionName** contains only a string named **partition_name**. Partition with the same partition_name is going to be deleted.

**Returns:** 

- **common.Status**

```protobuf
message Status {
    ErrorCode error_code = 1;
    string reason = 2;
}
```

**Status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.



###### 3.1.8 HasPartition

**Interface:**

```
rpc HasPartition(PartitionName) returns (BoolResponse) {}
```

**Description:**

This method is used to test partition existence.

**Parameters:** 

- **PartitionName** 

PartitionName  struct is shown as follows:

```protobuf
message PartitionName {
    string partition_name = 1;
}
```

**PartitionName** contains only a string named **partition_name**. Partition with the same partition_name is going to be tested.

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



###### 3.1.9 DescribePartition

**Interface:**

```
rpc DescribePartition(PartitionName) returns (PartitionDescription) {}
```

**Description:**

This method is used to show partition information

**Parameters:** 

- **PartitionName** 

PartitionName  struct is shown as follows:

```protobuf
message PartitionName {
    string partition_name = 1;
}
```

**PartitionName** contains only a string named **partition_name**. The server finds the partition through partition_name and get detailed partition information

**Returns:** 

- **PartitionDescription**

```protobuf
message PartitionDescription {
    common.Status status = 1;
    PartitionName name = 2;
    repeated common.KeyValuePair statistics = 3;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**name** is partition_name same as the PartitionName in [CreatePartition](#316-createpartition).

**statitistics** is a statistic used to count various information, such as the number of segments, how many rows there are, the number of visits in the last hour, etc.



###### 3.1.10 ShowPartitions

**Interface:**

```
rpc ShowPartitions(CollectionName) returns (StringListResponse) {}
```

**Description:**

This method is used to get partition description.

**Parameters:** 

- **CollectionName**

CollectionName  struct is shown as follows:

```protobuf
message CollectionName {
    string collection_name = 1;
}
```

**CollectionName** contains only a string named **collection_name**. Partition with the same collection_name is going to be listed.

**Returns:** 

- **StringListResponse**

```protobuf
message StringListResponse {
    common.Status status = 1;
    repeated string values = 2;
}
```

**status** represents the server error code. It doesn't contains grpc error but contains the server error code. We can get the executing result in common status. **error_code** is a enum type to distingush the executing error type. The total Errorcode is shown in the last of this code. And the **reason** field is a string to describes the detailed error.

**values**  is a list contains all partitions' name.



#### 3.2 Manipulation Requsts

###### 3.2.1 Insert

* Insert

###### 3.2.2 Delete

* DeleteByID



#### 3.3 Query



