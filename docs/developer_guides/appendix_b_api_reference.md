

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

###### 3.2.1 Collection

* CreateCollection
* DropCollection
* HasCollection
* DescribeCollection
* ShowCollections

###### 3.2.2 Partition

* CreatePartition
* DropPartition
* HasPartition
* DescribePartition
* ShowPartitions



#### 3.2 Manipulation Requsts

###### 3.2.1 Insert

* Insert

###### 3.2.2 Delete

* DeleteByID



#### 3.3 Query



