/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
namespace cl megasearch.thrift
namespace cpp megasearch.thrift
namespace py megasearch.thrift
namespace d megasearch.thrift
namespace dart megasearch.thrift
namespace java megasearch.thrift
namespace perl megasearch.thrift
namespace php megasearch.thrift
namespace haxe megasearch.thrift
namespace netcore megasearch.thrift

enum ErrorCode {
    SUCCESS = 0,
    CONNECT_FAILED,
    PERMISSION_DENIED,
	TABLE_NOT_EXISTS,
	PARTITION_NOT_EXIST,
	ILLEGAL_ARGUMENT,
	ILLEGAL_RANGE,
	ILLEGAL_DIMENSION,
}

exception Exception {
	1: ErrorCode code;
	2: string reason;
}


/**
 * @brief Table column description
 */
struct Column {
    1: required i32 type;          ///< Column Type: 0:invealid/1:int8/2:int16/3:int32/4:int64/5:float32/6:float64/7:date/8:vector
    2: required string name;       ///< Column name
}

/**
 * @brief Table vector column description
 */
struct VectorColumn {
    1: required Column base;                 ///< Base column schema
    2: required i64 dimension;               ///< Vector dimension
    3: required string index_type;           ///< Index type, optional: raw, ivf
    4: bool store_raw_vector = false;        ///< Is vector self stored in the table
}

/**
 * @brief Table Schema
 */
struct TableSchema {
    1: required string table_name;                                        ///< Table name
    2: required list<VectorColumn> vector_column_array;                   ///< Vector column description
    3: optional list<Column> attribute_column_array;                      ///< Columns description
    4: optional list<string> partition_column_name_array;                 ///< Partition column name
}

/**
 * @brief Range Schema
 */
struct Range {
    1: required string start_value;     ///< Range start
    2: required string end_value;       ///< Range stop
}

/**
 * @brief Create table partition parameters
 */
struct CreateTablePartitionParam {
    1: required string table_name;                              ///< Table name, vector/float32/float64 type column is not allowed for partition
    2: required string partition_name;                          ///< Partition name, created partition name
    3: required map<string, Range> range_map;              ///< Column name to Range map
}


/**
 * @brief Delete table partition parameters
 */
struct DeleteTablePartitionParam {
    1: required string table_name;                        ///< Table name
    2: required list<string> partition_name_array;        ///< Partition name array
}

/**
 * @brief Record inserted
 */
struct RowRecord {
    1: required map<string, binary> vector_map; ///< Vector columns
    2: map<string, string> attribute_map;            ///< Other attribute columns
}

/**
 * @brief Query record
 */
struct QueryRecord {
    1: required map<string, binary> vector_map;                       ///< Query vectors
    2: optional list<string> selected_column_array;                             ///< Output column array
    3: optional map<string, list<Range>> partition_filter_column_map;      ///< Range used to select partitions
}

/**
 * @brief Query result
 */
struct QueryResult {
    1: i64 id;                                     ///< Output result
    2: double score;                               ///< Vector similarity score: 0 ~ 100
    3: map<string, string> column_map;        ///< Other column
}

/**
 * @brief TopK query result
 */
struct TopKQueryResult {
    1: list<QueryResult> query_result_arrays;      ///< TopK query result
}

service MegasearchService {
    /**
     * @brief Create table method
     *
     * This method is used to create table
     *
     * @param param, use to provide table information to be created.
     *
     */
    void CreateTable(2: TableSchema param) throws(1: Exception e);


    /**
     * @brief Delete table method
     *
     * This method is used to delete table.
     *
     * @param table_name, table name is going to be deleted.
     *
     */
    void DeleteTable(2: string table_name) throws(1: Exception e);


    /**
     * @brief Create table partition
     *
     * This method is used to create table partition.
     *
     * @param param, use to provide partition information to be created.
     *
     */
    void CreateTablePartition(2: CreateTablePartitionParam param) throws(1: Exception e);


    /**
     * @brief Delete table partition
     *
     * This method is used to delete table partition.
     *
     * @param param, use to provide partition information to be deleted.
     *
     */
    void DeleteTablePartition(2: DeleteTablePartitionParam param) throws(1: Exception e);


    /**
     * @brief Add vector array to table
     *
     * This method is used to add vector array to table.
     *
     * @param table_name, table_name is inserted.
     * @param record_array, vector array is inserted.
     *
     * @return vector id array
     */
    list<i64> AddVector(2: string table_name,
                        3: list<RowRecord> record_array) throws(1: Exception e);


    /**
     * @brief Query vector
     *
     * This method is used to query vector in table.
     *
     * @param table_name, table_name is queried.
     * @param query_record_array, all vector are going to be queried.
     * @param topk, how many similarity vectors will be searched.
     *
     * @return query result array.
     */
    list<TopKQueryResult> SearchVector(2: string table_name,
                                       3: list<QueryRecord> query_record_array,
                                       4: i64 topk) throws(1: Exception e);

    /**
     * @brief Show table information
     *
     * This method is used to show table information.
     *
     * @param table_name, which table is show.
     *
     * @return table schema
     */
    TableSchema DescribeTable(2: string table_name) throws(1: Exception e);

    /**
     * @brief List all tables in database
     *
     * This method is used to list all tables.
     *
     *
     * @return table names.
     */
    list<string> ShowTables() throws(1: Exception e);

    /**
     * @brief Give the server status
     *
     * This method is used to give the server status.
     *
     * @return Server status.
     */
    string Ping(2: string cmd) throws(1: Exception e);
}