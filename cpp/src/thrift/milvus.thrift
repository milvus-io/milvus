/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
namespace cpp milvus.thrift
namespace py milvus.thrift
namespace d milvus.thrift
namespace dart milvus.thrift
namespace java milvus.thrift
namespace perl milvus.thrift
namespace php milvus.thrift
namespace haxe milvus.thrift
namespace netcore milvus.thrift

enum ErrorCode {
    SUCCESS = 0,
    UNEXPECTED_ERROR,
    CONNECT_FAILED,
    PERMISSION_DENIED,
    TABLE_NOT_EXISTS,
    ILLEGAL_ARGUMENT,
    ILLEGAL_RANGE,
    ILLEGAL_DIMENSION,
    ILLEGAL_INDEX_TYPE,
    ILLEGAL_TABLE_NAME,
    ILLEGAL_TOPK,
    ILLEGAL_ROWRECORD,
    ILLEGAL_VECTOR_ID,
    ILLEGAL_SEARCH_RESULT,
    FILE_NOT_FOUND,
    META_FAILED,
    CACHE_FAILED,
    CANNOT_CREATE_FOLDER,
    CANNOT_CREATE_FILE,
    CANNOT_DELETE_FOLDER,
    CANNOT_DELETE_FILE,
}

exception Exception {
    1: ErrorCode code;
    2: string reason;
}


/**
 * @brief Table Schema
 */
struct TableSchema {
    1: required string table_name;                   ///< Table name
    2: i32 index_type = 0;                           ///< Index type, optional: 0-invalid, 1-idmap, 2-ivflat
    3: i64 dimension = 0;                            ///< Vector dimension
    4: bool store_raw_vector = false;                ///< Store raw data
}

/**
 * @brief Range Schema
 */
struct Range {
    1: string start_value;                           ///< Range start
    2: string end_value;                             ///< Range stop
}

/**
 * @brief Record inserted
 */
struct RowRecord {
    1: required binary vector_data;                  ///< Vector data, double array
}

/**
 * @brief Query result
 */
struct QueryResult {
    1: i64 id;                                       ///< Output result
    2: double score;                                 ///< Vector similarity score: 0 ~ 100
}

/**
 * @brief TopK query result
 */
struct TopKQueryResult {
    1: list<QueryResult> query_result_arrays;        ///< TopK query result
}

service MilvusService {
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
     * @brief Test table existence method
     *
     * This method is used to test table existence.
     *
     * @param table_name, table name is going to be tested.
     *
     */
    bool HasTable(2: string table_name) throws(1: Exception e);


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
     * @param query_range_array, optional ranges for conditional search. If not specified, search whole table
     * @param topk, how many similarity vectors will be searched.
     *
     * @return query result array.
     */
    list<TopKQueryResult> SearchVector(2: string table_name,
                                       3: list<RowRecord> query_record_array,
                                       4: list<Range> query_range_array,
                                       5: i64 topk) throws(1: Exception e);

    /**
     * @brief Internal use query interface
     *
     * This method is used to query vector in specified files.
     *
     * @param file_id_array, specified files id array, queried.
     * @param query_record_array, all vector are going to be queried.
     * @param query_range_array, optional ranges for conditional search. If not specified, search whole table
     * @param topk, how many similarity vectors will be searched.
     *
     * @return query result array.
     */
    list<TopKQueryResult> SearchVectorInFiles(2: string table_name,
                                              3: list<string> file_id_array,
                                              4: list<RowRecord> query_record_array,
                                              5: list<Range> query_range_array,
                                              6: i64 topk) throws(1: Exception e);

    /**
     * @brief Get table schema
     *
     * This method is used to get table schema.
     *
     * @param table_name, target table name.
     *
     * @return table schema
     */
    TableSchema DescribeTable(2: string table_name) throws(1: Exception e);


    /**
     * @brief Get table row count
     *
     * This method is used to get table row count.
     *
     * @param table_name, target table name.
     *
     * @return table row count
     */
    i64 GetTableRowCount(2: string table_name) throws(1: Exception e);

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