#pragma once

#include "Status.h"

#include <string>
#include <vector>
#include <map>
#include <memory>

/** \brief MegaSearch SDK namespace
 */
namespace megasearch {


/**
 * @brief Column Type
 */
enum class ColumnType {
    invalid,
    int8,
    int16,
    int32,
    int64,
    float32,
    float64,
    date,
    vector
};

/**
 * @brief Index Type
 */
enum class IndexType {
    raw,
    ivfflat
};

/**
 * @brief Connect API parameter
 */
struct ConnectParam {
    std::string ip_address; ///< Server IP address
    std::string port;       ///< Server PORT
};

/**
 * @brief Table column description
 */
struct Column {
    ColumnType type = ColumnType::invalid;         ///< Column Type: enum ColumnType
    std::string name;                              ///< Column name
};

/**
 * @brief Table vector column description
 */
struct VectorColumn : public Column {
    VectorColumn() { type = ColumnType::vector; }
    int64_t dimension = 0;                 ///< Vector dimension
    IndexType index_type = IndexType::raw; ///< Index type
    bool store_raw_vector = false;         ///< Is vector self stored in the table
};

/**
 * @brief Table Schema
 */
struct TableSchema {
    std::string table_name;                                          ///< Table name
    std::vector<VectorColumn> vector_column_array;                   ///< Vector column description
    std::vector<Column> attribute_column_array;                      ///< Columns description
    std::vector<std::string> partition_column_name_array;            ///< Partition column name
};

/**
 * @brief Range information
 */
struct Range {
    std::string start_value;     ///< Range start
    std::string end_value;       ///< Range stop
};

/**
 * @brief Create table partition parameters
 */
struct CreateTablePartitionParam {
    std::string table_name;     ///< Table name, vector/float32/float64 type column is not allowed for partition
    std::string partition_name;             ///< Partition name, created partition name
    std::map<std::string, Range> range_map; ///< Column name to PartitionRange map
};


/**
 * @brief Delete table partition parameters
 */
struct DeleteTablePartitionParam {
    std::string table_name;                        ///< Table name
    std::vector<std::string> partition_name_array; ///< Partition name array
};

/**
 * @brief Record inserted
 */
struct RowRecord {
    std::map<std::string, std::vector<float>> vector_map; ///< Vector columns
    std::map<std::string, std::string> attribute_map;     ///< Other attribute columns
};

/**
 * @brief Query record
 */
struct QueryRecord {
    std::map<std::string, std::vector<float>> vector_map;                       ///< Query vectors
    std::vector<std::string> selected_column_array;                             ///< Output column array
    std::map<std::string, std::vector<Range>> partition_filter_column_map;      ///< Range used to select partitions
};

/**
 * @brief Query result
 */
struct QueryResult {
    int64_t id;                                     ///< Output result
    double score;                                   ///< Vector similarity score: 0 ~ 100
    std::map<std::string, std::string> column_map;  ///< Other column
};

/**
 * @brief TopK query result
 */
struct TopKQueryResult {
    std::vector<QueryResult> query_result_arrays;   ///< TopK query result
};

/**
 * @brief SDK main class
 */
class Connection {
 public:

    /**
     * @brief CreateConnection
     *
     * Create a connection instance and return it's shared pointer
     *
     * @return Connection instance pointer
     */

    static std::shared_ptr<Connection>
    Create();

    /**
     * @brief DestroyConnection
     *
     * Destroy the connection instance
     *
     * @param connection, the shared pointer to the instance to be destroyed
     *
     * @return if destroy is successful
     */

    static Status
    Destroy(std::shared_ptr<Connection> connection_ptr);

    /**
     * @brief Connect
     *
     * Connect function should be called before any operations
     * Server will be connected after Connect return OK
     *
     * @param param, use to provide server information
     *
     * @return Indicate if connect is successful
     */

    virtual Status Connect(const ConnectParam &param) = 0;

    /**
     * @brief Connect
     *
     * Connect function should be called before any operations
     * Server will be connected after Connect return OK
     *
     * @param uri, use to provide server information, example: megasearch://ipaddress:port
     *
     * @return Indicate if connect is successful
     */
    virtual Status Connect(const std::string &uri) = 0;

    /**
     * @brief connected
     *
     * Connection status.
     *
     * @return Indicate if connection status
     */
    virtual Status Connected() const = 0;

    /**
     * @brief Disconnect
     *
     * Server will be disconnected after Disconnect return OK
     *
     * @return Indicate if disconnect is successful
     */
    virtual Status Disconnect() = 0;


    /**
     * @brief Create table method
     *
     * This method is used to create table
     *
     * @param param, use to provide table information to be created.
     *
     * @return Indicate if table is created successfully
     */
    virtual Status CreateTable(const TableSchema &param) = 0;


    /**
     * @brief Delete table method
     *
     * This method is used to delete table.
     *
     * @param table_name, table name is going to be deleted.
     *
     * @return Indicate if table is delete successfully.
     */
    virtual Status DeleteTable(const std::string &table_name) = 0;


    /**
     * @brief Create table partition
     *
     * This method is used to create table partition.
     *
     * @param param, use to provide partition information to be created.
     *
     * @return Indicate if table partition is created successfully.
     */
    virtual Status CreateTablePartition(const CreateTablePartitionParam &param) = 0;


    /**
     * @brief Delete table partition
     *
     * This method is used to delete table partition.
     *
     * @param param, use to provide partition information to be deleted.
     *
     * @return Indicate if table partition is delete successfully.
     */
    virtual Status DeleteTablePartition(const DeleteTablePartitionParam &param) = 0;


    /**
     * @brief Add vector to table
     *
     * This method is used to add vector array to table.
     *
     * @param table_name, table_name is inserted.
     * @param record_array, vector array is inserted.
     * @param id_array, after inserted every vector is given a id.
     *
     * @return Indicate if vector array are inserted successfully
     */
    virtual Status AddVector(const std::string &table_name,
                     const std::vector<RowRecord> &record_array,
                     std::vector<int64_t> &id_array) = 0;


    /**
     * @brief Search vector
     *
     * This method is used to query vector in table.
     *
     * @param table_name, table_name is queried.
     * @param query_record_array, all vector are going to be queried.
     * @param topk_query_result_array, result array.
     * @param topk, how many similarity vectors will be searched.
     *
     * @return Indicate if query is successful.
     */
    virtual Status SearchVector(const std::string &table_name,
                        const std::vector<QueryRecord> &query_record_array,
                        std::vector<TopKQueryResult> &topk_query_result_array,
                        int64_t topk) = 0;

    /**
     * @brief Show table description
     *
     * This method is used to show table information.
     *
     * @param table_name, which table is show.
     * @param table_schema, table_schema is given when operation is successful.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status DescribeTable(const std::string &table_name, TableSchema &table_schema) = 0;

    /**
     * @brief Show all tables in database
     *
     * This method is used to list all tables.
     *
     * @param table_array, all tables are push into the array.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status ShowTables(std::vector<std::string> &table_array) = 0;

    /**
     * @brief Give the client version
     *
     * This method is used to give the client version.
     *
     * @return Client version.
     */
    virtual std::string ClientVersion() const = 0;

    /**
     * @brief Give the server version
     *
     * This method is used to give the server version.
     *
     * @return Server version.
     */
    virtual std::string ServerVersion() const = 0;

    /**
     * @brief Give the server status
     *
     * This method is used to give the server status.
     *
     * @return Server status.
     */
    virtual std::string ServerStatus() const = 0;
};

}