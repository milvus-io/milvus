#pragma once

#include "Status.h"

#include <string>
#include <vector>
#include <memory>

/** \brief Milvus SDK namespace
 */
namespace milvus {


/**
 * @brief Index Type
 */
enum class IndexType {
    invalid = 0,
    cpu_idmap,
    gpu_ivfflat,
    gpu_ivfsq8,
};

/**
 * @brief Connect API parameter
 */
struct ConnectParam {
    std::string ip_address;                                ///< Server IP address
    std::string port;                                      ///< Server PORT
};

/**
 * @brief Table Schema
 */
struct TableSchema {
    std::string table_name;                                ///< Table name
    IndexType index_type = IndexType::invalid;             ///< Index type
    int64_t dimension = 0;                                 ///< Vector dimension, must be a positive value
    bool store_raw_vector = false;                          ///< Is vector raw data stored in the table
};

/**
 * @brief Range information
 * for DATE partition, the format is like: 'year-month-day'
 */
struct Range {
    std::string start_value;                                ///< Range start
    std::string end_value;                                  ///< Range stop
};

/**
 * @brief Record inserted
 */
struct RowRecord {
    std::vector<float> data;                               ///< Vector raw data
};

/**
 * @brief Query result
 */
struct QueryResult {
    int64_t id;                                             ///< Output result
    double distance;                                        ///< Vector similarity distance
};

/**
 * @brief TopK query result
 */
struct TopKQueryResult {
    std::vector<QueryResult> query_result_arrays;           ///< TopK query result
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
     * @param uri, use to provide server information, example: milvus://ipaddress:port
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
     * @brief Test table existence method
     *
     * This method is used to create table
     *
     * @param table_name, table name is going to be tested.
     *
     * @return Indicate if table is cexist
     */
    virtual bool HasTable(const std::string &table_name) = 0;


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
     * @param query_range_array, time ranges, if not specified, will search in whole table
     * @param topk, how many similarity vectors will be searched.
     * @param topk_query_result_array, result array.
     *
     * @return Indicate if query is successful.
     */
    virtual Status SearchVector(const std::string &table_name,
                                const std::vector<RowRecord> &query_record_array,
                                const std::vector<Range> &query_range_array,
                                int64_t topk,
                                std::vector<TopKQueryResult> &topk_query_result_array) = 0;

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
     * @brief Get table row count
     *
     * This method is used to get table row count.
     *
     * @param table_name, table's name.
     * @param row_count, table total row count.
     *
     * @return Indicate if this operation is successful.
     */
    virtual Status GetTableRowCount(const std::string &table_name, int64_t &row_count) = 0;

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