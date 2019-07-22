#pragma onceinclude_directories(/usr/include)


#include "Status.h"

#include <string>
#include <vector>
#include <memory>
#include <thread>

/** \brief Milvus SDK namespace
*/
namespace zilliz {
namespace milvus {

//enum Error_Code {
//    SUCCESS = 0,
//    UNEXPECTED_ERROR = 1,
//    CONNECT_FAILED = 2,
//    PERMISSION_DENIED = 3,
//    TABLE_NOT_EXISTS = 4,
//    ILLEGAL_ARGUMENT = 5,
//    ILLEGAL_RANGE = 6,
//    ILLEGAL_DIMENSION = 7,
//    ILLEGAL_INDEX_TYPE = 8,
//    ILLEGAL_TABLE_NAME = 9,
//    ILLEGAL_TOPK = 10,
//    ILLEGAL_ROWRECORD = 11,
//    ILLEGAL_VECTOR_ID = 12,
//    ILLEGAL_SEARCH_RESULT = 13,
//    FILE_NOT_FOUND = 14,
//    META_FAILED = 15,
//    CACHE_FAILED = 16,
//    CANNOT_CREATE_FOLDER = 17,
//    CANNOT_CREATE_FILE = 18,
//    CANNOT_DELETE_FOLDER = 19,
//    CANNOT_DELETE_FILE = 20,
//};

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
* @brief Status for return
*/
 struct StatusMsg {
//    Error_Code errorCode;
     std::string reason;
 };

/**
* @brief Table Name
*/
 struct TableName {
     StatusMsg status;
     std::string table_name;
 };

/**
* @brief Table Schema
*/
 struct TableSchema {
     TableName table_name;                                  ///< Table name
     IndexType index_type = IndexType::invalid;             ///< Index type
     int64_t dimension = 0;                                 ///< Vector dimension, must be a positive value
     bool store_raw_vector = false;                         ///< Is vector raw data stored in the table
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


//struct InsertInfos {
//    std::string table_name;
//    std::vector<RowRecord> row_record_array;
//};

/**
* @brief Vector ids for return
*/
 struct VectorIds {
     StatusMsg status;
     std::vector<int64_t> vector_id_array;
 };

/**
* @brief Infos for searching vector
*/
 struct SearchVectorInfos {
     std::string table_name;
     std::vector<RowRecord> query_record_array;
     std::vector<Range> query_range_array;
     int64_t topk;
 };

/**
* @brief Infos for searching vector in files
*/
 struct SearchVectorInFilesInfos {
     std::vector<std::string> file_id_array;
     SearchVectorInfos search_vector_infos;
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
     StatusMsg status;
     std::vector<QueryResult> query_result_arrays;           ///< TopK query result
 };

/**
* @brief Server bool Reply
*/
 struct BoolReply {
     StatusMsg status;
     bool bool_reply;
 };

/**
* @brief Return table row count
*/
 struct TableRowCount {
     StatusMsg status;
     int64_t table_row_count;
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
      * @brief Drop table method
      *
      * This method is used to drop table.
      *
      * @param table_name, table name is going to be dropped.
      *
      * @return Indicate if table is drop successfully.
      */
     virtual Status DropTable(const std::string &table_name) = 0;


     /**
      * @brief Build index method
      *
      * This method is used to build index for whole table
      *
      * @param table_name, table name is going to be build index.
      *
      * @return Indicate if build index successfully.
      */
     virtual Status BuildIndex(const std::string &table_name) = 0;

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
     virtual Status InsertVector(const std::string &table_name,
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
}