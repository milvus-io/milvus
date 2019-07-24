/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <cstdint>
#include <string>

#include "MilvusService.h"

namespace zilliz {
namespace milvus {
namespace server {

class RequestHandler : virtual public ::milvus::thrift::MilvusServiceIf {
public:
    RequestHandler();

    /**
     * @brief Create table method
     *
     * This method is used to create table
     *
     * @param param, use to provide table information to be created.
     *
     *
     * @param param
     */
    void CreateTable(const ::milvus::thrift::TableSchema &param);

    /**
     * @brief Test table existence method
     *
     * This method is used to test table existence.
     *
     * @param table_name, table name is going to be tested.
     *
     *
     * @param table_name
     */
    bool HasTable(const std::string &table_name);

    /**
     * @brief Delete table method
     *
     * This method is used to delete table.
     *
     * @param table_name, table name is going to be deleted.
     *
     *
     * @param table_name
     */
    void DeleteTable(const std::string& table_name);

    /**
     * @brief build index by table method
     *
     * This method is used to build index by table in sync.
     *
     * @param table_name, table name is going to be built index.
     *
     *
     * @param table_name
     */
    void BuildIndex(const std::string &table_name);

    /**
     * @brief Add vector array to table
     *
     * This method is used to add vector array to table.
     *
     * @param table_name, table_name is inserted.
     * @param record_array, vector array is inserted.
     *
     * @return vector id array
     *
     * @param table_name
     * @param record_array
     */
    void AddVector(std::vector<int64_t> & _return,
            const std::string& table_name,
            const std::vector<::milvus::thrift::RowRecord> & record_array);

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
     *
     * @param table_name
     * @param query_record_array
     * @param query_range_array
     * @param topk
     */
    void SearchVector(std::vector<::milvus::thrift::TopKQueryResult> & _return,
            const std::string& table_name,
            const std::vector<::milvus::thrift::RowRecord> & query_record_array,
            const std::vector<::milvus::thrift::Range> & query_range_array,
            const int64_t topk);

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
     * @return query binary result array.
     *
     * @param table_name
     * @param query_record_array
     * @param query_range_array
     * @param topk
     */
    void SearchVector2(std::vector<::milvus::thrift::TopKQueryBinResult> & _return,
            const std::string& table_name,
            const std::vector<::milvus::thrift::RowRecord> & query_record_array,
            const std::vector<::milvus::thrift::Range> & query_range_array,
            const int64_t topk);

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
   *
   * @param file_id_array
   * @param query_record_array
   * @param query_range_array
   * @param topk
   */
    virtual void SearchVectorInFiles(std::vector<::milvus::thrift::TopKQueryResult> & _return,
            const std::string& table_name,
            const std::vector<std::string> & file_id_array,
            const std::vector<::milvus::thrift::RowRecord> & query_record_array,
            const std::vector<::milvus::thrift::Range> & query_range_array,
            const int64_t topk);

    /**
     * @brief Get table schema
     *
     * This method is used to get table schema.
     *
     * @param table_name, target table name.
     *
     * @return table schema
     *
     * @param table_name
     */
    void DescribeTable(::milvus::thrift::TableSchema& _return, const std::string& table_name);

    /**
     * @brief Get table row count
     *
     * This method is used to get table row count.
     *
     * @param table_name, target table name.
     *
     * @return table row count
     *
     * @param table_name
     */
    int64_t GetTableRowCount(const std::string& table_name);

    /**
     * @brief List all tables in database
     *
     * This method is used to list all tables.
     *
     *
     * @return table names.
     */
    void ShowTables(std::vector<std::string> & _return);

    /**
     * @brief Give the server status
     *
     * This method is used to give the server status.
     *
     * @return Server status.
     *
     * @param cmd
     */
    void Ping(std::string& _return, const std::string& cmd);
};

}
}
}
