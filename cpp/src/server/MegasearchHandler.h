/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <cstdint>
#include <string>

#include "MegasearchService.h"

namespace zilliz {
namespace vecwise {
namespace server {

class MegasearchServiceHandler : virtual public megasearch::thrift::MegasearchServiceIf {
public:
    MegasearchServiceHandler();

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
    void CreateTable(const megasearch::thrift::TableSchema& param);

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
     * @brief Create table partition
     *
     * This method is used to create table partition.
     *
     * @param param, use to provide partition information to be created.
     *
     *
     * @param param
     */
    void CreateTablePartition(const megasearch::thrift::CreateTablePartitionParam& param);

    /**
     * @brief Delete table partition
     *
     * This method is used to delete table partition.
     *
     * @param param, use to provide partition information to be deleted.
     *
     *
     * @param param
     */
    void DeleteTablePartition(const megasearch::thrift::DeleteTablePartitionParam& param);

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
            const std::vector<megasearch::thrift::RowRecord> & record_array);

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
     *
     * @param table_name
     * @param query_record_array
     * @param topk
     */
    void SearchVector(std::vector<megasearch::thrift::TopKQueryResult> & _return,
            const std::string& table_name,
            const std::vector<megasearch::thrift::QueryRecord> & query_record_array,
            const int64_t topk);

    /**
     * @brief Show table information
     *
     * This method is used to show table information.
     *
     * @param table_name, which table is show.
     *
     * @return table schema
     *
     * @param table_name
     */
    void DescribeTable(megasearch::thrift::TableSchema& _return, const std::string& table_name);

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
