/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <cstdint>
#include <string>

#include "milvus.grpc.pb.h"
#include "status.pb.h"

namespace zilliz {
namespace milvus {
namespace server {
class RequestHandler final : public ::milvus::grpc::MilvusService::Service {
public:
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
    ::grpc::Status
    CreateTable(::grpc::ServerContext* context,
            const ::milvus::grpc::TableSchema* request, ::milvus::grpc::Status* response) override ;

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
    ::grpc::Status
    HasTable(::grpc::ServerContext* context,
            const ::milvus::grpc::TableName* request, ::milvus::grpc::BoolReply* response) override ;

    /**
     * @brief Drop table method
     *
     * This method is used to drop table.
     *
     * @param table_name, table name is going to be deleted.
     *
     *
     * @param table_name
     */
    ::grpc::Status
    DropTable(::grpc::ServerContext* context,
            const ::milvus::grpc::TableName* request, ::milvus::grpc::Status* response) override;

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
    ::grpc::Status
    BuildIndex(::grpc::ServerContext* context,
            const ::milvus::grpc::TableName* request, ::milvus::grpc::Status* response) override;


    /**
     * @brief Insert vector array to table
     *
     * This method is used to insert vector array to table.
     *
     * @param table_name, table_name is inserted.
     * @param record_array, vector array is inserted.
     *
     * @return vector id array
     *
     * @param table_name
     * @param record_array
     */
    ::grpc::Status
    InsertVector(::grpc::ServerContext* context,
            const ::milvus::grpc::InsertInfos* request, ::milvus::grpc::VectorIds* response) override;

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
    ::grpc::Status
    SearchVector(::grpc::ServerContext* context,
            const ::milvus::grpc::SearchVectorInfos* request, ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>* writer) override;

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
    ::grpc::Status
    SearchVectorInFiles(::grpc::ServerContext* context,
            const ::milvus::grpc::SearchVectorInFilesInfos* request, ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>* writer) override;

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
    ::grpc::Status
    DescribeTable(::grpc::ServerContext* context,
            const ::milvus::grpc::TableName* request, ::milvus::grpc::TableSchema* response) override;

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
    ::grpc::Status
    GetTableRowCount(::grpc::ServerContext* context,
            const ::milvus::grpc::TableName* request, ::milvus::grpc::TableRowCount* response) override;

    /**
     * @brief List all tables in database
     *
     * This method is used to list all tables.
     *
     *
     * @return table names.
     */
    ::grpc::Status
    ShowTables(::grpc::ServerContext* context,
            const ::milvus::grpc::Command* request, ::grpc::ServerWriter< ::milvus::grpc::TableName>* writer) override;

    /**
     * @brief Give the server status
     *
     *
     * This method is used to give the server status.
     *
     * @return Server status.
     *
     * @param cmd
     */
    ::grpc::Status
    Ping(::grpc::ServerContext* context,
            const ::milvus::grpc::Command* request, ::milvus::grpc::ServerStatus* response) override;

};
}
}
}



