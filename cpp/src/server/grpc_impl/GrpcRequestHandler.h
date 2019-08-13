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
namespace grpc {
class GrpcRequestHandler final : public ::milvus::grpc::MilvusService::Service {
public:
    /**
     * @brief Create table method
     *
     * This method is used to create table
     *
     * @param context, add context for every RPC
     * @param request, used to provide table information to be created.
     * @param response, used to get the status
     *
     * @return status
     *
     * @param request
     * @param response
     * @param context
     */
    ::grpc::Status
    CreateTable(::grpc::ServerContext *context,
                const ::milvus::grpc::TableSchema *request, ::milvus::grpc::Status *response) override;

    /**
     * @brief Test table existence method
     *
     * This method is used to test table existence.
     *
     * @param context, add context for every RPC
     * @param request, table name is going to be tested.
     * @param response, get the bool reply of hastable
     *
     * @return status
     *
     * @param request
     * @param response
     * @param context
     */
    ::grpc::Status
    HasTable(::grpc::ServerContext *context,
             const ::milvus::grpc::TableName *request, ::milvus::grpc::BoolReply *response) override;

    /**
     * @brief Drop table method
     *
     * This method is used to drop table.
     *
     * @param context, add context for every RPC
     * @param request, table name is going to be deleted.
     * @param response, get the status of droptable
     *
     * @return status
     *
     * @param request
     * @param response
     * @param context
     */
    ::grpc::Status
    DropTable(::grpc::ServerContext *context,
              const ::milvus::grpc::TableName *request, ::milvus::grpc::Status *response) override;

    /**
     * @brief build index by table method
     *
     * This method is used to build index by table in sync.
     *
     * @param context, add context for every RPC
     * @param request, table name is going to be built index.
     * @param response, get the status of buildindex
     *
     * @return status
     *
     * @param request
     * @param response
     * @param context
     */
    ::grpc::Status
    CreateIndex(::grpc::ServerContext *context,
               const ::milvus::grpc::IndexParam *request, ::milvus::grpc::Status *response) override;


    /**
     * @brief Insert vector array to table
     *
     * This method is used to insert vector array to table.
     *
     * @param context, add context for every RPC
     * @param request, table_name is inserted.
     * @param response, vector array is inserted.
     *
     * @return status
     *
     * @param context
     * @param request
     * @param response
     */
    ::grpc::Status
    Insert(::grpc::ServerContext *context,
                 const ::milvus::grpc::InsertParam *request,
                 ::milvus::grpc::VectorIds *response) override;

    /**
     * @brief Query vector
     *
     * This method is used to query vector in table.
     *
     * @param context, add context for every RPC
     * @param request:
     *               table_name, table_name is queried.
     *               query_record_array, all vector are going to be queried.
     *               query_range_array, optional ranges for conditional search. If not specified, search whole table
     *               topk, how many similarity vectors will be searched.
     *
     * @param writer, write query result array.
     *
     * @return status
     *
     * @param context
     * @param request
     * @param writer
     */
    ::grpc::Status
    Search(::grpc::ServerContext *context,
                 const ::milvus::grpc::SearchParam *request,
                 ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> *writer) override;

    /**
   * @brief Internal use query interface
   *
   * This method is used to query vector in specified files.
   *
   * @param context, add context for every RPC
   * @param request:
   *                file_id_array, specified files id array, queried.
   *                query_record_array, all vector are going to be queried.
   *                query_range_array, optional ranges for conditional search. If not specified, search whole table
   *                topk, how many similarity vectors will be searched.
   *
   * @param writer, write query result array.
   *
   * @return status
   *
   * @param context
   * @param request
   * @param writer
   */
    ::grpc::Status
    SearchInFiles(::grpc::ServerContext *context,
                        const ::milvus::grpc::SearchInFilesParam *request,
                        ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> *writer) override;

    /**
     * @brief Get table schema
     *
     * This method is used to get table schema.
     *
     * @param context, add context for every RPC
     * @param request, target table name.
     * @param response, table schema
     *
     * @return status
     *
     * @param context
     * @param request
     * @param response
     */
    ::grpc::Status
    DescribeTable(::grpc::ServerContext *context,
                  const ::milvus::grpc::TableName *request,
                  ::milvus::grpc::TableSchema *response) override;

    /**
     * @brief Get table row count
     *
     * This method is used to get table row count.
     *
     * @param context, add context for every RPC
     * @param request, target table name.
     * @param response, table row count
     *
     * @return table row count
     *
     * @param request
     * @param response
     * @param context
     */
    ::grpc::Status
    CountTable(::grpc::ServerContext *context,
                     const ::milvus::grpc::TableName *request,
                     ::milvus::grpc::TableRowCount *response) override;

    /**
     * @brief List all tables in database
     *
     * This method is used to list all tables.
     *
     * @param context, add context for every RPC
     * @param request, show table command, usually not use
     * @param writer, write tables to client
     *
     * @return status
     *
     * @param context
     * @param request
     * @param writer
     */
    ::grpc::Status
    ShowTables(::grpc::ServerContext *context,
               const ::milvus::grpc::Command *request,
               ::grpc::ServerWriter<::milvus::grpc::TableName> *writer) override;

    /**
     * @brief Give the server status
     *
     *
     * This method is used to give the server status.
     * @param context, add context for every RPC
     * @param request, give server command
     * @param response, server status
     *
     * @return status
     *
     * @param context
     * @param request
     * @param response
     */
    ::grpc::Status
    Cmd(::grpc::ServerContext *context,
         const ::milvus::grpc::Command *request,
         ::milvus::grpc::StringReply *response) override;

    /**
     * @brief delete table by range
     *
     * This method is used to delete table by range.
     * @param context, add context for every RPC
     * @param request, table name and range
     * @param response, status
     *
     * @return status
     *
     * @param context
     * @param request
     * @param response
     */
    ::grpc::Status
    DeleteByRange(::grpc::ServerContext *context,
                  const ::milvus::grpc::DeleteByRangeParam *request,
                  ::milvus::grpc::Status *response) override;

    /**
     * @brief preload table
     *
     * This method is used to preload table.
     * @param context, add context for every RPC
     * @param request, table name
     * @param response, status
     *
     * @return status
     *
     * @param context
     * @param request
     * @param response
     */
    ::grpc::Status
    PreloadTable(::grpc::ServerContext *context,
                  const ::milvus::grpc::TableName *request,
                  ::milvus::grpc::Status *response) override;

    /**
     * @brief Describe index
     *
     * This method is used to describe index.
     * @param context, add context for every RPC
     * @param request, table name
     * @param response, index informations
     *
     * @return status
     *
     * @param context
     * @param request
     * @param response
     */
    ::grpc::Status
    DescribeIndex(::grpc::ServerContext *context,
                 const ::milvus::grpc::TableName *request,
                 ::milvus::grpc::IndexParam *response) override;

    /**
     * @brief Drop index
     *
     * This method is used to drop index.
     * @param context, add context for every RPC
     * @param request, table name
     * @param response, status
     *
     * @return status
     *
     * @param context
     * @param request
     * @param response
     */
    ::grpc::Status
    DropIndex(::grpc::ServerContext *context,
                 const ::milvus::grpc::TableName *request,
                 ::milvus::grpc::Status *response) override;

};
}
}
}
}

