/*******************************************************************************
* Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
* Unauthorized copying of this file, via any medium is strictly prohibited.
* Proprietary and confidential.
******************************************************************************/

#include "GrpcRequestHandler.h"
#include "GrpcRequestTask.h"
#include "utils/TimeRecorder.h"

namespace zilliz {
namespace milvus {
namespace server {
namespace grpc {

::grpc::Status
GrpcRequestHandler::CreateTable(::grpc::ServerContext *context,
                                const ::milvus::grpc::TableSchema *request,
                                ::milvus::grpc::Status *response) {

    BaseTaskPtr task_ptr = CreateTableTask::Create(*request);
    GrpcRequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::HasTable(::grpc::ServerContext *context,
                             const ::milvus::grpc::TableName *request,
                             ::milvus::grpc::BoolReply *response) {

    bool has_table = false;
    BaseTaskPtr task_ptr = HasTableTask::Create(request->table_name(), has_table);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_bool_reply(has_table);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropTable(::grpc::ServerContext *context,
                              const ::milvus::grpc::TableName *request,
                              ::milvus::grpc::Status *response) {

    BaseTaskPtr task_ptr = DropTableTask::Create(request->table_name());
    GrpcRequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CreateIndex(::grpc::ServerContext *context,
                               const ::milvus::grpc::IndexParam *request,
                               ::milvus::grpc::Status *response) {

    BaseTaskPtr task_ptr = CreateIndexTask::Create(*request);
    GrpcRequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Insert(::grpc::ServerContext *context,
                                 const ::milvus::grpc::InsertParam *request,
                                 ::milvus::grpc::VectorIds *response) {

    BaseTaskPtr task_ptr = InsertTask::Create(*request, *response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::Search(::grpc::ServerContext *context,
                                 const ::milvus::grpc::SearchParam *request,
                                 ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> *writer) {

    std::vector<std::string> file_id_array;
    BaseTaskPtr task_ptr = SearchTask::Create(*request, file_id_array, *writer);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    if (grpc_status.error_code() != SERVER_SUCCESS) {
        ::grpc::Status status(::grpc::INVALID_ARGUMENT, grpc_status.reason());
        return status;
    } else {
        return ::grpc::Status::OK;
    }
}

::grpc::Status
GrpcRequestHandler::SearchInFiles(::grpc::ServerContext *context,
                                        const ::milvus::grpc::SearchInFilesParam *request,
                                        ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult> *writer) {

    std::vector<std::string> file_id_array;
    BaseTaskPtr task_ptr = SearchTask::Create(request->search_param(), file_id_array, *writer);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    if (grpc_status.error_code() != SERVER_SUCCESS) {
        ::grpc::Status status(::grpc::INVALID_ARGUMENT, grpc_status.reason());
        return status;
    } else {
        return ::grpc::Status::OK;
    }
}

::grpc::Status
GrpcRequestHandler::DescribeTable(::grpc::ServerContext *context,
                                  const ::milvus::grpc::TableName *request,
                                  ::milvus::grpc::TableSchema *response) {

    BaseTaskPtr task_ptr = DescribeTableTask::Create(request->table_name(), *response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_table_name()->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_table_name()->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::CountTable(::grpc::ServerContext *context,
                                     const ::milvus::grpc::TableName *request,
                                     ::milvus::grpc::TableRowCount *response) {

    int64_t row_count = 0;
    BaseTaskPtr task_ptr = CountTableTask::Create(request->table_name(), row_count);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_table_row_count(row_count);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::ShowTables(::grpc::ServerContext *context,
                               const ::milvus::grpc::Command *request,
                               ::grpc::ServerWriter<::milvus::grpc::TableName> *writer) {

    BaseTaskPtr task_ptr = ShowTablesTask::Create(*writer);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    if (grpc_status.error_code() != SERVER_SUCCESS) {
        ::grpc::Status status(::grpc::UNKNOWN, grpc_status.reason());
        return status;
    } else {
        return ::grpc::Status::OK;
    }
}

::grpc::Status
GrpcRequestHandler::Cmd(::grpc::ServerContext *context,
                         const ::milvus::grpc::Command *request,
                         ::milvus::grpc::StringReply *response) {

    std::string result;
    BaseTaskPtr task_ptr = CmdTask::Create(request->cmd(), result);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_string_reply(result);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DeleteByRange(::grpc::ServerContext *context,
              const ::milvus::grpc::DeleteByRangeParam *request,
              ::milvus::grpc::Status *response) {
    BaseTaskPtr task_ptr = DeleteByRangeTask::Create(*request);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_error_code(grpc_status.error_code());
    response->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::PreloadTable(::grpc::ServerContext *context,
             const ::milvus::grpc::TableName *request,
             ::milvus::grpc::Status *response) {
    BaseTaskPtr task_ptr = PreloadTableTask::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DescribeIndex(::grpc::ServerContext *context,
              const ::milvus::grpc::TableName *request,
              ::milvus::grpc::IndexParam *response) {
    BaseTaskPtr task_ptr = DescribeIndexTask::Create(request->table_name(), *response);
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_table_name()->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_table_name()->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
GrpcRequestHandler::DropIndex(::grpc::ServerContext *context,
          const ::milvus::grpc::TableName *request,
          ::milvus::grpc::Status *response) {
    BaseTaskPtr task_ptr = DropIndexTask::Create(request->table_name());
    ::milvus::grpc::Status grpc_status;
    GrpcRequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_reason(grpc_status.reason());
    response->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}


}
}
}
}