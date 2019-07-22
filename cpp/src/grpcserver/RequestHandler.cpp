/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "RequestHandler.h"
#include "RequestTask.h"
#include "utils/TimeRecorder.h"

namespace zilliz {
namespace milvus {
namespace server {

::grpc::Status
RequestHandler::CreateTable(::grpc::ServerContext *context, const ::milvus::grpc::TableSchema *request, ::milvus::Status *response) {
    BaseTaskPtr task_ptr = CreateTableTask::Create(*request);
    RequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

//TODO: handle Response
::grpc::Status
RequestHandler::HasTable(::grpc::ServerContext *context, const ::milvus::grpc::TableName *request, ::milvus::grpc::BoolReply *response) {
    bool has_table = false;
    BaseTaskPtr task_ptr = HasTableTask::Create(request->table_name(), has_table);
    ::milvus::Status grpc_status;
    RequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_bool_reply(has_table);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::DropTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request, ::milvus::Status* response) {
    BaseTaskPtr task_ptr = DropTableTask::Create(request->table_name());
    RequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::BuildIndex(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request, ::milvus::Status* response) {
    BaseTaskPtr task_ptr = BuildIndexTask::Create(request->table_name());
    RequestScheduler::ExecTask(task_ptr, response);
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::InsertVector(::grpc::ServerContext* context, const ::milvus::grpc::InsertInfos* request, ::milvus::grpc::VectorIds* response) {
    BaseTaskPtr task_ptr = InsertVectorTask::Create(*request, *response);
    ::milvus::Status grpc_status;
    RequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::SearchVector(::grpc::ServerContext* context, const ::milvus::grpc::SearchVectorInfos* request, ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>* writer) {
    std::vector<std::string> file_id_array;
    //TODO: handle status
    BaseTaskPtr task_ptr = SearchVectorTask::Create(*request, file_id_array, *writer);
    RequestScheduler::ExecTask(task_ptr, nullptr);
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::SearchVectorInFiles(::grpc::ServerContext* context, const ::milvus::grpc::SearchVectorInFilesInfos* request, ::grpc::ServerWriter<::milvus::grpc::TopKQueryResult>* writer) {
    std::vector<std::string> file_id_array;
    BaseTaskPtr task_ptr = SearchVectorTask::Create(request->search_vector_infos(), file_id_array, *writer);
    RequestScheduler::ExecTask(task_ptr, nullptr);
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::DescribeTable(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request, ::milvus::grpc::TableSchema* response) {
    BaseTaskPtr task_ptr = DescribeTableTask::Create(request->table_name(), response);
    ::milvus::Status grpc_status;
    RequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->mutable_table_name()->mutable_status()->set_error_code(grpc_status.error_code());
    response->mutable_table_name()->mutable_status()->set_reason(grpc_status.reason());
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::GetTableRowCount(::grpc::ServerContext* context, const ::milvus::grpc::TableName* request, ::milvus::grpc::TableRowCount* response) {
    int64_t row_count = 0;
    BaseTaskPtr task_ptr = GetTableRowCountTask::Create(request->table_name(), row_count);
    ::milvus::Status grpc_status;
    RequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_table_row_count(row_count);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::ShowTables(::grpc::ServerContext* context, const ::milvus::grpc::Command* request, ::grpc::ServerWriter< ::milvus::grpc::TableName>* writer) {
    BaseTaskPtr task_ptr = ShowTablesTask::Create(*writer);
    RequestScheduler::ExecTask(task_ptr, nullptr);
    return ::grpc::Status::OK;
}

::grpc::Status
RequestHandler::Ping(::grpc::ServerContext* context, const ::milvus::grpc::Command* request, ::milvus::grpc::ServerStatus* response) {
    std::string result;
    BaseTaskPtr task_ptr = PingTask::Create(request->cmd(), result);
    ::milvus::Status grpc_status;
    RequestScheduler::ExecTask(task_ptr, &grpc_status);
    response->set_info(result);
    response->mutable_status()->set_reason(grpc_status.reason());
    response->mutable_status()->set_error_code(grpc_status.error_code());
    return ::grpc::Status::OK;
}


}
}
}