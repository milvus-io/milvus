//
// Created by yhz on 2019/12/6.
//

#include "RequestHandler.h"
#include "RequestScheduler.h"
#include "request/CreateTableRequest.h"
#include "request/HasTableRequest.h"
#include "request/BaseRequest.h"

namespace milvus {
namespace server {

Status
RequestHandler::CreateTable(const std::string& table_name,
                            int64_t dimension,
                            int32_t index_file_size,
                            int32_t metric_type) {
    BaseRequestPtr request_ptr = CreateTableRequest::Create(table_name, dimension, index_file_size, metric_type);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::HasTable(const std::string& table_name, bool& has_table) {
    BaseRequestPtr request_ptr = HasTableRequest::Create(table_name, has_table);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

} // namespace server
} // namespace milvus
