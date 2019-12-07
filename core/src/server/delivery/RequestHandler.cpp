//
// Created by yhz on 2019/12/6.
//

#include "RequestHandler.h"
#include "RequestScheduler.h"
#include "request/CreateTableRequest.h"
#include "request/HasTableRequest.h"
#include "request/DropTableRequest.h"
#include "request/CreateIndexRequest.h"
#include "request/InsertRequest.h"
#include "request/SearchRequest.h"
#include "request/DescribeTableRequest.h"
#include "request/CountTableRequest.h"
#include "request/ShowTablesRequest.h"
#include "request/CmdRequest.h"
#include "request/DeleteByDateRequest.h"
#include "request/PreloadTableRequest.h"
#include "request/DescribeIndexRequest.h"
#include "request/DropIndexRequest.h"
#include "request/CreatePartitionRequest.h"
#include "request/ShowPartitionsRequest.h"
#include "request/DropPartitionRequest.h"
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

Status
RequestHandler::DropTable(const std::string& table_name) {
    BaseRequestPtr request_ptr = DropTableRequest::Create(table_name);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::CreateIndex(const std::string& table_name, int32_t index_type, int32_t nlist) {
    BaseRequestPtr request_ptr = CreateIndexRequest::Create(table_name, index_type, nlist);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::Insert(const std::string& table_name,
                       std::vector<std::vector<float>>& records_array,
                       std::vector<int64_t>& id_array,
                       const std::string& partition_tag,
                       std::vector<int64_t>& id_out_array) {
    BaseRequestPtr request_ptr = InsertRequest::Create(table_name,
                                                       records_array,
                                                       id_array,
                                                       partition_tag,
                                                       id_out_array);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

} // namespace server
} // namespace milvus
