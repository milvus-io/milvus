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

Status
RequestHandler::ShowTables(std::vector<std::string>& tables) {
    BaseRequestPtr request_ptr = ShowTablesRequest::Create(tables);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::Search(const std::string& table_name,
                       const std::vector<std::vector<float>>& record_array,
                       const std::vector<std::pair<std::string, std::string>>& range_list,
                       int64_t topk,
                       int64_t nprobe,
                       const std::vector<std::string>& partition_list,
                       const std::vector<std::string>& file_id_list,
                       TopKQueryResult& result) {
    BaseRequestPtr request_ptr = SearchRequest::Create(table_name,
                                                       record_array,
                                                       range_list,
                                                       topk,
                                                       nprobe,
                                                       partition_list,
                                                       file_id_list,
                                                       result);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DescribeTable(const std::string& table_name, TableSchema& table_schema) {
    BaseRequestPtr request_ptr = DescribeTableRequest::Create(table_name, table_schema);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::CountTable(const std::string& table_name, int64_t& count) {
    BaseRequestPtr request_ptr = CountTableRequest::Create(table_name, count);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::Cmd(const std::string& cmd, std::string& reply) {
    BaseRequestPtr request_ptr = CmdRequest::Create(cmd, reply);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}


Status
RequestHandler::DeleteByRange(const std::string& table_name, const Range& range) {
    BaseRequestPtr request_ptr = DeleteByDateRequest::Create(table_name, range);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::PreloadTable(const std::string& table_name) {
    BaseRequestPtr request_ptr = PreloadTableRequest::Create(table_name);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DescribeIndex(const std::string& table_name, IndexParam& param) {
    BaseRequestPtr request_ptr = DescribeIndexRequest::Create(table_name, param);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DropIndex(const std::string& table_name) {
    BaseRequestPtr request_ptr = DropIndexRequest::Create(table_name);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::CreatePartition(const std::string& table_name,
                                const std::string& partition_name,
                                const std::string& tag) {
    BaseRequestPtr request_ptr = CreatePartitionRequest::Create(table_name, partition_name, tag);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::ShowPartitions(const std::string& table_name, std::vector<PartitionParam>& partitions) {
    BaseRequestPtr request_ptr = ShowPartitionsRequest::Create(table_name, partitions);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

Status
RequestHandler::DropPartition(const std::string& table_name,
                              const std::string& partition_name,
                              const std::string& tag) {
    BaseRequestPtr request_ptr = DropPartitionRequest::Create(table_name, partition_name, tag);
    RequestScheduler::ExecRequest(request_ptr);

    return request_ptr->status();
}

} // namespace server
} // namespace milvus
