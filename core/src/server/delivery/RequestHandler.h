//
// Created by yhz on 2019/12/6.
//

#pragma once

#include "src/utils/Status.h"
#include "server/delivery/request/BaseRequest.h"

#include <vector>

namespace milvus {
namespace server {

class RequestHandler {

 public:
    Status
    CreateTable(const std::shared_ptr<Context>& context, const std::string& table_name,
                int64_t dimension, int32_t index_file_size, int32_t metric_type);

    Status
    HasTable(const std::shared_ptr<Context>& context, const std::string& table_name, bool& has_table);

    Status
    DropTable(const std::shared_ptr<Context>& context, const std::string& table_name);

    Status
    CreateIndex(const std::shared_ptr<Context>& context, const std::string& table_name, int32_t index_type, int32_t nlist);

    Status
    Insert(const std::shared_ptr<Context>& context, const std::string& table_name,
           std::vector<std::vector<float>>& records_array,
           std::vector<int64_t>& id_array,
           const std::string& partition_tag,
           std::vector<int64_t>& id_out_array);

    Status
    ShowTables(const std::shared_ptr<Context>& context, std::vector<std::string>& tables);

    Status
    Search(const std::shared_ptr<Context>& context, const std::string& table_name,
           const std::vector<std::vector<float>>& record_array,
           const std::vector<std::pair<std::string, std::string>>& range_list,
           int64_t topk, int64_t nprobe,
           const std::vector<std::string>& partition_list,
           const std::vector<std::string>& file_id_list,
           TopKQueryResult& result);

    Status
    DescribeTable(const std::shared_ptr<Context>& context, const std::string& table_name, TableSchema& table_schema);

    Status
    CountTable(const std::shared_ptr<Context>& context, const std::string& table_name, int64_t& count);

    Status
    Cmd(const std::shared_ptr<Context>& context, const std::string& cmd, std::string& reply);

    Status
    DeleteByRange(const std::shared_ptr<Context>& context, const std::string& table_name, const Range& range);

    Status
    PreloadTable(const std::shared_ptr<Context>& context, const std::string& table_name);

    Status
    DescribeIndex(const std::shared_ptr<Context>& context, const std::string& table_name, IndexParam& param);

    Status
    DropIndex(const std::shared_ptr<Context>& context, const std::string& table_name);

    Status
    CreatePartition(const std::shared_ptr<Context>& context, const std::string& table_name, const std::string& partition_name, const std::string& tag);

    Status
    ShowPartitions(const std::shared_ptr<Context>& context, const std::string& table_name, std::vector<PartitionParam>& partitions);

    Status
    DropPartition(const std::shared_ptr<Context>& context, const std::string& table_name, const std::string& partition_name, const std::string& tag);
};

} // namespace server
} // namespace milvus

