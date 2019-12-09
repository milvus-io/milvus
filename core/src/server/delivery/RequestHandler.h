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
    static Status
    CreateTable(const std::string& table_name, int64_t dimension, int32_t index_file_size, int32_t metric_type);

    static Status
    HasTable(const std::string& table_name, bool& has_table);

    static Status
    DropTable(const std::string& table_name);

    static Status
    CreateIndex(const std::string& table_name, int32_t index_type, int32_t nlist);

    static Status
    Insert(const std::string& table_name,
           std::vector<std::vector<float>>& records_array,
           std::vector<int64_t>& id_array,
           const std::string& partition_tag,
           std::vector<int64_t>& id_out_array);

    static Status
    ShowTables(std::vector<std::string>& tables);

    static Status
    Search(const std::string& table_name,
           const std::vector<std::vector<float>>& record_array,
           const std::vector<std::pair<std::string, std::string>>& range_list,
           int64_t topk,
           int64_t nprobe,
           const std::vector<std::string>& partition_list,
           const std::vector<std::string>& file_id_list,
           TopKQueryResult& result);

    static Status
    DescribeTable(const std::string& table_name, TableSchema& table_schema);

    static Status
    CountTable(const std::string& table_name, int64_t& count);
};

} // namespace server
} // namespace milvus

