//
// Created by yhz on 2019/12/6.
//

#pragma once

#include "src/utils/Status.h"

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
    CreateIndex(const std::string table_name, int32_t index_type, int32_t nlist);

};

} // namespace server
} // namespace milvus

