#pragma once

#include "Error.h"

namespace zilliz {
namespace milvus {
namespace server {

class ValidationUtil {
public:
    static ServerError
    ValidateTableName(const std::string &table_name);

    static ServerError
    ValidateTableDimension(int64_t dimension);

    static ServerError
    ValidateTableIndexType(int32_t index_type);

    static ServerError
    ValidateGpuIndex(uint32_t gpu_index);

    static ServerError
    GetGpuMemory(uint32_t gpu_index, size_t &memory);
};

}
}
}