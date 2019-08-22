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
    ValidateTableIndexNlist(int32_t nlist);

    static ServerError
    ValidateTableIndexFileSize(int32_t index_file_size);

    static ServerError
    ValidateTableIndexMetricType(int32_t metric_type);

    static ServerError
    ValidateGpuIndex(uint32_t gpu_index);

    static ServerError
    GetGpuMemory(uint32_t gpu_index, size_t &memory);
};

}
}
}