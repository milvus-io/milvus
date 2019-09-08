#pragma once

#include "db/meta/MetaTypes.h"
#include "Error.h"

namespace zilliz {
namespace milvus {
namespace server {

class ValidationUtil {
public:
    static ErrorCode
    ValidateTableName(const std::string &table_name);

    static ErrorCode
    ValidateTableDimension(int64_t dimension);

    static ErrorCode
    ValidateTableIndexType(int32_t index_type);

    static ErrorCode
    ValidateTableIndexNlist(int32_t nlist);

    static ErrorCode
    ValidateTableIndexFileSize(int64_t index_file_size);

    static ErrorCode
    ValidateTableIndexMetricType(int32_t metric_type);

    static ErrorCode
    ValidateSearchTopk(int64_t top_k, const engine::meta::TableSchema& table_schema);

    static ErrorCode
    ValidateSearchNprobe(int64_t nprobe, const engine::meta::TableSchema& table_schema);

    static ErrorCode
    ValidateGpuIndex(uint32_t gpu_index);

    static ErrorCode
    GetGpuMemory(uint32_t gpu_index, size_t &memory);

    static ErrorCode
    ValidateConfig();
};

}
}
}