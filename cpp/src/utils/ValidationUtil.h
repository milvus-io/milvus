#pragma once

#include "Error.h"

namespace zilliz {
namespace milvus {
namespace server {

ServerError
ValidateTableName(const std::string& table_name);

ServerError
ValidateTableDimension(int64_t dimension);

ServerError
ValidateTableIndexType(int32_t index_type);

}
}
}