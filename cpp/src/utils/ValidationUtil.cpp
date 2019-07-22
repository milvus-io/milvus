#include <src/db/ExecutionEngine.h>
#include "ValidationUtil.h"
#include "Log.h"


namespace zilliz {
namespace milvus {
namespace server {

constexpr size_t table_name_size_limit = 255;
constexpr int64_t table_dimension_limit = 16384;

ServerError
ValidateTableName(const std::string &table_name) {

    // Table name shouldn't be empty.
    if (table_name.empty()) {
        SERVER_LOG_ERROR << "Empty table name";
        return SERVER_INVALID_TABLE_NAME;
    }

    // Table name size shouldn't exceed 16384.
    if (table_name.size() > table_name_size_limit) {
        SERVER_LOG_ERROR << "Table name size exceed the limitation";
        return SERVER_INVALID_TABLE_NAME;
    }

    // Table name first character should be underscore or character.
    char first_char = table_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        SERVER_LOG_ERROR << "Table name first character isn't underscore or character: " << first_char;
        return SERVER_INVALID_TABLE_NAME;
    }

    int64_t table_name_size = table_name.size();
    for (int64_t i = 1; i < table_name_size; ++i) {
        char name_char = table_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            SERVER_LOG_ERROR << "Table name character isn't underscore or alphanumber: " << name_char;
            return SERVER_INVALID_TABLE_NAME;
        }
    }

    return SERVER_SUCCESS;
}

ServerError
ValidateTableDimension(int64_t dimension) {
    if (dimension <= 0 || dimension > table_dimension_limit) {
        SERVER_LOG_ERROR << "Table dimension excceed the limitation: " << table_dimension_limit;
        return SERVER_INVALID_VECTOR_DIMENSION;
    } else {
        return SERVER_SUCCESS;
    }
}

ServerError
ValidateTableIndexType(int32_t index_type) {
    int engine_type = (int)engine::EngineType(index_type);
    if(engine_type <= 0 || engine_type > (int)engine::EngineType::MAX_VALUE) {
        return SERVER_INVALID_INDEX_TYPE;
    }

    return SERVER_SUCCESS;
}

}
}
}