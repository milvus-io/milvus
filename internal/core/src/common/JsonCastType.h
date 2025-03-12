#pragma once

#include <string>
#include "common/EasyAssert.h"
namespace milvus {

enum class JsonCastType { BOOL, DOUBLE, VARCHAR };

inline auto
format_as(JsonCastType f) {
    return fmt::underlying(f);
}

inline const std::unordered_map<std::string, JsonCastType> JsonCastTypeMap = {
    {"BOOL", JsonCastType::BOOL},
    {"DOUBLE", JsonCastType::DOUBLE},
    {"VARCHAR", JsonCastType::VARCHAR}};

inline JsonCastType
ConvertToJsonCastType(const std::string& str) {
    auto it = JsonCastTypeMap.find(str);
    if (it != JsonCastTypeMap.end()) {
        return it->second;
    }
    PanicInfo(Unsupported, "Invalid json cast type: " + str);
}

}  // namespace milvus