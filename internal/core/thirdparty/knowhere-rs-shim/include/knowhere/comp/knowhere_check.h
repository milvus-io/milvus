#pragma once

#include <string>

#include "knowhere/comp/index_param.h"

namespace knowhere {

enum class VecType {
    BINARY_VECTOR = 100,
    FLOAT_VECTOR = 101,
    FLOAT16_VECTOR = 102,
    BFLOAT16_VECTOR = 103,
    SPARSE_FLOAT_VECTOR = 104,
    INT8_VECTOR = 105,
    VECTOR_ARRAY = 106,
};

class KnowhereCheck {
 public:
    static bool
    IndexTypeAndDataTypeCheck(const std::string&, VecType, bool) {
        return true;
    }

    static bool
    SupportMmapIndexTypeCheck(const std::string&) {
        return false;
    }
};

}  // namespace knowhere
