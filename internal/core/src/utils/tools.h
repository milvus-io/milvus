#pragma once
#include <cstdint>
#include "EasyAssert.h"
namespace milvus {
inline int64_t
upper_align(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = (value + align - 1) / align;
    return groups * align;
}

inline int64_t
upper_div(int64_t value, int64_t align) {
    Assert(align > 0);
    auto groups = (value + align - 1) / align;
    return groups;
}

}  // namespace milvus
