#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "knowhere/dataset.h"

namespace knowhere {

inline constexpr float FloatAccuracy = 1e-6F;
inline constexpr uint64_t seed = 0xc70f6907UL;

inline bool
IsMetricType(const std::string& str, const MetricType& metric_type) {
    return strcasecmp(str.c_str(), metric_type.c_str()) == 0;
}

inline bool
IsFlatIndex(const IndexType& index_type) {
    return index_type == IndexEnum::INDEX_FAISS_IDMAP ||
           index_type == IndexEnum::INDEX_FAISS_BIN_IDMAP;
}

inline float
NormalizeVec(float*, int32_t) {
    return 1.0F;
}

inline std::vector<float>
NormalizeVecs(float*, size_t rows, int32_t) {
    return std::vector<float>(rows, 1.0F);
}

inline void
Normalize(const DataSet&) {
}

inline uint64_t
hash_vec(const float* x, size_t d) {
    uint64_t h = seed;
    for (size_t i = 0; i < d; ++i) {
        auto bits = reinterpret_cast<const uint32_t*>(x + i);
        h = h * 13331 + *bits;
    }
    return h;
}

inline uint64_t
hash_binary_vec(const uint8_t* x, size_t d) {
    size_t len = (d + 7) / 8;
    uint64_t h = seed;
    for (size_t i = 0; i < len; ++i) {
        h = h * 13331 + x[i];
    }
    return h;
}

template <typename T>
inline T
round_down(const T value, const T align) {
    return value / align * align;
}

}  // namespace knowhere
