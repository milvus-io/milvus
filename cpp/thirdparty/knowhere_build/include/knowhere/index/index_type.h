#pragma once


namespace zilliz {
namespace knowhere {


enum class IndexType {
    kUnknown = 0,
    kVecIdxBegin = 100,
    kVecIVFFlat = kVecIdxBegin,
    kVecIdxEnd,
};


} // namespace knowhere
} // namespace zilliz
