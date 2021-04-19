#pragma once
#include <optional>
#include "segcore/SegmentSmallIndex.h"
#include <boost/dynamic_bitset.hpp>

namespace milvus::query {
using BitmapChunk = boost::dynamic_bitset<>;
using BitmapSimple = std::deque<BitmapChunk>;

// note: c++17 don't support optional ref
Status
QueryBruteForceImpl(const segcore::SegmentSmallIndex& segment,
                    const QueryInfo& info,
                    const float* query_data,
                    int64_t num_queries,
                    Timestamp timestamp,
                    std::optional<const BitmapSimple*> bitmap_opt,
                    segcore::QueryResult& results);
}  // namespace milvus::query
