// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <utility>
#include "common/BitsetView.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "query/helper.h"
#include "segcore/ConcurrentVector.h"
#include "index/VectorIndex.h"

namespace milvus::query {

// This class is used to cache the search results from Knowhere
// search iterators and filter the results based on the last_bound,
// radius and range_filter.
// It provides a number of constructors to support different scenarios,
// including growing/sealed, chunked/non-chunked.
//
// It does not care about TopK in search_info
// The topk in SearchResult will be set to the batch_size for compatibility
//
// TODO: introduce the pool of results in the near future
// TODO: replace VectorIterator class
class CachedSearchIterator {
 public:
    // For sealed segment with vector index
    CachedSearchIterator(const milvus::index::VectorIndex& index,
                         const knowhere::DataSetPtr& dataset,
                         const SearchInfo& search_info,
                         const BitsetView& bitset);

    // For sealed segment, BF
    CachedSearchIterator(const dataset::SearchDataset& dataset,
                         const dataset::RawDataset& raw_ds,
                         const SearchInfo& search_info,
                         const std::map<std::string, std::string>& index_info,
                         const BitsetView& bitset,
                         const milvus::DataType& data_type);

    // For growing segment with chunked data, BF
    CachedSearchIterator(const dataset::SearchDataset& dataset,
                         const segcore::VectorBase* vec_data,
                         const int64_t row_count,
                         const SearchInfo& search_info,
                         const std::map<std::string, std::string>& index_info,
                         const BitsetView& bitset,
                         const milvus::DataType& data_type);

    // For sealed segment with chunked data, BF
    CachedSearchIterator(const std::shared_ptr<ChunkedColumnBase>& column,
                         const dataset::SearchDataset& dataset,
                         const SearchInfo& search_info,
                         const std::map<std::string, std::string>& index_info,
                         const BitsetView& bitset,
                         const milvus::DataType& data_type);

    // This method fetches the next batch of search results based on the provided search information
    // and updates the search_result object with the new batch of results.
    void
    NextBatch(const SearchInfo& search_info, SearchResult& search_result);

    // Disable copy and move
    CachedSearchIterator(const CachedSearchIterator&) = delete;
    CachedSearchIterator&
    operator=(const CachedSearchIterator&) = delete;
    CachedSearchIterator(CachedSearchIterator&&) = delete;
    CachedSearchIterator&
    operator=(CachedSearchIterator&&) = delete;

 private:
    using DisIdPair = std::pair<float, int64_t>;
    using IterIdx = size_t;
    using IterIdDisIdPair = std::pair<IterIdx, DisIdPair>;
    using GetChunkDataFunc =
        std::function<std::pair<const void*, int64_t>(int64_t)>;

    int64_t batch_size_ = 0;
    std::vector<knowhere::IndexNode::IteratorPtr> iterators_;
    int8_t sign_ = 1;
    size_t num_chunks_ = 1;
    size_t nq_ = 0;

    struct IterIdDisIdPairComparator {
        bool
        operator()(const IterIdDisIdPair& lhs, const IterIdDisIdPair& rhs) {
            if (lhs.second.first == rhs.second.first) {
                return lhs.second.second > rhs.second.second;
            }
            return lhs.second.first > rhs.second.first;
        }
    };
    std::vector<std::priority_queue<IterIdDisIdPair,
                                    std::vector<IterIdDisIdPair>,
                                    IterIdDisIdPairComparator>>
        chunked_heaps_;

    inline bool
    IsValid(const DisIdPair& result,
            const std::optional<float>& last_bound,
            const std::optional<float>& radius,
            const std::optional<float>& range_filter) {
        const float dist = result.first;
        const bool is_valid =
            !last_bound.has_value() || dist > last_bound.value();

        if (!radius.has_value()) {
            return is_valid;
        }

        if (!range_filter.has_value()) {
            return is_valid && dist < radius.value();
        }

        return is_valid && dist < radius.value() &&
               dist >= range_filter.value();
    }

    inline DisIdPair
    ConvertIteratorResult(const std::pair<int64_t, float>& iter_rst) {
        DisIdPair rst;
        rst.first = iter_rst.second * sign_;
        rst.second = iter_rst.first;
        return rst;
    }

    inline std::optional<float>
    ConvertIncomingDistance(std::optional<float> dist) {
        if (dist.has_value()) {
            dist = dist.value() * sign_;
        }
        return dist;
    }

    std::optional<DisIdPair>
    GetNextValidResult(size_t iterator_idx,
                       const std::optional<float>& last_bound,
                       const std::optional<float>& radius,
                       const std::optional<float>& range_filter);

    void
    MergeChunksResults(size_t query_idx,
                       const std::optional<float>& last_bound,
                       const std::optional<float>& radius,
                       const std::optional<float>& range_filter,
                       std::vector<DisIdPair>& rst);

    void
    ValidateSearchInfo(const SearchInfo& search_info);

    std::vector<DisIdPair>
    GetBatchedNextResults(size_t query_idx, const SearchInfo& search_info);

    void
    WriteSingleQuerySearchResult(SearchResult& search_result,
                                 const size_t idx,
                                 std::vector<DisIdPair>& rst,
                                 const int64_t round_decimal);

    void
    Init(const SearchInfo& search_info);

    void
    InitializeChunkedIterators(
        const dataset::SearchDataset& dataset,
        const SearchInfo& search_info,
        const std::map<std::string, std::string>& index_info,
        const BitsetView& bitset,
        const milvus::DataType& data_type,
        const GetChunkDataFunc& get_chunk_data);
};
}  // namespace milvus::query
