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

#include "query/CachedSearchIterator.h"
#include "query/SearchBruteForce.h"
#include <algorithm>

namespace milvus::query {

CachedSearchIterator::CachedSearchIterator(
    const milvus::index::VectorIndex& index,
    const knowhere::DataSetPtr& query_ds,
    const SearchInfo& search_info,
    const BitsetView& bitset) {
    if (query_ds == nullptr) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Query dataset is nullptr, cannot initialize iterator");
    }
    nq_ = query_ds->GetRows();
    Init(search_info);

    auto search_json = index.PrepareSearchParams(search_info);
    index::CheckAndUpdateKnowhereRangeSearchParam(
        search_info, batch_size_, index.GetMetricType(), search_json);

    auto expected_iterators =
        index.VectorIterators(query_ds, search_json, bitset);
    if (expected_iterators.has_value()) {
        iterators_ = std::move(expected_iterators.value());
    } else {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Failed to create iterators from index");
    }
}

CachedSearchIterator::CachedSearchIterator(
    const dataset::SearchDataset& query_ds,
    const dataset::RawDataset& raw_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    const milvus::DataType& data_type) {
    nq_ = query_ds.num_queries;
    Init(search_info);

    auto expected_iterators = GetBruteForceSearchIterators(
        query_ds, raw_ds, search_info, index_info, bitset, data_type);
    if (expected_iterators.has_value()) {
        iterators_ = std::move(expected_iterators.value());
    } else {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Failed to create iterators from index");
    }
}

void
CachedSearchIterator::InitializeChunkedIterators(
    const dataset::SearchDataset& query_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    const milvus::DataType& data_type,
    const GetChunkDataFunc& get_chunk_data) {
    int64_t offset = 0;
    chunked_heaps_.resize(nq_);
    for (int64_t chunk_id = 0; chunk_id < num_chunks_; ++chunk_id) {
        auto [chunk_data, chunk_size] = get_chunk_data(chunk_id);
        auto sub_data = query::dataset::RawDataset{
            offset, query_ds.dim, chunk_size, chunk_data};

        auto expected_iterators = GetBruteForceSearchIterators(
            query_ds, sub_data, search_info, index_info, bitset, data_type);
        if (expected_iterators.has_value()) {
            auto& chunk_iterators = expected_iterators.value();
            iterators_.insert(iterators_.end(),
                              std::make_move_iterator(chunk_iterators.begin()),
                              std::make_move_iterator(chunk_iterators.end()));
        } else {
            PanicInfo(ErrorCode::UnexpectedError,
                      "Failed to create iterators from index");
        }
        offset += chunk_size;
    }
}

CachedSearchIterator::CachedSearchIterator(
    const dataset::SearchDataset& query_ds,
    const segcore::VectorBase* vec_data,
    const int64_t row_count,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    const milvus::DataType& data_type) {
    if (vec_data == nullptr) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Vector data is nullptr, cannot initialize iterator");
    }

    if (row_count <= 0) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Number of rows is 0, cannot initialize iterator");
    }

    const int64_t vec_size_per_chunk = vec_data->get_size_per_chunk();
    num_chunks_ = upper_div(row_count, vec_size_per_chunk);
    nq_ = query_ds.num_queries;
    Init(search_info);

    iterators_.reserve(nq_ * num_chunks_);
    InitializeChunkedIterators(
        query_ds,
        search_info,
        index_info,
        bitset,
        data_type,
        [&vec_data, vec_size_per_chunk, row_count](
            int64_t chunk_id) -> std::pair<const void*, int64_t> {
            const auto chunk_data = vec_data->get_chunk_data(chunk_id);
            int64_t chunk_size = std::min(
                vec_size_per_chunk, row_count - chunk_id * vec_size_per_chunk);
            return {chunk_data, chunk_size};
        });
}

CachedSearchIterator::CachedSearchIterator(
    const std::shared_ptr<ChunkedColumnBase>& column,
    const dataset::SearchDataset& query_ds,
    const SearchInfo& search_info,
    const std::map<std::string, std::string>& index_info,
    const BitsetView& bitset,
    const milvus::DataType& data_type) {
    if (column == nullptr) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Column is nullptr, cannot initialize iterator");
    }

    num_chunks_ = column->num_chunks();
    nq_ = query_ds.num_queries;
    Init(search_info);

    iterators_.reserve(nq_ * num_chunks_);
    InitializeChunkedIterators(
        query_ds,
        search_info,
        index_info,
        bitset,
        data_type,
        [&column](int64_t chunk_id) {
            const char* chunk_data = column->Data(chunk_id);
            int64_t chunk_size = column->chunk_row_nums(chunk_id);
            return std::make_pair(static_cast<const void*>(chunk_data),
                                  chunk_size);
        });
}

void
CachedSearchIterator::NextBatch(const SearchInfo& search_info,
                                SearchResult& search_result) {
    if (iterators_.empty()) {
        return;
    }

    if (iterators_.size() != nq_ * num_chunks_) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Iterator size mismatch, expect %d, but got %d",
                  nq_ * num_chunks_,
                  iterators_.size());
    }

    ValidateSearchInfo(search_info);

    search_result.total_nq_ = nq_;
    search_result.unity_topK_ = batch_size_;
    search_result.seg_offsets_.resize(nq_ * batch_size_);
    search_result.distances_.resize(nq_ * batch_size_);

    for (size_t query_idx = 0; query_idx < nq_; ++query_idx) {
        auto rst = GetBatchedNextResults(query_idx, search_info);
        WriteSingleQuerySearchResult(
            search_result, query_idx, rst, search_info.round_decimal_);
    }
}

void
CachedSearchIterator::ValidateSearchInfo(const SearchInfo& search_info) {
    if (!search_info.iterator_v2_info_.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Iterator v2 SearchInfo is not set");
    }

    auto iterator_v2_info = search_info.iterator_v2_info_.value();
    if (iterator_v2_info.batch_size != batch_size_) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Batch size mismatch, expect %d, but got %d",
                  batch_size_,
                  iterator_v2_info.batch_size);
    }
}

std::optional<CachedSearchIterator::DisIdPair>
CachedSearchIterator::GetNextValidResult(
    const size_t iterator_idx,
    const std::optional<float>& last_bound,
    const std::optional<float>& radius,
    const std::optional<float>& range_filter) {
    auto& iterator = iterators_[iterator_idx];
    while (iterator->HasNext()) {
        auto result = ConvertIteratorResult(iterator->Next());
        if (IsValid(result, last_bound, radius, range_filter)) {
            return result;
        }
    }
    return std::nullopt;
}

// TODO: Optimize this method
void
CachedSearchIterator::MergeChunksResults(
    size_t query_idx,
    const std::optional<float>& last_bound,
    const std::optional<float>& radius,
    const std::optional<float>& range_filter,
    std::vector<DisIdPair>& rst) {
    auto& heap = chunked_heaps_[query_idx];

    if (heap.empty()) {
        for (size_t chunk_id = 0; chunk_id < num_chunks_; ++chunk_id) {
            const size_t iterator_idx = query_idx + chunk_id * nq_;
            if (auto next_result = GetNextValidResult(
                    iterator_idx, last_bound, radius, range_filter);
                next_result.has_value()) {
                heap.emplace(iterator_idx, next_result.value());
            }
        }
    }

    while (!heap.empty() && rst.size() < batch_size_) {
        const auto [iterator_idx, cur_rst] = heap.top();
        heap.pop();

        // last_bound may change between NextBatch calls, discard any invalid results
        if (!IsValid(cur_rst, last_bound, radius, range_filter)) {
            continue;
        }
        rst.emplace_back(cur_rst);

        if (auto next_result = GetNextValidResult(
                iterator_idx, last_bound, radius, range_filter);
            next_result.has_value()) {
            heap.emplace(iterator_idx, next_result.value());
        }
    }
}

std::vector<CachedSearchIterator::DisIdPair>
CachedSearchIterator::GetBatchedNextResults(size_t query_idx,
                                            const SearchInfo& search_info) {
    auto last_bound = ConvertIncomingDistance(
        search_info.iterator_v2_info_.value().last_bound);
    auto radius = ConvertIncomingDistance(
        index::GetValueFromConfig<float>(search_info.search_params_, RADIUS));
    auto range_filter =
        ConvertIncomingDistance(index::GetValueFromConfig<float>(
            search_info.search_params_, RANGE_FILTER));

    std::vector<DisIdPair> rst;
    rst.reserve(batch_size_);

    if (num_chunks_ == 1) {
        auto& iterator = iterators_[query_idx];
        while (iterator->HasNext() && rst.size() < batch_size_) {
            auto result = ConvertIteratorResult(iterator->Next());
            if (IsValid(result, last_bound, radius, range_filter)) {
                rst.emplace_back(result);
            }
        }
    } else {
        MergeChunksResults(query_idx, last_bound, radius, range_filter, rst);
    }
    std::sort(rst.begin(), rst.end());
    if (sign_ == -1) {
        std::for_each(rst.begin(), rst.end(), [this](DisIdPair& x) {
            x.first = x.first * sign_;
        });
    }
    while (rst.size() < batch_size_) {
        rst.emplace_back(1.0f / 0.0f, -1);
    }
    return rst;
}

void
CachedSearchIterator::WriteSingleQuerySearchResult(
    SearchResult& search_result,
    const size_t idx,
    std::vector<DisIdPair>& rst,
    const int64_t round_decimal) {
    const float multiplier = pow(10.0, round_decimal);

    std::transform(rst.begin(),
                   rst.end(),
                   search_result.distances_.begin() + idx * batch_size_,
                   [multiplier, round_decimal](DisIdPair& x) {
                       if (round_decimal != -1) {
                           x.first =
                               std::round(x.first * multiplier) / multiplier;
                       }
                       return x.first;
                   });

    std::transform(rst.begin(),
                   rst.end(),
                   search_result.seg_offsets_.begin() + idx * batch_size_,
                   [](const DisIdPair& x) { return x.second; });
}

void
CachedSearchIterator::Init(const SearchInfo& search_info) {
    if (!search_info.iterator_v2_info_.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Iterator v2 info is not set, cannot initialize iterator");
    }

    auto iterator_v2_info = search_info.iterator_v2_info_.value();
    if (iterator_v2_info.batch_size == 0) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Batch size is 0, cannot initialize iterator");
    }
    batch_size_ = iterator_v2_info.batch_size;

    if (search_info.metric_type_.empty()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Metric type is empty, cannot initialize iterator");
    }
    if (PositivelyRelated(search_info.metric_type_)) {
        sign_ = -1;
    } else {
        sign_ = 1;
    }

    if (nq_ == 0) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "Number of queries is 0, cannot initialize iterator");
    }

    // disable multi-query for now
    if (nq_ > 1) {
        PanicInfo(
            ErrorCode::UnexpectedError,
            "Number of queries is greater than 1, cannot initialize iterator");
    }
}

}  // namespace milvus::query