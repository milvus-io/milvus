// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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

#include <queue>
#include <unordered_set>

#include "common/Types.h"
#include "segcore/segment_c.h"
#include "query/PlanImpl.h"
#include "common/QueryResult.h"
#include "segcore/ReduceStructure.h"
#include "common/EasyAssert.h"

namespace milvus::segcore {
class MergedSearchResult {
 public:
    bool has_result_;
    std::vector<PkType> primary_keys_;
    std::vector<float> distances_;
    std::optional<std::vector<GroupByValueType>> group_by_values_;

    // set output fields data when filling target entity
    std::map<FieldId, std::unique_ptr<milvus::DataArray>> output_fields_data_;

    // used for reduce, filter invalid pk, get real topks count
    std::vector<size_t> topk_per_nq_prefix_sum_;
    // fill data during reducing search result
    std::vector<int64_t> result_offsets_;
    std::vector<int64_t> reduced_offsets_;
};

struct StreamSearchResultPair {
    milvus::PkType primary_key_;
    float distance_;
    milvus::SearchResult* search_result_;
    MergedSearchResult* merged_result_;
    int64_t segment_index_;
    int64_t offset_;
    int64_t offset_rb_;
    std::optional<milvus::GroupByValueType> group_by_value_;

    StreamSearchResultPair(milvus::PkType primary_key,
                           float distance,
                           SearchResult* result,
                           int64_t index,
                           int64_t lb,
                           int64_t rb)
        : StreamSearchResultPair(primary_key,
                                 distance,
                                 result,
                                 nullptr,
                                 index,
                                 lb,
                                 rb,
                                 std::nullopt) {
    }

    StreamSearchResultPair(
        milvus::PkType primary_key,
        float distance,
        SearchResult* result,
        MergedSearchResult* merged_result,
        int64_t index,
        int64_t lb,
        int64_t rb,
        std::optional<milvus::GroupByValueType> group_by_value)
        : primary_key_(std::move(primary_key)),
          distance_(distance),
          search_result_(result),
          merged_result_(merged_result),
          segment_index_(index),
          offset_(lb),
          offset_rb_(rb),
          group_by_value_(group_by_value) {
        AssertInfo(
            search_result_ != nullptr || merged_result_ != nullptr,
            "For a valid StreamSearchResult pair, "
            "at least one of merged_result_ or search_result_ is not nullptr");
    }

    bool
    operator>(const StreamSearchResultPair& other) const {
        if (std::fabs(distance_ - other.distance_) < 0.0000000119) {
            return primary_key_ < other.primary_key_;
        }
        return distance_ > other.distance_;
    }

    void
    advance() {
        offset_++;
        if (offset_ < offset_rb_) {
            if (search_result_ != nullptr) {
                primary_key_ = search_result_->primary_keys_.at(offset_);
                distance_ = search_result_->distances_.at(offset_);
                if (search_result_->group_by_values_.has_value() &&
                    offset_ < search_result_->group_by_values_.value().size()) {
                    group_by_value_ =
                        search_result_->group_by_values_.value().at(offset_);
                }
            } else {
                primary_key_ = merged_result_->primary_keys_.at(offset_);
                distance_ = merged_result_->distances_.at(offset_);
                if (merged_result_->group_by_values_.has_value() &&
                    offset_ < merged_result_->group_by_values_.value().size()) {
                    group_by_value_ =
                        merged_result_->group_by_values_.value().at(offset_);
                }
            }
        } else {
            primary_key_ = INVALID_PK;
            distance_ = std::numeric_limits<float>::min();
        }
    }
};

struct StreamSearchResultPairComparator {
    bool
    operator()(const std::shared_ptr<StreamSearchResultPair> lhs,
               const std::shared_ptr<StreamSearchResultPair> rhs) const {
        return (*rhs.get()) > (*lhs.get());
    }
};

class StreamReducerHelper {
 public:
    explicit StreamReducerHelper(milvus::query::Plan* plan,
                                 int64_t* slice_nqs,
                                 int64_t* slice_topKs,
                                 int64_t slice_num)
        : plan_(plan),
          slice_nqs_(slice_nqs, slice_nqs + slice_num),
          slice_topKs_(slice_topKs, slice_topKs + slice_num) {
        AssertInfo(slice_nqs_.size() > 0, "empty_nqs");
        AssertInfo(slice_nqs_.size() == slice_topKs_.size(),
                   "unaligned slice_nqs and slice_topKs");
        merged_search_result = std::make_unique<MergedSearchResult>();
        merged_search_result->has_result_ = false;
        num_slice_ = slice_nqs_.size();
        slice_nqs_prefix_sum_.resize(num_slice_ + 1);
        std::partial_sum(slice_nqs_.begin(),
                         slice_nqs_.end(),
                         slice_nqs_prefix_sum_.begin() + 1);
        total_nq_ = slice_nqs_prefix_sum_[num_slice_];
    }

    void
    SetSearchResultsToMerge(std::vector<SearchResult*>& search_results) {
        search_results_to_merge_ = search_results;
        num_segments_ = search_results_to_merge_.size();
        AssertInfo(num_segments_ > 0, "empty search result");
    }

 public:
    void
    MergeReduce();
    void*
    SerializeMergedResult();

 protected:
    void
    FilterSearchResults();

    void
    InitializeReduceRecords();

    void
    FillPrimaryKeys();

    void
    FilterInvalidSearchResult(SearchResult* search_result);

    void
    ReduceResultData();

 private:
    void
    RefreshSearchResult();

    void
    StreamReduceSearchResultForOneNQ(int64_t qi, int64_t topK, int64_t& offset);

    void
    FillEntryData();

    void
    AssembleMergedResult();

    std::vector<char>
    GetSearchResultDataSlice(int slice_index);

    void
    CleanReduceStatus();

    std::unique_ptr<MergedSearchResult> merged_search_result;
    milvus::query::Plan* plan_;
    std::vector<int64_t> slice_nqs_;
    std::vector<int64_t> slice_topKs_;
    std::vector<SearchResult*> search_results_to_merge_;
    int64_t num_segments_{0};
    int64_t num_slice_{0};
    std::vector<int64_t> slice_nqs_prefix_sum_;
    std::priority_queue<std::shared_ptr<StreamSearchResultPair>,
                        std::vector<std::shared_ptr<StreamSearchResultPair>>,
                        StreamSearchResultPairComparator>
        heap_;
    std::unordered_set<milvus::PkType> pk_set_;
    std::unordered_set<milvus::GroupByValueType> group_by_val_set_;
    std::vector<std::vector<std::vector<int64_t>>> final_search_records_;
    int64_t total_nq_{0};
};
}  // namespace milvus::segcore
