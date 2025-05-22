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

#include "Reduce.h"

#include "log/Log.h"
#include <cstdint>
#include <vector>

#include "common/EasyAssert.h"
#include "monitor/prometheus_client.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Utils.h"
#include "segcore/pkVisitor.h"
#include "segcore/ReduceUtils.h"

namespace milvus::segcore {

void
ReduceHelper::Initialize() {
    AssertInfo(search_results_.size() > 0, "empty search result");
    AssertInfo(slice_nqs_.size() > 0, "empty slice_nqs");
    AssertInfo(slice_nqs_.size() == slice_topKs_.size(),
               "unaligned slice_nqs and slice_topKs");

    total_nq_ = search_results_[0]->total_nq_;
    num_segments_ = search_results_.size();
    num_slices_ = slice_nqs_.size();

    // prefix sum, get slices offsets
    AssertInfo(num_slices_ > 0, "empty slice_nqs is not allowed");
    slice_nqs_prefix_sum_.resize(num_slices_ + 1);
    std::partial_sum(slice_nqs_.begin(),
                     slice_nqs_.end(),
                     slice_nqs_prefix_sum_.begin() + 1);
    AssertInfo(slice_nqs_prefix_sum_[num_slices_] == total_nq_,
               "illegal req sizes, slice_nqs_prefix_sum_[last] = " +
                   std::to_string(slice_nqs_prefix_sum_[num_slices_]) +
                   ", total_nq = " + std::to_string(total_nq_));

    // init final_search_records and final_read_topKs
    final_search_records_.resize(num_segments_);
    for (auto& search_record : final_search_records_) {
        search_record.resize(total_nq_);
    }
}

void
ReduceHelper::Reduce() {
    FillPrimaryKey();
    ReduceResultData();
    RefreshSearchResults();
    FillEntryData();
}

void
ReduceHelper::Marshal() {
    tracer::AutoSpan span("ReduceHelper::Marshal", tracer::GetRootSpan());
    // get search result data blobs of slices
    search_result_data_blobs_ =
        std::make_unique<milvus::segcore::SearchResultDataBlobs>();
    search_result_data_blobs_->blobs.resize(num_slices_);
    for (int i = 0; i < num_slices_; i++) {
        auto proto = GetSearchResultDataSlice(i);
        search_result_data_blobs_->blobs[i] = proto;
    }
}

void
ReduceHelper::FilterInvalidSearchResult(SearchResult* search_result) {
    auto nq = search_result->total_nq_;
    auto topK = search_result->unity_topK_;
    AssertInfo(search_result->seg_offsets_.size() == nq * topK,
               "wrong seg offsets size, size = " +
                   std::to_string(search_result->seg_offsets_.size()) +
                   ", expected size = " + std::to_string(nq * topK));
    AssertInfo(search_result->distances_.size() == nq * topK,
               "wrong distances size, size = " +
                   std::to_string(search_result->distances_.size()) +
                   ", expected size = " + std::to_string(nq * topK));
    std::vector<int64_t> real_topks(nq, 0);
    uint32_t valid_index = 0;
    auto segment = static_cast<SegmentInterface*>(search_result->segment_);
    auto& offsets = search_result->seg_offsets_;
    auto& distances = search_result->distances_;

    int segment_row_count = segment->get_row_count();
    //1. for sealed segment, segment_row_count will not change as delete records will take effect as bitset
    //2. for growing segment, segment_row_count is the minimum position acknowledged, which will only increase after
    //the time at which the search operation is executed, so it's safe here to keep this value inside stack
    for (auto i = 0; i < nq; ++i) {
        for (auto j = 0; j < topK; ++j) {
            auto index = i * topK + j;
            if (offsets[index] != INVALID_SEG_OFFSET) {
                AssertInfo(
                    0 <= offsets[index] && offsets[index] < segment_row_count,
                    fmt::format("invalid offset {}, segment {} with "
                                "rows num {}, data or index corruption",
                                offsets[index],
                                segment->get_segment_id(),
                                segment_row_count));
                real_topks[i]++;
                offsets[valid_index] = offsets[index];
                distances[valid_index] = distances[index];
                valid_index++;
            }
        }
    }
    offsets.resize(valid_index);
    distances.resize(valid_index);
    search_result->topk_per_nq_prefix_sum_.resize(nq + 1);
    std::partial_sum(real_topks.begin(),
                     real_topks.end(),
                     search_result->topk_per_nq_prefix_sum_.begin() + 1);
}

void
ReduceHelper::FillPrimaryKey() {
    tracer::AutoSpan span("ReduceHelper::FillPrimaryKey",
                          tracer::GetRootSpan());
    // get primary keys for duplicates removal
    uint32_t valid_index = 0;
    for (auto& search_result : search_results_) {
        // skip when results num is 0
        if (search_result->unity_topK_ == 0) {
            continue;
        }
        FilterInvalidSearchResult(search_result);
        LOG_DEBUG("the size of search result: {}",
                  search_result->seg_offsets_.size());
        auto segment = static_cast<SegmentInterface*>(search_result->segment_);
        if (search_result->get_total_result_count() > 0) {
            segment->FillPrimaryKeys(plan_, *search_result);
            search_results_[valid_index++] = search_result;
        }
    }
    search_results_.resize(valid_index);
    num_segments_ = search_results_.size();
}

void
ReduceHelper::RefreshSearchResults() {
    tracer::AutoSpan span("ReduceHelper::RefreshSearchResults",
                          tracer::GetRootSpan());
    for (int i = 0; i < num_segments_; i++) {
        std::vector<int64_t> real_topks(total_nq_, 0);
        auto search_result = search_results_[i];
        if (search_result->result_offsets_.size() != 0) {
            RefreshSingleSearchResult(search_result, i, real_topks);
        }
        std::partial_sum(real_topks.begin(),
                         real_topks.end(),
                         search_result->topk_per_nq_prefix_sum_.begin() + 1);
    }
}

void
ReduceHelper::RefreshSingleSearchResult(SearchResult* search_result,
                                        int seg_res_idx,
                                        std::vector<int64_t>& real_topks) {
    uint32_t index = 0;
    for (int j = 0; j < total_nq_; j++) {
        for (auto offset : final_search_records_[seg_res_idx][j]) {
            search_result->primary_keys_[index] =
                search_result->primary_keys_[offset];
            search_result->distances_[index] =
                search_result->distances_[offset];
            search_result->seg_offsets_[index] =
                search_result->seg_offsets_[offset];
            index++;
            real_topks[j]++;
        }
    }
    search_result->primary_keys_.resize(index);
    search_result->distances_.resize(index);
    search_result->seg_offsets_.resize(index);
}

void
ReduceHelper::FillEntryData() {
    tracer::AutoSpan span("ReduceHelper::FillEntryData", tracer::GetRootSpan());
    for (auto search_result : search_results_) {
        auto segment = static_cast<milvus::segcore::SegmentInterface*>(
            search_result->segment_);
        std::chrono::high_resolution_clock::time_point get_target_entry_start =
            std::chrono::high_resolution_clock::now();
        segment->FillTargetEntry(plan_, *search_result);
        std::chrono::high_resolution_clock::time_point get_target_entry_end =
            std::chrono::high_resolution_clock::now();
        double get_entry_cost =
            std::chrono::duration<double, std::micro>(get_target_entry_end -
                                                      get_target_entry_start)
                .count();
        monitor::internal_core_search_get_target_entry_latency.Observe(
            get_entry_cost / 1000);
    }
}

int64_t
ReduceHelper::ReduceSearchResultForOneNQ(int64_t qi,
                                         int64_t topk,
                                         int64_t& offset) {
    std::priority_queue<SearchResultPair*,
                        std::vector<SearchResultPair*>,
                        SearchResultPairComparator>
        heap;
    pk_set_.clear();
    pairs_.clear();

    pairs_.reserve(num_segments_);
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        auto offset_beg = search_result->topk_per_nq_prefix_sum_[qi];
        auto offset_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
        if (offset_beg == offset_end) {
            continue;
        }
        auto primary_key = search_result->primary_keys_[offset_beg];
        auto distance = search_result->distances_[offset_beg];
        pairs_.emplace_back(
            primary_key, distance, search_result, i, offset_beg, offset_end);
        heap.push(&pairs_.back());
    }

    // nq has no results for all segments
    if (heap.size() == 0) {
        return 0;
    }

    int64_t dup_cnt = 0;
    auto start = offset;
    while (offset - start < topk && !heap.empty()) {
        auto pilot = heap.top();
        heap.pop();

        auto index = pilot->segment_index_;
        auto pk = pilot->primary_key_;
        // no valid search result for this nq, break to next
        if (pk == INVALID_PK) {
            break;
        }
        // remove duplicates
        if (pk_set_.count(pk) == 0) {
            pilot->search_result_->result_offsets_.push_back(offset++);
            final_search_records_[index][qi].push_back(pilot->offset_);
            pk_set_.insert(pk);
        } else {
            // skip entity with same primary key
            dup_cnt++;
        }
        pilot->advance();
        if (pilot->primary_key_ != INVALID_PK) {
            heap.push(pilot);
        }
    }
    return dup_cnt;
}

void
ReduceHelper::ReduceResultData() {
    tracer::AutoSpan span("ReduceHelper::ReduceResultData",
                          tracer::GetRootSpan());
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        auto result_count = search_result->get_total_result_count();
        AssertInfo(search_result != nullptr,
                   "search result must not equal to nullptr");
        AssertInfo(search_result->distances_.size() == result_count,
                   "incorrect search result distance size");
        AssertInfo(search_result->seg_offsets_.size() == result_count,
                   "incorrect search result seg offset size");
        AssertInfo(search_result->primary_keys_.size() == result_count,
                   "incorrect search result primary key size");
    }

    int64_t filtered_count = 0;
    for (int64_t slice_index = 0; slice_index < num_slices_; slice_index++) {
        auto nq_begin = slice_nqs_prefix_sum_[slice_index];
        auto nq_end = slice_nqs_prefix_sum_[slice_index + 1];

        // reduce search results
        int64_t offset = 0;
        for (int64_t qi = nq_begin; qi < nq_end; qi++) {
            filtered_count += ReduceSearchResultForOneNQ(
                qi, slice_topKs_[slice_index], offset);
        }
    }
    if (filtered_count > 0) {
        LOG_DEBUG("skip duplicated search result, count = {}", filtered_count);
    }
}

void
ReduceHelper::FillOtherData(
    int result_count,
    int64_t nq_begin,
    int64_t nq_end,
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_res_data) {
    //simple batch reduce do nothing for other data
}

std::vector<char>
ReduceHelper::GetSearchResultDataSlice(int slice_index) {
    auto nq_begin = slice_nqs_prefix_sum_[slice_index];
    auto nq_end = slice_nqs_prefix_sum_[slice_index + 1];

    int64_t result_count = 0;
    int64_t all_search_count = 0;
    for (auto search_result : search_results_) {
        AssertInfo(search_result->topk_per_nq_prefix_sum_.size() ==
                       search_result->total_nq_ + 1,
                   "incorrect topk_per_nq_prefix_sum_ size in search result");
        result_count += search_result->topk_per_nq_prefix_sum_[nq_end] -
                        search_result->topk_per_nq_prefix_sum_[nq_begin];
        all_search_count += search_result->total_data_cnt_;
    }

    auto search_result_data =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    // set unify_topK and total_nq
    search_result_data->set_top_k(slice_topKs_[slice_index]);
    search_result_data->set_num_queries(nq_end - nq_begin);
    search_result_data->mutable_topks()->Resize(nq_end - nq_begin, 0);
    search_result_data->set_all_search_count(all_search_count);

    // `result_pairs` contains the SearchResult and result_offset info, used for filling output fields
    std::vector<MergeBase> result_pairs(result_count);

    // reserve space for pks
    auto primary_field_id =
        plan_->schema_.get_primary_field_id().value_or(milvus::FieldId(-1));
    AssertInfo(primary_field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    auto pk_type = plan_->schema_[primary_field_id].get_data_type();
    switch (pk_type) {
        case milvus::DataType::INT64: {
            auto ids = std::make_unique<milvus::proto::schema::LongArray>();
            ids->mutable_data()->Resize(result_count, 0);
            search_result_data->mutable_ids()->set_allocated_int_id(
                ids.release());
            break;
        }
        case milvus::DataType::VARCHAR: {
            auto ids = std::make_unique<milvus::proto::schema::StringArray>();
            std::vector<std::string> string_pks(result_count);
            // TODO: prevent mem copy
            *ids->mutable_data() = {string_pks.begin(), string_pks.end()};
            search_result_data->mutable_ids()->set_allocated_str_id(
                ids.release());
            break;
        }
        default: {
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported primary key type {}", pk_type));
        }
    }

    // reserve space for distances
    search_result_data->mutable_scores()->Resize(result_count, 0);

    // fill pks and distances
    for (auto qi = nq_begin; qi < nq_end; qi++) {
        int64_t topk_count = 0;
        for (auto search_result : search_results_) {
            AssertInfo(search_result != nullptr,
                       "null search result when reorganize");
            if (search_result->result_offsets_.size() == 0) {
                continue;
            }

            auto topk_start = search_result->topk_per_nq_prefix_sum_[qi];
            auto topk_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
            topk_count += topk_end - topk_start;

            for (auto ki = topk_start; ki < topk_end; ki++) {
                auto loc = search_result->result_offsets_[ki];
                AssertInfo(loc < result_count && loc >= 0,
                           "invalid loc when GetSearchResultDataSlice, loc = " +
                               std::to_string(loc) + ", result_count = " +
                               std::to_string(result_count));
                // set result pks
                switch (pk_type) {
                    case milvus::DataType::INT64: {
                        search_result_data->mutable_ids()
                            ->mutable_int_id()
                            ->mutable_data()
                            ->Set(loc,
                                  std::visit(Int64PKVisitor{},
                                             search_result->primary_keys_[ki]));
                        break;
                    }
                    case milvus::DataType::VARCHAR: {
                        *search_result_data->mutable_ids()
                             ->mutable_str_id()
                             ->mutable_data()
                             ->Mutable(loc) = std::visit(
                            StrPKVisitor{}, search_result->primary_keys_[ki]);
                        break;
                    }
                    default: {
                        PanicInfo(DataTypeInvalid,
                                  fmt::format("unsupported primary key type {}",
                                              pk_type));
                    }
                }

                search_result_data->mutable_scores()->Set(
                    loc, search_result->distances_[ki]);
                // set result offset to fill output fields data
                result_pairs[loc] = {&search_result->output_fields_data_, ki};
            }
        }

        // update result topKs
        search_result_data->mutable_topks()->Set(qi - nq_begin, topk_count);
    }

    AssertInfo(search_result_data->scores_size() == result_count,
               "wrong scores size, size = " +
                   std::to_string(search_result_data->scores_size()) +
                   ", expected size = " + std::to_string(result_count));
    // fill other wanted data
    FillOtherData(result_count, nq_begin, nq_end, search_result_data);

    // set output fields
    for (auto field_id : plan_->target_entries_) {
        auto& field_meta = plan_->schema_[field_id];
        auto field_data =
            milvus::segcore::MergeDataArray(result_pairs, field_meta);
        if (field_meta.get_data_type() == DataType::ARRAY) {
            field_data->mutable_scalars()
                ->mutable_array_data()
                ->set_element_type(
                    proto::schema::DataType(field_meta.get_element_type()));
        }
        search_result_data->mutable_fields_data()->AddAllocated(
            field_data.release());
    }

    // SearchResultData to blob
    auto size = search_result_data->ByteSizeLong();
    auto buffer = std::vector<char>(size);
    search_result_data->SerializePartialToArray(buffer.data(), size);

    return buffer;
}

}  // namespace milvus::segcore
