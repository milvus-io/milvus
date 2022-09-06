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

#include <cstdint>
#include <vector>
#include <algorithm>
#include <log/Log.h>

#include "Reduce.h"
#include "pkVisitor.h"
#include "SegmentInterface.h"
#include "ReduceStructure.h"
#include "Utils.h"

namespace milvus::segcore {

void
ReduceHelper::Initialize() {
    AssertInfo(search_results_.size() > 0, "empty search result");
    AssertInfo(slice_nqs_.size() > 0, "empty slice_nqs");
    AssertInfo(slice_nqs_.size() == slice_topKs_.size(), "unaligned slice_nqs and slice_topKs");

    total_nq_ = search_results_[0]->total_nq_;
    num_segments_ = search_results_.size();
    num_slices_ = slice_nqs_.size();

    // prefix sum, get slices offsets
    AssertInfo(num_slices_ > 0, "empty slice_nqs is not allowed");
    slice_nqs_prefix_sum_.resize(num_slices_ + 1);
    std::partial_sum(slice_nqs_.begin(), slice_nqs_.end(), slice_nqs_prefix_sum_.begin() + 1);
    AssertInfo(slice_nqs_prefix_sum_[num_slices_] == total_nq_, "illegal req sizes, slice_nqs_prefix_sum_[last] = " +
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
    RefreshSearchResult();
    FillEntryData();
}

void
ReduceHelper::Marshal() {
    // get search result data blobs of slices
    search_result_data_blobs_ = std::make_unique<milvus::segcore::SearchResultDataBlobs>();
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
               "wrong seg offsets size, size = " + std::to_string(search_result->seg_offsets_.size()) +
                   ", expected size = " + std::to_string(nq * topK));
    AssertInfo(search_result->distances_.size() == nq * topK,
               "wrong distances size, size = " + std::to_string(search_result->distances_.size()) +
                   ", expected size = " + std::to_string(nq * topK));
    std::vector<int64_t> real_topks(nq);
    std::vector<float> distances;
    std::vector<int64_t> seg_offsets;
    for (auto i = 0; i < nq; i++) {
        real_topks[i] = 0;
        for (auto j = 0; j < topK; j++) {
            auto offset = i * topK + j;
            if (search_result->seg_offsets_[offset] != INVALID_SEG_OFFSET) {
                real_topks[i]++;
                seg_offsets.push_back(search_result->seg_offsets_[offset]);
                distances.push_back(search_result->distances_[offset]);
            }
        }
    }

    search_result->distances_.swap(distances);
    search_result->seg_offsets_.swap(seg_offsets);
    search_result->topk_per_nq_prefix_sum_.resize(nq + 1);
    std::partial_sum(real_topks.begin(), real_topks.end(), search_result->topk_per_nq_prefix_sum_.begin() + 1);
}

void
ReduceHelper::FillPrimaryKey() {
    std::vector<SearchResult*> valid_search_results;
    // get primary keys for duplicates removal
    for (auto search_result : search_results_) {
        FilterInvalidSearchResult(search_result);
        if (search_result->get_total_result_count() > 0) {
            auto segment = static_cast<SegmentInterface*>(search_result->segment_);
            segment->FillPrimaryKeys(plan_, *search_result);
            valid_search_results.emplace_back(search_result);
        }
    }
    search_results_.swap(valid_search_results);
    num_segments_ = search_results_.size();
}

void
ReduceHelper::RefreshSearchResult() {
    for (int i = 0; i < num_segments_; i++) {
        std::vector<int64_t> real_topks(total_nq_, 0);
        auto search_result = search_results_[i];
        if (search_result->result_offsets_.size() != 0) {
            std::vector<milvus::PkType> primary_keys;
            std::vector<float> distances;
            std::vector<int64_t> seg_offsets;
            for (int j = 0; j < total_nq_; j++) {
                for (auto offset : final_search_records_[i][j]) {
                    primary_keys.push_back(search_result->primary_keys_[offset]);
                    distances.push_back(search_result->distances_[offset]);
                    seg_offsets.push_back(search_result->seg_offsets_[offset]);
                    real_topks[j]++;
                }
            }
            search_result->primary_keys_ = std::move(primary_keys);
            search_result->distances_ = std::move(distances);
            search_result->seg_offsets_ = std::move(seg_offsets);
        }
        std::partial_sum(real_topks.begin(), real_topks.end(), search_result->topk_per_nq_prefix_sum_.begin() + 1);
    }
}

void
ReduceHelper::FillEntryData() {
    for (auto search_result : search_results_) {
        auto segment = static_cast<milvus::segcore::SegmentInterface*>(search_result->segment_);
        segment->FillTargetEntry(plan_, *search_result);
    }
}

int64_t
ReduceHelper::ReduceSearchResultForOneNQ(int64_t qi, int64_t topk, int64_t& offset) {
    std::vector<SearchResultPair> result_pairs;
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        auto offset_beg = search_result->topk_per_nq_prefix_sum_[qi];
        auto offset_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
        if (offset_beg == offset_end) {
            continue;
        }
        auto primary_key = search_result->primary_keys_[offset_beg];
        auto distance = search_result->distances_[offset_beg];
        result_pairs.emplace_back(primary_key, distance, search_result, i, offset_beg, offset_end);
    }

    // nq has no results for all segments
    if (result_pairs.size() == 0) {
        return 0;
    }

    int64_t dup_cnt = 0;
    std::unordered_set<milvus::PkType> pk_set;
    int64_t prev_offset = offset;
    while (offset - prev_offset < topk) {
        std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
        auto& pilot = result_pairs[0];
        auto index = pilot.segment_index_;
        auto pk = pilot.primary_key_;
        // no valid search result for this nq, break to next
        if (pk == INVALID_PK) {
            break;
        }
        // remove duplicates
        if (pk_set.count(pk) == 0) {
            pilot.search_result_->result_offsets_.push_back(offset++);
            final_search_records_[index][qi].push_back(pilot.offset_);
            pk_set.insert(pk);
        } else {
            // skip entity with same primary key
            dup_cnt++;
        }
        pilot.reset();
    }
    return dup_cnt;
}

void
ReduceHelper::ReduceResultData() {
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        auto result_count = search_result->get_total_result_count();
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
        AssertInfo(search_result->distances_.size() == result_count, "incorrect search result distance size");
        AssertInfo(search_result->seg_offsets_.size() == result_count, "incorrect search result seg offset size");
        AssertInfo(search_result->primary_keys_.size() == result_count, "incorrect search result primary key size");
    }

    int64_t skip_dup_cnt = 0;
    for (int64_t slice_index = 0; slice_index < num_slices_; slice_index++) {
        auto nq_begin = slice_nqs_prefix_sum_[slice_index];
        auto nq_end = slice_nqs_prefix_sum_[slice_index + 1];

        // reduce search results
        int64_t result_offset = 0;
        for (int64_t qi = nq_begin; qi < nq_end; qi++) {
            skip_dup_cnt += ReduceSearchResultForOneNQ(qi, slice_topKs_[slice_index], result_offset);
        }
    }
    if (skip_dup_cnt > 0) {
        LOG_SEGCORE_DEBUG_ << "skip duplicated search result, count = " << skip_dup_cnt;
    }
}

std::vector<char>
ReduceHelper::GetSearchResultDataSlice(int slice_index) {
    auto nq_begin = slice_nqs_prefix_sum_[slice_index];
    auto nq_end = slice_nqs_prefix_sum_[slice_index + 1];

    int64_t result_count = 0;
    for (auto search_result : search_results_) {
        AssertInfo(search_result->topk_per_nq_prefix_sum_.size() == search_result->total_nq_ + 1,
                   "incorrect topk_per_nq_prefix_sum_ size in search result");
        result_count +=
            search_result->topk_per_nq_prefix_sum_[nq_end] - search_result->topk_per_nq_prefix_sum_[nq_begin];
    }

    auto search_result_data = std::make_unique<milvus::proto::schema::SearchResultData>();
    // set unify_topK and total_nq
    search_result_data->set_top_k(slice_topKs_[slice_index]);
    search_result_data->set_num_queries(nq_end - nq_begin);
    search_result_data->mutable_topks()->Resize(nq_end - nq_begin, 0);

    // `result_pairs` contains the SearchResult and result_offset info, used for filling output fields
    std::vector<std::pair<SearchResult*, int64_t>> result_pairs(result_count);

    // reserve space for pks
    auto primary_field_id = plan_->schema_.get_primary_field_id().value_or(milvus::FieldId(-1));
    AssertInfo(primary_field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    auto pk_type = plan_->schema_[primary_field_id].get_data_type();
    switch (pk_type) {
        case milvus::DataType::INT64: {
            auto ids = std::make_unique<milvus::proto::schema::LongArray>();
            ids->mutable_data()->Resize(result_count, 0);
            search_result_data->mutable_ids()->set_allocated_int_id(ids.release());
            break;
        }
        case milvus::DataType::VARCHAR: {
            auto ids = std::make_unique<milvus::proto::schema::StringArray>();
            std::vector<std::string> string_pks(result_count);
            // TODO: prevent mem copy
            *ids->mutable_data() = {string_pks.begin(), string_pks.end()};
            search_result_data->mutable_ids()->set_allocated_str_id(ids.release());
            break;
        }
        default: {
            PanicInfo("unsupported primary key type");
        }
    }

    // reserve space for distances
    search_result_data->mutable_scores()->Resize(result_count, 0);

    // fill pks and distances
    for (auto qi = nq_begin; qi < nq_end; qi++) {
        int64_t topk_count = 0;
        for (auto search_result : search_results_) {
            AssertInfo(search_result != nullptr, "null search result when reorganize");
            if (search_result->result_offsets_.size() == 0) {
                continue;
            }

            auto topk_start = search_result->topk_per_nq_prefix_sum_[qi];
            auto topk_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
            topk_count += topk_end - topk_start;

            for (auto ki = topk_start; ki < topk_end; ki++) {
                auto loc = search_result->result_offsets_[ki];
                AssertInfo(loc < result_count && loc >= 0,
                           "invalid loc when GetSearchResultDataSlice, loc = " + std::to_string(loc) +
                               ", result_count = " + std::to_string(result_count));
                // set result pks
                switch (pk_type) {
                    case milvus::DataType::INT64: {
                        search_result_data->mutable_ids()->mutable_int_id()->mutable_data()->Set(
                            loc, std::visit(Int64PKVisitor{}, search_result->primary_keys_[ki]));
                        break;
                    }
                    case milvus::DataType::VARCHAR: {
                        *search_result_data->mutable_ids()->mutable_str_id()->mutable_data()->Mutable(loc) =
                            std::visit(StrPKVisitor{}, search_result->primary_keys_[ki]);
                        break;
                    }
                    default: {
                        PanicInfo("unsupported primary key type");
                    }
                }

                // set result distances
                search_result_data->mutable_scores()->Set(loc, search_result->distances_[ki]);
                // set result offset to fill output fields data
                result_pairs[loc] = std::make_pair(search_result, ki);
            }
        }

        // update result topKs
        search_result_data->mutable_topks()->Set(qi - nq_begin, topk_count);
    }

    AssertInfo(search_result_data->scores_size() == result_count,
               "wrong scores size, size = " + std::to_string(search_result_data->scores_size()) +
                   ", expected size = " + std::to_string(result_count));

    // set output fields
    for (auto field_id : plan_->target_entries_) {
        auto& field_meta = plan_->schema_[field_id];
        auto field_data = milvus::segcore::MergeDataArray(result_pairs, field_meta);
        search_result_data->mutable_fields_data()->AddAllocated(field_data.release());
    }

    // SearchResultData to blob
    auto size = search_result_data->ByteSize();
    auto buffer = std::vector<char>(size);
    search_result_data->SerializePartialToArray(buffer.data(), size);

    return buffer;
}

}  // namespace milvus::segcore
