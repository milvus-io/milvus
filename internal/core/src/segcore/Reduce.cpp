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

    unify_topK_ = search_results_[0]->unity_topK_;
    total_nq_ = search_results_[0]->total_nq_;
    num_segments_ = search_results_.size();
    num_slices_ = slice_nqs_.size();

    // prefix sum, get slices offsets
    AssertInfo(num_slices_ > 0, "empty slice_nqs is not allowed");
    auto slice_offsets_size = num_slices_ + 1;
    nq_slice_offsets_ = std::vector<int32_t>(slice_offsets_size);

    for (int i = 1; i < slice_offsets_size; i++) {
        nq_slice_offsets_[i] = nq_slice_offsets_[i - 1] + slice_nqs_[i - 1];
        for (auto j = nq_slice_offsets_[i - 1]; j < nq_slice_offsets_[i]; j++) {
        }
    }
    AssertInfo(nq_slice_offsets_[num_slices_] == total_nq_,
               "illegal req sizes"
               ", nq_slice_offsets[last] = " +
                   std::to_string(nq_slice_offsets_[num_slices_]) + ", total_nq = " + std::to_string(total_nq_));

    // init final_search_records and final_read_topKs
    final_search_records_ = std::vector<std::vector<int64_t>>(num_segments_);
    final_real_topKs_ = std::vector<std::vector<int64_t>>(num_segments_);
    for (auto& topKs : final_real_topKs_) {
        // `topKs` records real topK of each query
        topKs.resize(total_nq_);
    }
}

void
ReduceHelper::Reduce() {
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
    search_results_ = valid_search_results;
    num_segments_ = search_results_.size();
    if (valid_search_results.size() == 0) {
        // TODO: return empty search result?
        return;
    }

    for (int i = 0; i < num_slices_; i++) {
        // ReduceResultData for each slice
        ReduceResultData(i);
    }
    // after reduce, remove invalid primary_keys, distances and ids by `final_search_records`
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        if (search_result->result_offsets_.size() != 0) {
            std::vector<milvus::PkType> primary_keys;
            std::vector<float> distances;
            std::vector<int64_t> seg_offsets;
            for (int j = 0; j < final_search_records_[i].size(); j++) {
                auto& offset = final_search_records_[i][j];
                primary_keys.push_back(search_result->primary_keys_[offset]);
                distances.push_back(search_result->distances_[offset]);
                seg_offsets.push_back(search_result->seg_offsets_[offset]);
            }

            search_result->primary_keys_ = std::move(primary_keys);
            search_result->distances_ = std::move(distances);
            search_result->seg_offsets_ = std::move(seg_offsets);
        }
        search_result->topk_per_nq_prefix_sum_.resize(final_real_topKs_[i].size() + 1);
        std::partial_sum(final_real_topKs_[i].begin(), final_real_topKs_[i].end(),
                         search_result->topk_per_nq_prefix_sum_.begin() + 1);
    }

    // fill target entry
    for (auto& search_result : search_results_) {
        auto segment = static_cast<milvus::segcore::SegmentInterface*>(search_result->segment_);
        segment->FillTargetEntry(plan_, *search_result);
    }
}

void
ReduceHelper::Marshal() {
    // example:
    //  ----------------------------------
    //           nq0     nq1     nq2
    //   sr0    topk00  topk01  topk02
    //   sr1    topk10  topk11  topk12
    //  ----------------------------------
    // then:
    // result_slice_offsets[] = {
    //      0,
    //                                      == sr0->topk_per_nq_prefix_sum_[0] + sr1->topk_per_nq_prefix_sum_[0]
    //      ((topk00) + (topk10)),
    //                                      == sr0->topk_per_nq_prefix_sum_[1] + sr1->topk_per_nq_prefix_sum_[1]
    //      ((topk00 + topk01) + (topk10 + topk11)),
    //                                      == sr0->topk_per_nq_prefix_sum_[2] + sr1->topk_per_nq_prefix_sum_[2]
    //      ((topk00 + topk01 + topk02) + (topk10 + topk11 + topk12)),
    //                                      == sr0->topk_per_nq_prefix_sum_[3] + sr1->topk_per_nq_prefix_sum_[3]
    // }
    auto result_slice_offsets = std::vector<int64_t>(nq_slice_offsets_.size(), 0);
    for (auto search_result : search_results_) {
        AssertInfo(search_result->topk_per_nq_prefix_sum_.size() == search_result->total_nq_ + 1,
                   "incorrect topk_per_nq_prefix_sum_ size in search result");
        for (int i = 1; i < nq_slice_offsets_.size(); i++) {
            result_slice_offsets[i] += search_result->topk_per_nq_prefix_sum_[nq_slice_offsets_[i]];
        }
    }
    AssertInfo(result_slice_offsets[num_slices_] <= total_nq_ * unify_topK_,
               "illegal result_slice_offsets when Marshal, result_slice_offsets[last] = " +
                   std::to_string(result_slice_offsets[num_slices_]) + ", total_nq = " + std::to_string(total_nq_) +
                   ", unify_topK = " + std::to_string(unify_topK_));

    // get search result data blobs of slices
    search_result_data_blobs_ = std::make_unique<milvus::segcore::SearchResultDataBlobs>();
    search_result_data_blobs_->blobs.resize(num_slices_);
    //#pragma omp parallel for
    for (int i = 0; i < num_slices_; i++) {
        auto result_count = result_slice_offsets[i + 1] - result_slice_offsets[i];
        auto proto = GetSearchResultDataSlice(i, result_count);
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

    search_result->distances_ = std::move(distances);
    search_result->seg_offsets_ = std::move(seg_offsets);
    search_result->topk_per_nq_prefix_sum_.resize(nq + 1);
    std::partial_sum(real_topks.begin(), real_topks.end(), search_result->topk_per_nq_prefix_sum_.begin() + 1);
}

void
ReduceHelper::ReduceResultData(int slice_index) {
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        auto result_count = search_result->get_total_result_count();
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
        AssertInfo(search_result->primary_keys_.size() == result_count, "incorrect search result primary key size");
        AssertInfo(search_result->distances_.size() == result_count, "incorrect search result distance size");
    }

    auto nq_offset_begin = nq_slice_offsets_[slice_index];
    auto nq_offset_end = nq_slice_offsets_[slice_index + 1];
    AssertInfo(nq_offset_begin < nq_offset_end,
               "illegal nq offsets when ReduceResultData, nq_offset_begin = " + std::to_string(nq_offset_begin) +
                   ", nq_offset_end = " + std::to_string(nq_offset_end));

    // `search_records` records the search result offsets
    std::vector<std::vector<int64_t>> search_records(num_segments_);
    int64_t skip_dup_cnt = 0;

    // reduce search results
    int64_t result_offset = 0;
    for (int64_t qi = nq_offset_begin; qi < nq_offset_end; qi++) {
        std::vector<SearchResultPair> result_pairs;
        for (int i = 0; i < num_segments_; i++) {
            auto search_result = search_results_[i];
            if (search_result->topk_per_nq_prefix_sum_[qi + 1] - search_result->topk_per_nq_prefix_sum_[qi] == 0) {
                continue;
            }
            auto base_offset = search_result->topk_per_nq_prefix_sum_[qi];
            auto primary_key = search_result->primary_keys_[base_offset];
            auto distance = search_result->distances_[base_offset];
            result_pairs.emplace_back(primary_key, distance, search_result, i, base_offset,
                                      search_result->topk_per_nq_prefix_sum_[qi + 1]);
        }

        // nq has no results for all segments
        if (result_pairs.size() == 0) {
            continue;
        }
        std::unordered_set<milvus::PkType> pk_set;
        int64_t last_nq_result_offset = result_offset;
        while (result_offset - last_nq_result_offset < slice_topKs_[slice_index]) {
            std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
            auto& pilot = result_pairs[0];
            auto index = pilot.segment_index_;
            auto curr_pk = pilot.primary_key_;
            // no valid search result for this nq, break to next
            if (curr_pk == INVALID_PK) {
                break;
            }
            // remove duplicates
            if (pk_set.count(curr_pk) == 0) {
                pilot.search_result_->result_offsets_.push_back(result_offset++);
                search_records[index].push_back(pilot.offset_);
                pk_set.insert(curr_pk);
                final_real_topKs_[index][qi]++;
            } else {
                // skip entity with same primary key
                skip_dup_cnt++;
            }
            pilot.reset();
        }
    }

    if (skip_dup_cnt > 0) {
        LOG_SEGCORE_DEBUG_ << "skip duplicated search result, count = " << skip_dup_cnt;
    }

    // append search_records to final_search_records
    for (int i = 0; i < num_segments_; i++) {
        for (int j = 0; j < search_records[i].size(); j++) {
            final_search_records_[i].emplace_back(search_records[i][j]);
        }
    }
}

std::vector<char>
ReduceHelper::GetSearchResultDataSlice(int slice_index_, int64_t result_count) {
    auto nq_offset_begin = nq_slice_offsets_[slice_index_];
    auto nq_offset_end = nq_slice_offsets_[slice_index_ + 1];
    AssertInfo(nq_offset_begin <= nq_offset_end,
               "illegal offsets when GetSearchResultDataSlice, nq_offset_begin = " + std::to_string(nq_offset_begin) +
                   ", nq_offset_end = " + std::to_string(nq_offset_end));

    auto search_result_data = std::make_unique<milvus::proto::schema::SearchResultData>();
    // set unify_topK and total_nq
    search_result_data->set_top_k(slice_topKs_[slice_index_]);
    search_result_data->set_num_queries(nq_offset_end - nq_offset_begin);
    search_result_data->mutable_topks()->Resize(nq_offset_end - nq_offset_begin, 0);

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
    for (auto nq_offset = nq_offset_begin; nq_offset < nq_offset_end; nq_offset++) {
        int64_t topK_count = 0;
        for (int i = 0; i < search_results_.size(); i++) {
            auto search_result = search_results_[i];
            AssertInfo(search_result != nullptr, "null search result when reorganize");
            if (search_result->result_offsets_.size() == 0) {
                continue;
            }

            auto result_start = search_result->topk_per_nq_prefix_sum_[nq_offset];
            auto result_end = search_result->topk_per_nq_prefix_sum_[nq_offset + 1];
            for (auto offset = result_start; offset < result_end; offset++) {
                auto loc = search_result->result_offsets_[offset];
                AssertInfo(loc < result_count && loc >= 0,
                           "invalid loc when GetSearchResultDataSlice, loc = " + std::to_string(loc) +
                               ", result_count = " + std::to_string(result_count));
                // set result pks
                switch (pk_type) {
                    case milvus::DataType::INT64: {
                        search_result_data->mutable_ids()->mutable_int_id()->mutable_data()->Set(
                            loc, std::visit(Int64PKVisitor{}, search_result->primary_keys_[offset]));
                        break;
                    }
                    case milvus::DataType::VARCHAR: {
                        *search_result_data->mutable_ids()->mutable_str_id()->mutable_data()->Mutable(loc) =
                            std::visit(StrPKVisitor{}, search_result->primary_keys_[offset]);
                        break;
                    }
                    default: {
                        PanicInfo("unsupported primary key type");
                    }
                }

                // set result distances
                search_result_data->mutable_scores()->Set(loc, search_result->distances_[offset]);
                // set result offset to fill output fields data
                result_pairs[loc] = std::make_pair(search_result, offset);
            }

            topK_count += search_result->topk_per_nq_prefix_sum_[nq_offset + 1] -
                          search_result->topk_per_nq_prefix_sum_[nq_offset];
        }

        // update result topKs
        search_result_data->mutable_topks()->Set(nq_offset - nq_offset_begin, topK_count);
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
