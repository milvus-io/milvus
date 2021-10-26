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

#include <vector>
#include <unordered_set>

#include "common/Consts.h"
#include "common/Types.h"
#include "exceptions/EasyAssert.h"
#include "log/Log.h"
#include "query/Plan.h"
#include "segcore/reduce_c.h"
#include "segcore/Reduce.h"
#include "segcore/ReduceStructure.h"
#include "segcore/SegmentInterface.h"
#include "pb/milvus.pb.h"

using SearchResult = milvus::SearchResult;

int
MergeInto(int64_t num_queries, int64_t topk, float* distances, int64_t* uids, float* new_distances, int64_t* new_uids) {
    auto status = milvus::segcore::merge_into(num_queries, topk, distances, uids, new_distances, new_uids);
    return status.code();
}

struct MarshaledHitsPerGroup {
    std::vector<std::string> hits_;
    std::vector<int64_t> blob_length_;
};

struct MarshaledHits {
    explicit MarshaledHits(int64_t num_group) {
        marshaled_hits_.resize(num_group);
    }

    int
    get_num_group() {
        return marshaled_hits_.size();
    }

    std::vector<MarshaledHitsPerGroup> marshaled_hits_;
};

void
DeleteMarshaledHits(CMarshaledHits c_marshaled_hits) {
    auto hits = (MarshaledHits*)c_marshaled_hits;
    delete hits;
}

void
GetResultData(std::vector<std::vector<int64_t>>& search_records,
              std::vector<SearchResult*>& search_results,
              int64_t query_idx,
              int64_t topk) {
    auto num_segments = search_results.size();
    AssertInfo(num_segments > 0, "num segment must greater than 0");
    std::vector<SearchResultPair> result_pairs;
    int64_t query_offset = query_idx * topk;
    for (int j = 0; j < num_segments; ++j) {
        auto search_result = search_results[j];
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
        auto distance = search_result->result_distances_[query_offset];
        result_pairs.push_back(SearchResultPair(distance, search_result, query_offset, j));
    }
    int64_t loc_offset = query_offset;
    AssertInfo(topk > 0, "topk must greater than 0");

#if 0
    for (int i = 0; i < topk; ++i) {
        result_pairs[0].reset_distance();
        std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
        auto& result_pair = result_pairs[0];
        auto index = result_pair.index_;
        result_pair.search_result_->result_offsets_.push_back(loc_offset++);
        search_records[index].push_back(result_pair.offset_++);
    }
#else
    float prev_dis = MAXFLOAT;
    std::unordered_set<int64_t> prev_pk_set;
    while (loc_offset - query_offset < topk) {
        result_pairs[0].reset_distance();
        std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
        auto& result_pair = result_pairs[0];
        auto index = result_pair.index_;
        int64_t curr_pk = result_pair.search_result_->primary_keys_[result_pair.offset_];
        float curr_dis = result_pair.search_result_->result_distances_[result_pair.offset_];
        // remove duplicates
        if (curr_pk == INVALID_ID || std::abs(curr_dis - prev_dis) > 0.00001) {
            result_pair.search_result_->result_offsets_.push_back(loc_offset++);
            search_records[index].push_back(result_pair.offset_);
            prev_dis = curr_dis;
            prev_pk_set.clear();
            prev_pk_set.insert(curr_pk);
        } else {
            // To handle this case:
            //    e1: [100, 0.99]
            //    e2: [101, 0.99]   ==> not duplicated, should keep
            //    e3: [100, 0.99]   ==> duplicated, should remove
            if (prev_pk_set.count(curr_pk) == 0) {
                result_pair.search_result_->result_offsets_.push_back(loc_offset++);
                search_records[index].push_back(result_pair.offset_);
                // prev_pk_set keeps all primary keys with same distance
                prev_pk_set.insert(curr_pk);
            } else {
                // the entity with same distance and same primary key must be duplicated
                LOG_SEGCORE_DEBUG_ << "skip duplicated search result, primary key " << curr_pk;
            }
        }
        result_pair.offset_++;
    }
#endif
}

void
ResetSearchResult(std::vector<std::vector<int64_t>>& search_records, std::vector<SearchResult*>& search_results) {
    auto num_segments = search_results.size();
    AssertInfo(num_segments > 0, "num segment must greater than 0");
    for (int i = 0; i < num_segments; i++) {
        auto search_result = search_results[i];
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
        if (search_result->result_offsets_.size() == 0) {
            continue;
        }

        std::vector<int64_t> primary_keys;
        std::vector<float> result_distances;
        std::vector<int64_t> internal_seg_offsets;

        for (int j = 0; j < search_records[i].size(); j++) {
            auto& offset = search_records[i][j];
            auto primary_key = search_result->primary_keys_[offset];
            auto distance = search_result->result_distances_[offset];
            auto internal_seg_offset = search_result->internal_seg_offsets_[offset];
            primary_keys.push_back(primary_key);
            result_distances.push_back(distance);
            internal_seg_offsets.push_back(internal_seg_offset);
        }

        search_result->primary_keys_ = primary_keys;
        search_result->result_distances_ = result_distances;
        search_result->internal_seg_offsets_ = internal_seg_offsets;
    }
}

CStatus
ReduceSearchResultsAndFillData(CSearchPlan c_plan, CSearchResult* c_search_results, int64_t num_segments) {
    try {
        auto plan = (milvus::query::Plan*)c_plan;
        std::vector<SearchResult*> search_results;
        for (int i = 0; i < num_segments; ++i) {
            search_results.push_back((SearchResult*)c_search_results[i]);
        }
        auto topk = search_results[0]->topk_;
        auto num_queries = search_results[0]->num_queries_;
        std::vector<std::vector<int64_t>> search_records(num_segments);

        // get primary keys for duplicates removal
        for (auto& search_result : search_results) {
            auto segment = (milvus::segcore::SegmentInterface*)(search_result->segment_);
            segment->FillPrimaryKeys(plan, *search_result);
        }

        for (int i = 0; i < num_queries; ++i) {
            GetResultData(search_records, search_results, i, topk);
        }
        ResetSearchResult(search_records, search_results);

        // fill in other entities
        for (auto& search_result : search_results) {
            auto segment = (milvus::segcore::SegmentInterface*)(search_result->segment_);
            segment->FillTargetEntry(plan, *search_result);
        }

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
ReorganizeSearchResults(CMarshaledHits* c_marshaled_hits, CSearchResult* c_search_results, int64_t num_segments) {
    try {
        auto marshaledHits = std::make_unique<MarshaledHits>(1);
        auto sr = (SearchResult*)c_search_results[0];
        auto topk = sr->topk_;
        auto num_queries = sr->num_queries_;

        std::vector<float> result_distances(num_queries * topk);
        std::vector<std::vector<char>> row_datas(num_queries * topk);

        std::vector<int64_t> counts(num_segments);
        for (int i = 0; i < num_segments; i++) {
            auto search_result = (SearchResult*)c_search_results[i];
            AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
            auto size = search_result->result_offsets_.size();
            if (size == 0) {
                continue;
            }
#pragma omp parallel for
            for (int j = 0; j < size; j++) {
                auto loc = search_result->result_offsets_[j];
                result_distances[loc] = search_result->result_distances_[j];
                row_datas[loc] = search_result->row_data_[j];
            }
            counts[i] = size;
        }

        int64_t total_count = 0;
        for (int i = 0; i < num_segments; i++) {
            total_count += counts[i];
        }
        AssertInfo(total_count == num_queries * topk, "the reduces result's size less than total_num_queries*topk");

        MarshaledHitsPerGroup& hits_per_group = (*marshaledHits).marshaled_hits_[0];
        hits_per_group.hits_.resize(num_queries);
        hits_per_group.blob_length_.resize(num_queries);
        std::vector<milvus::proto::milvus::Hits> hits(num_queries);
#pragma omp parallel for
        for (int m = 0; m < num_queries; m++) {
            for (int n = 0; n < topk; n++) {
                int64_t result_offset = m * topk + n;
                hits[m].add_scores(result_distances[result_offset]);
                auto& row_data = row_datas[result_offset];
                hits[m].add_row_data(row_data.data(), row_data.size());
                hits[m].add_ids(*(int64_t*)row_data.data());
            }
        }

#pragma omp parallel for
        for (int j = 0; j < num_queries; j++) {
            auto blob = hits[j].SerializeAsString();
            hits_per_group.hits_[j] = blob;
            hits_per_group.blob_length_[j] = blob.size();
        }

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto marshaled_res = (CMarshaledHits)marshaledHits.release();
        *c_marshaled_hits = marshaled_res;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *c_marshaled_hits = nullptr;
        return status;
    }
}

int64_t
GetHitsBlobSize(CMarshaledHits c_marshaled_hits) {
    int64_t total_size = 0;
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto num_group = marshaled_hits->get_num_group();
    for (int i = 0; i < num_group; i++) {
        auto& length_vector = marshaled_hits->marshaled_hits_[i].blob_length_;
        for (int j = 0; j < length_vector.size(); j++) {
            total_size += length_vector[j];
        }
    }
    return total_size;
}

void
GetHitsBlob(CMarshaledHits c_marshaled_hits, const void* hits) {
    auto byte_hits = (char*)hits;
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto num_group = marshaled_hits->get_num_group();
    int offset = 0;
    for (int i = 0; i < num_group; i++) {
        auto& hits = marshaled_hits->marshaled_hits_[i];
        auto num_queries = hits.hits_.size();
        for (int j = 0; j < num_queries; j++) {
            auto blob_size = hits.blob_length_[j];
            memcpy(byte_hits + offset, hits.hits_[j].data(), blob_size);
            offset += blob_size;
        }
    }
}

int64_t
GetNumQueriesPerGroup(CMarshaledHits c_marshaled_hits, int64_t group_index) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& hits = marshaled_hits->marshaled_hits_[group_index].hits_;
    return hits.size();
}

void
GetHitSizePerQueries(CMarshaledHits c_marshaled_hits, int64_t group_index, int64_t* hit_size_peer_query) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& blob_lens = marshaled_hits->marshaled_hits_[group_index].blob_length_;
    for (int i = 0; i < blob_lens.size(); i++) {
        hit_size_peer_query[i] = blob_lens[i];
    }
}
