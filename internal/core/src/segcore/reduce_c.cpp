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
#include <utils/EasyAssert.h>
#include "segcore/reduce_c.h"

#include "segcore/Reduce.h"
#include "utils/Types.h"
#include "pb/service_msg.pb.h"

using SearchResult = milvus::engine::QueryResult;

int
MergeInto(int64_t num_queries, int64_t topk, float* distances, int64_t* uids, float* new_distances, int64_t* new_uids) {
    auto status = milvus::segcore::merge_into(num_queries, topk, distances, uids, new_distances, new_uids);
    return status.code();
}

struct MarshaledHitsPeerGroup {
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

    std::vector<MarshaledHitsPeerGroup> marshaled_hits_;
};

void
DeleteMarshaledHits(CMarshaledHits c_marshaled_hits) {
    auto hits = (MarshaledHits*)c_marshaled_hits;
    delete hits;
}

struct SearchResultPair {
    uint64_t id_;
    float distance_;
    int64_t segment_id_;

    SearchResultPair(uint64_t id, float distance, int64_t segment_id)
        : id_(id), distance_(distance), segment_id_(segment_id) {
    }

    bool
    operator<(const SearchResultPair& pair) const {
        return (distance_ < pair.distance_);
    }
};

void
GetResultData(std::vector<SearchResult*>& search_results,
              SearchResult& final_result,
              int64_t query_offset,
              int64_t topk) {
    auto num_segments = search_results.size();
    std::map<int, int> iter_loc_peer_result;
    std::vector<SearchResultPair> result_pairs;
    for (int j = 0; j < num_segments; ++j) {
        auto id = search_results[j]->result_ids_[query_offset];
        auto distance = search_results[j]->result_distances_[query_offset];
        result_pairs.push_back(SearchResultPair(id, distance, j));
        iter_loc_peer_result[j] = query_offset;
    }
    std::sort(result_pairs.begin(), result_pairs.end());
    final_result.result_ids_.push_back(result_pairs[0].id_);
    final_result.result_distances_.push_back(result_pairs[0].distance_);

    for (int i = 1; i < topk; ++i) {
        auto segment_id = result_pairs[0].segment_id_;
        auto query_offset = ++(iter_loc_peer_result[segment_id]);
        auto id = search_results[segment_id]->result_ids_[query_offset];
        auto distance = search_results[segment_id]->result_distances_[query_offset];
        result_pairs[0] = SearchResultPair(id, distance, segment_id);
        std::sort(result_pairs.begin(), result_pairs.end());
        final_result.result_ids_.push_back(result_pairs[0].id_);
        final_result.result_distances_.push_back(result_pairs[0].distance_);
    }
}

CQueryResult
ReduceQueryResults(CQueryResult* query_results, int64_t num_segments) {
    std::vector<SearchResult*> search_results;
    for (int i = 0; i < num_segments; ++i) {
        search_results.push_back((SearchResult*)query_results[i]);
    }
    auto topk = search_results[0]->topK_;
    auto num_queries = search_results[0]->num_queries_;
    auto final_result = std::make_unique<SearchResult>();

    int64_t query_offset = 0;
    for (int j = 0; j < num_queries; ++j) {
        GetResultData(search_results, *final_result, query_offset, topk);
        query_offset += topk;
    }

    return (CQueryResult)final_result.release();
}

CMarshaledHits
ReorganizeQueryResults(CQueryResult c_query_result,
                       CPlan c_plan,
                       CPlaceholderGroup* c_placeholder_groups,
                       int64_t num_groups) {
    auto marshaledHits = std::make_unique<MarshaledHits>(num_groups);
    auto search_result = (milvus::engine::QueryResult*)c_query_result;
    auto& result_ids = search_result->result_ids_;
    auto& result_distances = search_result->result_distances_;
    auto topk = GetTopK(c_plan);
    int64_t queries_offset = 0;
    for (int i = 0; i < num_groups; i++) {
        auto num_queries = GetNumOfQueries(c_placeholder_groups[i]);
        MarshaledHitsPeerGroup& hits_peer_group = (*marshaledHits).marshaled_hits_[i];
        for (int j = 0; j < num_queries; j++) {
            auto index = topk * queries_offset++;
            milvus::proto::service::Hits hits;
            for (int k = index; k < index + topk; k++) {
                hits.add_ids(result_ids[k]);
                hits.add_scores(result_distances[k]);
            }
            auto blob = hits.SerializeAsString();
            hits_peer_group.hits_.push_back(blob);
            hits_peer_group.blob_length_.push_back(blob.size());
        }
    }

    return (CMarshaledHits)marshaledHits.release();
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
GetNumQueriesPeerGroup(CMarshaledHits c_marshaled_hits, int64_t group_index) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& hits = marshaled_hits->marshaled_hits_[group_index].hits_;
    return hits.size();
}

void
GetHitSizePeerQueries(CMarshaledHits c_marshaled_hits, int64_t group_index, int64_t* hit_size_peer_query) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& blob_lens = marshaled_hits->marshaled_hits_[group_index].blob_length_;
    for (int i = 0; i < blob_lens.size(); i++) {
        hit_size_peer_query[i] = blob_lens[i];
    }
}
