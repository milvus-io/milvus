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

#include <limits>
#include <unordered_set>
#include <vector>

#include "Reduce.h"
#include "common/CGoHelper.h"
#include "common/Consts.h"
#include "common/Types.h"
#include "common/QueryResult.h"
#include "exceptions/EasyAssert.h"
#include "log/Log.h"
#include "pb/milvus.pb.h"
#include "query/Plan.h"
#include "segcore/ReduceStructure.h"
#include "segcore/SegmentInterface.h"
#include "segcore/reduce_c.h"
#include "segcore/Utils.h"

using SearchResult = milvus::SearchResult;

// void
// PrintSearchResult(char* buf, const milvus::SearchResult* result, int64_t seg_idx, int64_t from, int64_t to) {
//    const int64_t MAXLEN = 32;
//    snprintf(buf + strlen(buf), MAXLEN, "{ seg No.%ld ", seg_idx);
//    for (int64_t i = from; i < to; i++) {
//        snprintf(buf + strlen(buf), MAXLEN, "(%ld, %ld, %f), ", i, result->primary_keys_[i], result->distances_[i]);
//    }
//    snprintf(buf + strlen(buf), MAXLEN, "} ");
//}

void
ReduceResultData(std::vector<SearchResult*>& search_results, int64_t nq, int64_t topk) {
    auto num_segments = search_results.size();
    AssertInfo(num_segments > 0, "num segment must greater than 0");
    for (int i = 0; i < num_segments; i++) {
        auto search_result = search_results[i];
        auto result_count = search_result->get_total_result_count();
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
        AssertInfo(search_result->primary_keys_.size() == result_count, "incorrect search result primary key size");
        AssertInfo(search_result->distances_.size() == result_count, "incorrect search result distance size");
    }

    std::vector<std::vector<int64_t>> final_real_topks(num_segments);
    for (auto& topks : final_real_topks) {
        topks.resize(nq);
    }
    std::vector<std::vector<int64_t>> search_records(num_segments);
    std::unordered_set<milvus::PkType> pk_set;
    int64_t skip_dup_cnt = 0;

    // reduce search results
    int64_t result_offset = 0;
    for (int64_t qi = 0; qi < nq; qi++) {
        std::vector<SearchResultPair> result_pairs;
        for (int i = 0; i < num_segments; i++) {
            auto search_result = search_results[i];
            auto base_offset = search_result->get_result_count(qi);
            auto primary_key = search_result->primary_keys_[base_offset];
            auto distance = search_result->distances_[base_offset];
            result_pairs.push_back(SearchResultPair(primary_key, distance, search_result, i, base_offset,
                                                    base_offset + search_result->real_topK_per_nq_[qi]));
        }

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
        pk_set.clear();
        int64_t last_nq_result_offset = result_offset;
        while (result_offset - last_nq_result_offset < topk) {
            std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
            auto& pilot = result_pairs[0];
            auto index = pilot.index_;
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
                final_real_topks[index][qi]++;
            } else {
                // skip entity with same primary key
                skip_dup_cnt++;
            }
            pilot.reset();
        }
#endif
    }
    LOG_SEGCORE_DEBUG_ << "skip duplicated search result, count = " << skip_dup_cnt;

    // after reduce, remove redundant values in primary_keys, distances and ids
    for (int i = 0; i < num_segments; i++) {
        auto search_result = search_results[i];
        if (search_result->result_offsets_.size() != 0) {
            std::vector<milvus::PkType> primary_keys;
            std::vector<float> distances;
            std::vector<int64_t> ids;
            for (int j = 0; j < search_records[i].size(); j++) {
                auto& offset = search_records[i][j];
                primary_keys.push_back(search_result->primary_keys_[offset]);
                distances.push_back(search_result->distances_[offset]);
                ids.push_back(search_result->seg_offsets_[offset]);
            }

            search_result->primary_keys_ = std::move(primary_keys);
            search_result->distances_ = std::move(distances);
            search_result->seg_offsets_ = std::move(ids);
        }
        search_result->real_topK_per_nq_ = std::move(final_real_topks[i]);
    }
}

struct Int64PKVisitor {
    template <typename T>
    int64_t
    operator()(T t) const {
        PanicInfo("invalid int64 pk value");
    }
};

template <>
int64_t
Int64PKVisitor::operator()<int64_t>(int64_t t) const {
    return t;
}

struct StrPKVisitor {
    template <typename T>
    std::string
    operator()(T t) const {
        PanicInfo("invalid string pk value");
    }
};

template <>
std::string
StrPKVisitor::operator()<std::string>(std::string t) const {
    return t;
}

std::vector<char>
GetSearchResultDataSlice(std::vector<SearchResult*>& search_results,
                         milvus::query::Plan* plan,
                         int64_t nq_offset_begin,
                         int64_t nq_offset_end,
                         int64_t result_offset_begin,
                         int64_t result_offset_end,
                         int64_t nq,
                         int64_t topK) {
    AssertInfo(nq_offset_begin <= nq_offset_end,
               "illegal offsets when GetSearchResultDataSlice, nq_offset_begin = " + std::to_string(nq_offset_begin) +
                   ", nq_offset_end = " + std::to_string(nq_offset_end));
    AssertInfo(nq_offset_end <= nq, "illegal nq_offset_end when GetSearchResultDataSlice, nq_offset_end = " +
                                        std::to_string(nq_offset_end) + ", nq = " + std::to_string(nq));

    AssertInfo(result_offset_begin <= result_offset_end,
               "illegal result offsets when GetSearchResultDataSlice, result_offset_begin = " +
                   std::to_string(result_offset_begin) + ", result_offset_end = " + std::to_string(result_offset_end));
    AssertInfo(result_offset_end <= nq * topK,
               "illegal result_offset_end when GetSearchResultDataSlice, result_offset_end = " +
                   std::to_string(result_offset_end) + ", nq = " + std::to_string(nq) +
                   ", topk = " + std::to_string(topK));

    auto search_result_data = std::make_unique<milvus::proto::schema::SearchResultData>();
    // set topK and nq
    search_result_data->set_top_k(topK);
    search_result_data->set_num_queries(nq_offset_end - nq_offset_begin);
    search_result_data->mutable_topks()->Resize(nq_offset_end - nq_offset_begin, 0);

    auto num_segments = search_results.size();
    auto total_result_count = result_offset_end - result_offset_begin;

    // use for fill field data
    std::vector<std::pair<SearchResult*, int64_t>> result_offsets(total_result_count);

    // reverse space for pks
    auto primary_field_id = plan->schema_.get_primary_field_id().value_or(milvus::FieldId(-1));
    AssertInfo(primary_field_id.get() != INVALID_FIELD_ID, "Primary key is -1");
    auto pk_type = plan->schema_[primary_field_id].get_data_type();
    switch (pk_type) {
        case milvus::DataType::INT64: {
            auto ids = std::make_unique<milvus::proto::schema::LongArray>();
            ids->mutable_data()->Resize(total_result_count, 0);
            search_result_data->mutable_ids()->set_allocated_int_id(ids.release());
            break;
        }
        case milvus::DataType::VARCHAR: {
            auto ids = std::make_unique<milvus::proto::schema::StringArray>();
            std::vector<std::string> string_pks(total_result_count);
            *ids->mutable_data() = {string_pks.begin(), string_pks.end()};
            search_result_data->mutable_ids()->set_allocated_str_id(ids.release());
            break;
        }
        default: {
            PanicInfo("unsupported primary key type");
        }
    }

    // reverse space for distances
    search_result_data->mutable_scores()->Resize(total_result_count, 0);

    // fill pks and distances
    for (auto nq_offset = nq_offset_begin; nq_offset < nq_offset_end; nq_offset++) {
        int64_t result_count = 0;
        for (int i = 0; i < num_segments; i++) {
            auto search_result = search_results[i];
            AssertInfo(search_result != nullptr, "null search result when reorganize");
            if (search_result->result_offsets_.size() == 0) {
                continue;
            }

            auto seg_result_offset_start = search_result->get_result_count(nq_offset);
            auto seg_result_offset_end = seg_result_offset_start + search_result->real_topK_per_nq_[nq_offset];
            for (auto j = seg_result_offset_start; j < seg_result_offset_end; j++) {
                auto loc = search_result->result_offsets_[j] - result_offset_begin;
                // set result pks
                switch (pk_type) {
                    case milvus::DataType::INT64: {
                        search_result_data->mutable_ids()->mutable_int_id()->mutable_data()->Set(
                            loc, std::visit(Int64PKVisitor{}, search_result->primary_keys_[j]));
                        break;
                    }
                    case milvus::DataType::VARCHAR: {
                        *search_result_data->mutable_ids()->mutable_str_id()->mutable_data()->Mutable(loc) =
                            std::visit(StrPKVisitor{}, search_result->primary_keys_[j]);
                        break;
                    }
                    default: {
                        PanicInfo("unsupported primary key type");
                    }
                }

                // set result distances
                search_result_data->mutable_scores()->Set(loc, search_result->distances_[j]);
                // set result offset to fill output fields data
                result_offsets[loc] = std::make_pair(search_result, j);
            }

            result_count += search_result->real_topK_per_nq_[nq_offset];
        }

        // update result topks
        search_result_data->mutable_topks()->Set(nq_offset - nq_offset_begin, result_count);
    }

    AssertInfo(search_result_data->scores_size() == total_result_count,
               "wrong scores size"
               ", size = " +
                   std::to_string(search_result_data->scores_size()) +
                   ", expected size = " + std::to_string(total_result_count));

    // set output fields
    for (auto field_id : plan->target_entries_) {
        auto& field_meta = plan->schema_[field_id];
        auto field_data = milvus::segcore::MergeDataArray(result_offsets, field_meta);
        search_result_data->mutable_fields_data()->AddAllocated(field_data.release());
    }

    // SearchResultData to blob
    auto size = search_result_data->ByteSize();
    auto buffer = std::vector<char>(size);
    search_result_data->SerializePartialToArray(buffer.data(), size);

    return buffer;
}

CStatus
Marshal(CSearchResultDataBlobs* cSearchResultDataBlobs,
        CSearchResult* c_search_results,
        CSearchPlan c_plan,
        int32_t num_segments,
        int32_t* nq_slice_sizes,
        int32_t num_slices) {
    try {
        // parse search results and get topK, nq
        std::vector<SearchResult*> search_results(num_segments);
        for (int i = 0; i < num_segments; ++i) {
            search_results[i] = static_cast<SearchResult*>(c_search_results[i]);
        }
        AssertInfo(search_results.size() > 0, "empty search result when Marshal");
        auto plan = (milvus::query::Plan*)c_plan;
        auto topK = search_results[0]->topk_;
        auto nq = search_results[0]->num_queries_;

        std::vector<int64_t> result_count_per_nq(nq);
        for (auto search_result : search_results) {
            AssertInfo(search_result->real_topK_per_nq_.size() == nq,
                       "incorrect real_topK_per_nq_ size in search result");
            for (int j = 0; j < nq; j++) {
                result_count_per_nq[j] += search_result->real_topK_per_nq_[j];
            }
        }

        // prefix sum, get slices offsets
        AssertInfo(num_slices > 0, "empty nq_slice_sizes is not allowed");
        auto slice_offsets_size = num_slices + 1;
        auto nq_slice_offsets = std::vector<int32_t>(slice_offsets_size);
        auto result_slice_offset = std::vector<int64_t>(slice_offsets_size);

        for (int i = 1; i < slice_offsets_size; i++) {
            nq_slice_offsets[i] = nq_slice_offsets[i - 1] + nq_slice_sizes[i - 1];
            result_slice_offset[i] = result_slice_offset[i - 1];
            for (auto j = nq_slice_offsets[i - 1]; j < nq_slice_offsets[i]; j++) {
                result_slice_offset[i] += result_count_per_nq[j];
            }
        }
        AssertInfo(nq_slice_offsets[num_slices] == nq,
                   "illegal req sizes"
                   ", nq_slice_offsets[last] = " +
                       std::to_string(nq_slice_offsets[num_slices]) + ", nq = " + std::to_string(nq));

        // get search result data blobs by slices
        auto search_result_data_blobs = std::make_unique<milvus::segcore::SearchResultDataBlobs>();
        search_result_data_blobs->blobs.resize(num_slices);
        //#pragma omp parallel for
        for (int i = 0; i < num_slices; i++) {
            auto proto = GetSearchResultDataSlice(search_results, plan, nq_slice_offsets[i], nq_slice_offsets[i + 1],
                                                  result_slice_offset[i], result_slice_offset[i + 1], nq, topK);
            search_result_data_blobs->blobs[i] = proto;
        }

        // set final result ptr
        *cSearchResultDataBlobs = search_result_data_blobs.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        DeleteSearchResultDataBlobs(cSearchResultDataBlobs);
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetSearchResultDataBlob(CProto* searchResultDataBlob,
                        CSearchResultDataBlobs cSearchResultDataBlobs,
                        int32_t blob_index) {
    try {
        auto search_result_data_blobs =
            reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(cSearchResultDataBlobs);
        AssertInfo(blob_index < search_result_data_blobs->blobs.size(), "blob_index out of range");
        searchResultDataBlob->proto_blob = search_result_data_blobs->blobs[blob_index].data();
        searchResultDataBlob->proto_size = search_result_data_blobs->blobs[blob_index].size();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        searchResultDataBlob->proto_blob = nullptr;
        searchResultDataBlob->proto_size = 0;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

void
DeleteSearchResultDataBlobs(CSearchResultDataBlobs cSearchResultDataBlobs) {
    if (cSearchResultDataBlobs == nullptr) {
        return;
    }
    auto search_result_data_blobs = reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(cSearchResultDataBlobs);
    delete search_result_data_blobs;
}

void
FilterInvalidSearchResult(SearchResult* search_result) {
    auto nq = search_result->num_queries_;
    auto topk = search_result->topk_;
    AssertInfo(search_result->seg_offsets_.size() == nq * topk,
               "wrong seg offsets size, size = " + std::to_string(search_result->seg_offsets_.size()) +
                   ", expected size = " + std::to_string(nq * topk));
    AssertInfo(search_result->distances_.size() == nq * topk,
               "wrong distances size, size = " + std::to_string(search_result->distances_.size()) +
                   ", expected size = " + std::to_string(nq * topk));
    std::vector<int64_t> real_topks(nq);
    std::vector<float> distances;
    std::vector<int64_t> seg_offsets;
    for (auto i = 0; i < nq; i++) {
        real_topks[i] = 0;
        for (auto j = 0; j < topk; j++) {
            auto offset = i * topk + j;
            if (search_result->seg_offsets_[offset] != INVALID_SEG_OFFSET) {
                real_topks[i]++;
                seg_offsets.push_back(search_result->seg_offsets_[offset]);
                distances.push_back(search_result->distances_[offset]);
            }
        }
    }

    search_result->distances_ = std::move(distances);
    search_result->seg_offsets_ = std::move(seg_offsets);
    search_result->real_topK_per_nq_ = std::move(real_topks);
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

        std::vector<SearchResult*> valid_search_results;
        // get primary keys for duplicates removal
        for (auto search_result : search_results) {
            auto segment = (milvus::segcore::SegmentInterface*)(search_result->segment_);
            FilterInvalidSearchResult(search_result);
            segment->FillPrimaryKeys(plan, *search_result);
            if (search_result->get_total_result_count() > 0) {
                valid_search_results.push_back(search_result);
            }
        }

        if (valid_search_results.size() > 0) {
            ReduceResultData(valid_search_results, num_queries, topk);
        }

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
