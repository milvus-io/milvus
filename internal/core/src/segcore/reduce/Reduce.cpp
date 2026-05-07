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

#include <math.h>
#include <algorithm>
#include <cstdint>
#include <future>
#include <numeric>
#include <optional>
#include <queue>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Utils.h"
#include "fmt/core.h"
#include "folly/ScopeGuard.h"
#include "glog/logging.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "log/Log.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentInterface.h"
#include "storage/ThreadPools.h"

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
    AssertInfo(total_nq_ > 0, "empty nq is not allowed");
    slice_nqs_prefix_sum_.resize(num_slices_ + 1);
    std::partial_sum(slice_nqs_.begin(),
                     slice_nqs_.end(),
                     slice_nqs_prefix_sum_.begin() + 1);
    AssertInfo(
        slice_nqs_prefix_sum_[num_slices_] == total_nq_,
        "illegal req sizes, slice_nqs_prefix_sum_[last] = {}, total_nq = {}",
        slice_nqs_prefix_sum_[num_slices_],
        total_nq_);

    // init final_search_records and final_read_topKs
    final_search_records_.resize(num_segments_);
    for (auto& search_record : final_search_records_) {
        search_record.resize(total_nq_);
    }
}

void
ReduceHelper::PreReduce() {
    tracer::AutoSpan span =
        trace_ctx_ != nullptr
            ? tracer::AutoSpan("ReduceHelper::PreReduce", trace_ctx_, true)
            : tracer::AutoSpan(
                  "ReduceHelper::PreReduce", tracer::GetRootSpan(), true);

    auto global_refine_enable =
        plan_->plan_node_->search_info_.global_refine_enable_;
    AssertInfo(!(global_refine_enable &&
                 plan_->plan_node_->search_info_.has_group_by()),
               "global refine is not enabled for group_by");

    auto use_global_refine = global_refine_enable && CanUseGlobalRefine();

    FilterInvalidSearchResults();
    if (use_global_refine) {
        TruncateToRefineTopk();
        RefineDistances();
    }
    FillPrimaryKey();
}

bool
ReduceHelper::CanUseGlobalRefine() const {
    if (placeholder_group_ == nullptr || placeholder_group_->empty()) {
        return false;
    }
    // there are chances that indexes are mixed, some are refine enabled, some are not
    // and we will skip refine search for those refine-enabled segments and return coarse distances
    // so we have to calc refine distances for them
    return std::any_of(search_results_.begin(),
                       search_results_.end(),
                       [this](SearchResult* search_result) {
                           return IsSearchResultRefineEnabled(search_result);
                       });
}

bool
ReduceHelper::IsSearchResultRefineEnabled(SearchResult* search_result) const {
    if (search_result == nullptr || search_result->segment_ == nullptr) {
        return false;
    }
    auto segment = static_cast<SegmentInterface*>(search_result->segment_);
    return segment->IsIndexRefineEnabled(
        plan_->plan_node_->search_info_.field_id_);
}

void
ReduceHelper::FilterInvalidSearchResult(SearchResult* search_result) {
    auto nq = search_result->total_nq_;
    auto topK = search_result->unity_topK_;
    AssertInfo(search_result->seg_offsets_.size() == nq * topK,
               "wrong seg offsets size, size = {}, expected size = {}",
               search_result->seg_offsets_.size(),
               nq * topK);
    AssertInfo(search_result->distances_.size() == nq * topK,
               "wrong distances size, size = {}, expected size = {}",
               search_result->distances_.size(),
               nq * topK);
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
                if (search_result->element_level_)
                    search_result->element_indices_[valid_index] =
                        search_result->element_indices_[index];
                valid_index++;
            }
        }
    }
    offsets.resize(valid_index);
    distances.resize(valid_index);
    if (search_result->element_level_)
        search_result->element_indices_.resize(valid_index);
    search_result->topk_per_nq_prefix_sum_.resize(nq + 1);
    std::partial_sum(real_topks.begin(),
                     real_topks.end(),
                     search_result->topk_per_nq_prefix_sum_.begin() + 1);
}

void
ReduceHelper::FilterInvalidSearchResults() {
    tracer::AutoSpan span("ReduceHelper::FilterInvalidSearchResults",
                          tracer::GetRootSpan());
    // First pass: filter invalid results and compact the array sequentially
    uint32_t valid_index = 0;
    for (auto& search_result : search_results_) {
        if (search_result->unity_topK_ == 0) {
            continue;
        }
        // Group-by results are already compact: SearchGroupByOperator does
        // not pad with INVALID_SEG_OFFSET and topk_per_nq_prefix_sum_ is
        // populated by the operator. Skip FilterInvalidSearchResult, whose
        // assertion (size == nq * topK) does not hold for group-by layouts.
        if (!search_result->composite_group_by_values_.has_value()) {
            FilterInvalidSearchResult(search_result);
        }
        LOG_DEBUG("the size of search result: {}",
                  search_result->seg_offsets_.size());
        if (search_result->get_total_result_count() > 0) {
            search_results_[valid_index++] = search_result;
        }
    }
    search_results_.resize(valid_index);
    num_segments_ = search_results_.size();
}

void
ReduceHelper::FillPrimaryKey() {
    tracer::AutoSpan span("ReduceHelper::FillPrimaryKey",
                          tracer::GetRootSpan());
    // Second pass: fill primary keys
    if (num_segments_ > 1) {
        // Parallel execution using MIDDLE thread pool for multiple segments
        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
        std::vector<std::future<void>> futures;
        futures.reserve(num_segments_);
        for (auto& search_result : search_results_) {
            auto future = pool.Submit([this, search_result] {
                auto segment =
                    static_cast<SegmentInterface*>(search_result->segment_);
                segment->FillPrimaryKeys(plan_, *search_result, op_ctx_);
            });
            futures.emplace_back(std::move(future));
        }
        auto futures_guard = folly::makeGuard([&futures]() {
            for (auto& f : futures) {
                if (f.valid()) {
                    try {
                        f.get();
                    } catch (...) {
                    }
                }
            }
        });
        for (auto& future : futures) {
            future.get();
        }
    } else if (num_segments_ == 1) {
        auto segment =
            static_cast<SegmentInterface*>(search_results_[0]->segment_);
        segment->FillPrimaryKeys(plan_, *search_results_[0], op_ctx_);
    }
}

void
ReduceHelper::TruncateToRefineTopk() {
    auto refine_topk_ratio = plan_->plan_node_->search_info_.refine_topk_ratio_;
    if (search_results_.empty()) {
        return;
    }

    // get search range from search params for refinement
    int64_t search_range = index::GetValueFromConfig<int64_t>(
                               plan_->plan_node_->search_info_.search_params_,
                               index::HNSW_QUERY_EF)
                               .value_or(0);

    // Iterate per slice/per NQ with a distance-only merge (no PKs available
    // at this stage).
    // Record selected offsets in final_search_records_, then compact.
    for (int64_t slice_index = 0; slice_index < num_slices_; ++slice_index) {
        auto refine_topk = static_cast<int64_t>(
            std::ceil(refine_topk_ratio *
                      std::max(slice_topKs_[slice_index], search_range)));
        auto nq_begin = slice_nqs_prefix_sum_[slice_index];
        auto nq_end = slice_nqs_prefix_sum_[slice_index + 1];

        for (int64_t qi = nq_begin; qi < nq_end; ++qi) {
            TruncateSearchResultForOneNQ(qi, refine_topk);
        }
    }

    // Compact search results using final_search_records_
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        std::vector<int64_t> real_topks(total_nq_, 0);
        uint32_t index = 0;
        for (int j = 0; j < total_nq_; j++) {
            for (auto offset : final_search_records_[i][j]) {
                search_result->distances_[index] =
                    search_result->distances_[offset];
                search_result->seg_offsets_[index] =
                    search_result->seg_offsets_[offset];
                if (search_result->element_level_) {
                    search_result->element_indices_[index] =
                        search_result->element_indices_[offset];
                }
                index++;
                real_topks[j]++;
            }
        }
        search_result->distances_.resize(index);
        search_result->seg_offsets_.resize(index);
        if (search_result->element_level_) {
            search_result->element_indices_.resize(index);
        }
        search_result->topk_per_nq_prefix_sum_.assign(total_nq_ + 1, 0);
        std::partial_sum(real_topks.begin(),
                         real_topks.end(),
                         search_result->topk_per_nq_prefix_sum_.begin() + 1);
    }
    ResetMergeState();
}

void
ReduceHelper::TruncateSearchResultForOneNQ(int64_t qi, int64_t topk) {
    // Lightweight cursor for distance-only merge (no PKs needed)
    struct Cursor {
        SearchResult* search_result_;
        int64_t segment_index_;
        int64_t offset_;
        int64_t offset_end_;
        float distance_;

        bool
        operator<(const Cursor& rhs) const {
            if (std::fabs(distance_ - rhs.distance_) >= EPSILON) {
                return distance_ < rhs.distance_;
            }
            return segment_index_ > rhs.segment_index_;
        }
    };

    std::priority_queue<Cursor> heap;
    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        auto offset_beg = search_result->topk_per_nq_prefix_sum_[qi];
        auto offset_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
        if (offset_beg == offset_end) {
            continue;
        }
        heap.push({search_result,
                   i,
                   static_cast<int64_t>(offset_beg),
                   static_cast<int64_t>(offset_end),
                   search_result->distances_[offset_beg]});
    }

    int64_t selected = 0;
    while (selected < topk && !heap.empty()) {
        auto cursor = heap.top();
        heap.pop();

        final_search_records_[cursor.segment_index_][qi].push_back(
            cursor.offset_);
        ++selected;

        auto next = cursor.offset_ + 1;
        if (next < cursor.offset_end_) {
            cursor.offset_ = next;
            cursor.distance_ = cursor.search_result_->distances_[next];
            heap.push(cursor);
        }
    }
}

void
ReduceHelper::ResetMergeState() {
    for (auto& search_result : search_results_) {
        search_result->result_offsets_.clear();
    }
    for (auto& seg_records : final_search_records_) {
        for (auto& nq_records : seg_records) {
            nq_records.clear();
        }
    }
}

void
ReduceHelper::RefineOneSegment(SearchResult* search_result,
                               FieldId field_id,
                               bool is_cosine,
                               bool is_negated,
                               int64_t dim,
                               int64_t element_size,
                               const char* dense_blob) {
    auto segment = static_cast<SegmentInterface*>(search_result->segment_);
    auto nq = search_result->total_nq_;
    std::vector<size_t> indices;
    std::vector<float> new_distances;
    auto query_dataset = std::make_shared<knowhere::DataSet>();
    query_dataset->SetRows(1);
    query_dataset->SetDim(dim);
    query_dataset->SetTensorBeginId(0);
    query_dataset->SetIsOwner(false);
    for (int64_t qi = 0; qi < nq; ++qi) {
        auto nq_begin = search_result->topk_per_nq_prefix_sum_[qi];
        auto nq_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
        auto count = nq_end - nq_begin;
        if (count == 0) {
            continue;
        }

        auto query_ptr = dense_blob + qi * element_size;
        query_dataset->SetTensor(query_ptr);

        auto result_count = static_cast<size_t>(count);
        auto* offsets = &search_result->seg_offsets_[nq_begin];
        new_distances.resize(result_count);
        bool ok = segment->CalcDistByIDs(field_id,
                                         query_dataset,
                                         offsets,
                                         count,
                                         is_cosine,
                                         new_distances.data());
        if (!ok) {
            LOG_WARN(
                "failed to refine distances by ids, keep approximate "
                "distances, segment_id={}, field_id={}, nq_idx={}, "
                "candidate_count={}",
                segment->get_segment_id(),
                field_id.get(),
                qi,
                count);
            continue;
        }

        if (is_negated) {
            for (auto& d : new_distances) {
                d *= -1;
            }
        }

        if (count == 1) {
            search_result->distances_[nq_begin] = new_distances[0];
            continue;
        }

        indices.resize(result_count);
        std::iota(indices.begin(), indices.end(), 0);
        std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
            return new_distances[a] > new_distances[b];
        });
        ApplyRefinedOrderForOneNQ(search_result,
                                  static_cast<size_t>(nq_begin),
                                  indices,
                                  new_distances);
    }
}

void
ReduceHelper::RefineDistances() {
    auto& search_info = plan_->plan_node_->search_info_;

    if (placeholder_group_ == nullptr || placeholder_group_->empty()) {
        return;
    }
    auto& placeholder = placeholder_group_->at(0);
    auto field_id = search_info.field_id_;
    bool is_cosine = search_info.metric_type_ == knowhere::metric::COSINE;
    bool is_negated = !PositivelyRelated(search_info.metric_type_);
    auto& field = plan_->schema_->operator[](field_id);
    AssertInfo(field.get_data_type() != DataType::VECTOR_SPARSE_U32_F32,
               "global refine is not supported for sparse vector");
    auto dim = field.get_dim();
    auto element_size = milvus::GetDataTypeSize(field.get_data_type(), dim);
    auto dense_blob = static_cast<const char*>(placeholder.get_blob());

    if (num_segments_ > 1) {
        auto& pool =
            ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::MIDDLE);
        std::vector<std::future<void>> futures;
        futures.reserve(num_segments_);
        for (auto& search_result : search_results_) {
            if (!IsSearchResultRefineEnabled(search_result)) {
                continue;
            }
            auto future = pool.Submit([this,
                                       search_result,
                                       field_id,
                                       is_cosine,
                                       is_negated,
                                       dim,
                                       element_size,
                                       dense_blob] {
                RefineOneSegment(search_result,
                                 field_id,
                                 is_cosine,
                                 is_negated,
                                 dim,
                                 element_size,
                                 dense_blob);
            });
            futures.emplace_back(std::move(future));
        }
        auto futures_guard = folly::makeGuard([&futures]() {
            for (auto& f : futures) {
                if (f.valid()) {
                    try {
                        f.get();
                    } catch (...) {
                    }
                }
            }
        });
        for (auto& future : futures) {
            future.get();
        }
    } else if (num_segments_ == 1 &&
               IsSearchResultRefineEnabled(search_results_[0])) {
        RefineOneSegment(search_results_[0],
                         field_id,
                         is_cosine,
                         is_negated,
                         dim,
                         element_size,
                         dense_blob);
    }
}

void
ReduceHelper::ApplyRefinedOrderForOneNQ(
    SearchResult* search_result,
    size_t nq_begin,
    std::vector<size_t>& indices,
    const std::vector<float>& new_distances) {
    auto* offsets = &search_result->seg_offsets_[nq_begin];
    auto* element_indices = search_result->element_level_
                                ? &search_result->element_indices_[nq_begin]
                                : nullptr;
    auto* distances = search_result->distances_.data() + nq_begin;
    // Update distances at real fixed points up front; the in-place cycle
    // loop below rewrites indices[curr] = curr to mark processed slots,
    // which would otherwise be indistinguishable from a real fixed point.
    for (size_t i = 0; i < indices.size(); ++i) {
        if (indices[i] == i) {
            distances[i] = new_distances[i];
        }
    }
    for (size_t i = 0; i < indices.size();) {
        size_t target = indices[i];
        if (target == i) {
            ++i;
            continue;
        }

        float temp_distance = new_distances[i];
        int64_t temp_offset = offsets[i];
        int32_t temp_elem_idx =
            search_result->element_level_ ? element_indices[i] : -1;

        size_t curr = i;
        while (indices[curr] != i) {
            size_t next = indices[curr];
            distances[curr] = new_distances[next];
            offsets[curr] = offsets[next];
            if (search_result->element_level_) {
                element_indices[curr] = element_indices[next];
            }
            indices[curr] = curr;
            curr = next;
        }

        distances[curr] = temp_distance;
        offsets[curr] = temp_offset;
        if (search_result->element_level_) {
            element_indices[curr] = temp_elem_idx;
        }
        indices[curr] = curr;
    }
}

}  // namespace milvus::segcore
