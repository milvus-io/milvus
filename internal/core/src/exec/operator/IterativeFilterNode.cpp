// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "IterativeFilterNode.h"
#include "common/Tracer.h"
#include "fmt/format.h"

#include "exec/Driver.h"
#include "monitor/Monitor.h"
namespace milvus {
namespace exec {
PhyIterativeFilterNode::PhyIterativeFilterNode(
    int32_t operator_id,
    DriverContext* driverctx,
    const std::shared_ptr<const plan::FilterNode>& filter)
    : Operator(driverctx,
               filter->output_type(),
               operator_id,
               filter->id(),
               "PhyIterativeFilterNode") {
    ExecContext* exec_context = operator_context_->get_exec_context();
    query_context_ = exec_context->get_query_context();
    std::vector<expr::TypedExprPtr> filters;
    filters.emplace_back(filter->filter());
    exprs_ = std::make_unique<ExprSet>(filters, exec_context);
    const auto& exprs = exprs_->exprs();
    for (const auto& expr : exprs) {
        is_native_supported_ =
            (is_native_supported_ && (expr->SupportOffsetInput()));
    }
    need_process_rows_ = query_context_->get_active_count();
    num_processed_rows_ = 0;
}

void
PhyIterativeFilterNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

bool
PhyIterativeFilterNode::IsFinished() {
    return is_finished_;
}

template <bool large_is_better>
inline size_t
find_binsert_position(const std::vector<float>& distances,
                      size_t lo,
                      size_t hi,
                      float dist) {
    while (lo < hi) {
        size_t mid = lo + ((hi - lo) >> 1);
        if constexpr (large_is_better) {
            if (distances[mid] < dist) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        } else {
            if (distances[mid] > dist) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
    }
    return lo;
}

inline void
insert_helper(milvus::SearchResult& search_result,
              int& topk,
              const bool large_is_better,
              const FixedVector<float>& distances,
              const FixedVector<int32_t>& offsets,
              const int64_t nq_index,
              const int64_t unity_topk,
              const int i,
              const IArrayOffsets* array_offsets = nullptr) {
    auto pos = large_is_better
                   ? find_binsert_position<true>(search_result.distances_,
                                                 nq_index * unity_topk,
                                                 nq_index * unity_topk + topk,
                                                 distances[i])
                   : find_binsert_position<false>(search_result.distances_,
                                                  nq_index * unity_topk,
                                                  nq_index * unity_topk + topk,
                                                  distances[i]);

    // For element-level: convert element_id to (doc_id, element_index)
    int64_t doc_id;
    int32_t elem_idx = -1;
    if (array_offsets != nullptr) {
        auto [doc, idx] = array_offsets->ElementIDToDoc(offsets[i]);
        doc_id = doc;
        elem_idx = idx;
    } else {
        doc_id = offsets[i];
    }

    if (topk > pos) {
        std::memmove(&search_result.distances_[pos + 1],
                     &search_result.distances_[pos],
                     (topk - pos) * sizeof(float));
        std::memmove(&search_result.seg_offsets_[pos + 1],
                     &search_result.seg_offsets_[pos],
                     (topk - pos) * sizeof(int64_t));
        if (array_offsets != nullptr) {
            std::memmove(&search_result.element_indices_[pos + 1],
                         &search_result.element_indices_[pos],
                         (topk - pos) * sizeof(int32_t));
        }
    }
    search_result.seg_offsets_[pos] = doc_id;
    if (array_offsets != nullptr) {
        search_result.element_indices_[pos] = elem_idx;
    }
    search_result.distances_[pos] = distances[i];
    ++topk;
}

RowVectorPtr
PhyIterativeFilterNode::GetOutput() {
    milvus::exec::checkCancellation(query_context_);

    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

    tracer::AutoSpan span(
        "PhyIterativeFilterNode::Execute", tracer::GetRootSpan(), true);

    DeferLambda([&]() { is_finished_ = true; });

    if (input_ == nullptr) {
        return nullptr;
    }

    std::chrono::high_resolution_clock::time_point scalar_start =
        std::chrono::high_resolution_clock::now();

    milvus::SearchResult search_result = query_context_->get_search_result();
    int64_t nq = search_result.total_nq_;
    int64_t unity_topk = search_result.unity_topK_;
    knowhere::MetricType metric_type = query_context_->get_metric_type();
    bool large_is_better = PositivelyRelated(metric_type);
    TargetBitmap bitset;
    // get bitset of whole segment first
    if (!is_native_supported_) {
        EvalCtx eval_ctx(operator_context_->get_exec_context(), exprs_.get());

        TargetBitmap valid_bitset;
        while (num_processed_rows_ < need_process_rows_) {
            exprs_->Eval(0, 1, true, eval_ctx, results_);

            AssertInfo(
                results_.size() == 1 && results_[0] != nullptr,
                "PhyIterativeFilterNode result size should be size one and not "
                "be nullptr");

            if (auto col_vec =
                    std::dynamic_pointer_cast<ColumnVector>(results_[0])) {
                if (col_vec->IsBitmap()) {
                    auto col_vec_size = col_vec->size();
                    TargetBitmapView view(col_vec->GetRawData(), col_vec_size);
                    bitset.append(view);
                    TargetBitmapView valid_view(col_vec->GetValidRawData(),
                                                col_vec_size);
                    valid_bitset.append(valid_view);
                    num_processed_rows_ += col_vec_size;
                } else {
                    ThrowInfo(ExprInvalid,
                              "PhyIterativeFilterNode result should be bitmap");
                }
            } else {
                ThrowInfo(
                    ExprInvalid,
                    "PhyIterativeFilterNode result should be ColumnVector");
            }
        }
        Assert(bitset.size() == need_process_rows_);
        Assert(valid_bitset.size() == need_process_rows_);
    }
    if (search_result.vector_iterators_.has_value()) {
        AssertInfo(search_result.vector_iterators_.value().size() ==
                       search_result.total_nq_,
                   "Vector Iterators' count must be equal to total_nq_, Check "
                   "your code");

        bool element_level = search_result.element_level_;
        auto array_offsets = query_context_->get_array_offsets();

        // For element-level, we need array_offsets to convert element_id → doc_id
        if (element_level) {
            AssertInfo(
                array_offsets != nullptr,
                "Array offsets required for element-level iterative filter");
        }

        int nq_index = 0;

        search_result.seg_offsets_.resize(nq * unity_topk, INVALID_SEG_OFFSET);
        search_result.distances_.resize(nq * unity_topk);
        if (element_level) {
            search_result.element_indices_.resize(nq * unity_topk, -1);
        }

        // Reuse memory allocation across batches and nqs
        FixedVector<int32_t> doc_offsets;
        std::vector<int64_t> element_to_doc_mapping;
        std::unordered_map<int64_t, bool> doc_eval_cache;
        std::unordered_set<int64_t> unique_doc_ids;

        for (auto& iterator : search_result.vector_iterators_.value()) {
            EvalCtx eval_ctx(operator_context_->get_exec_context(),
                             exprs_.get());
            int topk = 0;
            while (iterator->HasNext() && topk < unity_topk) {
                FixedVector<int32_t> offsets;
                FixedVector<float> distances;
                // remain unfilled size as iterator batch size
                int64_t batch_size = unity_topk - topk;
                offsets.reserve(batch_size);
                distances.reserve(batch_size);
                while (iterator->HasNext()) {
                    auto offset_dis_pair = iterator->Next();
                    AssertInfo(
                        offset_dis_pair.has_value(),
                        "Wrong state! iterator cannot return valid result "
                        "whereas it still"
                        "tells hasNext, terminate operation");
                    auto offset = offset_dis_pair.value().first;
                    auto dis = offset_dis_pair.value().second;
                    offsets.emplace_back(offset);
                    distances.emplace_back(dis);
                    if (offsets.size() == batch_size) {
                        break;
                    }
                }

                // Clear but retain capacity
                doc_offsets.clear();
                element_to_doc_mapping.clear();
                doc_eval_cache.clear();
                unique_doc_ids.clear();

                if (element_level) {
                    // 1. Convert element_ids to doc_ids and do filter on those doc_ids
                    // 2. element_ids with doc_ids that pass the filter are what we interested in
                    element_to_doc_mapping.reserve(offsets.size());

                    for (auto element_id : offsets) {
                        auto [doc_id, elem_index] =
                            array_offsets->ElementIDToDoc(element_id);
                        element_to_doc_mapping.push_back(doc_id);
                        unique_doc_ids.insert(doc_id);
                    }

                    doc_offsets.reserve(unique_doc_ids.size());
                    for (auto doc_id : unique_doc_ids) {
                        doc_offsets.emplace_back(static_cast<int32_t>(doc_id));
                    }
                } else {
                    doc_offsets = offsets;
                }

                if (is_native_supported_) {
                    eval_ctx.set_offset_input(&doc_offsets);
                    std::vector<VectorPtr> results;
                    exprs_->Eval(0, 1, true, eval_ctx, results);
                    AssertInfo(
                        results.size() == 1 && results[0] != nullptr,
                        "PhyIterativeFilterNode result size should be size "
                        "one and not "
                        "be nullptr");

                    auto col_vec =
                        std::dynamic_pointer_cast<ColumnVector>(results[0]);
                    auto col_vec_size = col_vec->size();
                    TargetBitmapView bitsetview(col_vec->GetRawData(),
                                                col_vec_size);

                    if (element_level) {
                        Assert(bitsetview.size() == doc_offsets.size());
                        for (size_t i = 0; i < doc_offsets.size(); ++i) {
                            doc_eval_cache[doc_offsets[i]] =
                                (bitsetview[i] > 0);
                        }

                        for (size_t i = 0; i < offsets.size(); ++i) {
                            int64_t doc_id = element_to_doc_mapping[i];
                            if (doc_eval_cache[doc_id]) {
                                insert_helper(search_result,
                                              topk,
                                              large_is_better,
                                              distances,
                                              offsets,
                                              nq_index,
                                              unity_topk,
                                              i,
                                              array_offsets.get());
                                if (topk == unity_topk) {
                                    break;
                                }
                            }
                        }
                    } else {
                        Assert(bitsetview.size() <= batch_size);
                        Assert(bitsetview.size() == offsets.size());
                        for (auto i = 0; i < offsets.size(); ++i) {
                            if (bitsetview[i] > 0) {
                                insert_helper(search_result,
                                              topk,
                                              large_is_better,
                                              distances,
                                              offsets,
                                              nq_index,
                                              unity_topk,
                                              i);
                                if (topk == unity_topk) {
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    Assert(!element_level);
                    for (auto i = 0; i < offsets.size(); ++i) {
                        if (bitset[offsets[i]] > 0) {
                            insert_helper(search_result,
                                          topk,
                                          large_is_better,
                                          distances,
                                          offsets,
                                          nq_index,
                                          unity_topk,
                                          i);
                            if (topk == unity_topk) {
                                break;
                            }
                        }
                    }
                }
                if (topk == unity_topk) {
                    break;
                }
            }
            nq_index++;
        }
    }
    query_context_->set_search_result(std::move(search_result));
    std::chrono::high_resolution_clock::time_point scalar_end =
        std::chrono::high_resolution_clock::now();
    double scalar_cost =
        std::chrono::duration<double, std::micro>(scalar_end - scalar_start)
            .count();
    milvus::monitor::internal_core_search_latency_iterative_filter.Observe(
        scalar_cost / 1000);

    if (!is_native_supported_) {
        tracer::AddEvent(fmt::format("total_processed: {}, matched: {}",
                                     need_process_rows_,
                                     need_process_rows_ - bitset.count()));
    }

    return input_;
}

}  // namespace exec
}  // namespace milvus
