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

#include "monitor/prometheus_client.h"

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
              const int i) {
    auto pos = large_is_better
                   ? find_binsert_position<true>(search_result.distances_,
                                                 nq_index * unity_topk,
                                                 nq_index * unity_topk + topk,
                                                 distances[i])
                   : find_binsert_position<false>(search_result.distances_,
                                                  nq_index * unity_topk,
                                                  nq_index * unity_topk + topk,
                                                  distances[i]);
    if (topk > pos) {
        std::memmove(&search_result.distances_[pos + 1],
                     &search_result.distances_[pos],
                     (topk - pos) * sizeof(float));
        std::memmove(&search_result.seg_offsets_[pos + 1],
                     &search_result.seg_offsets_[pos],
                     (topk - pos) * sizeof(int64_t));
    }
    search_result.seg_offsets_[pos] = offsets[i];
    search_result.distances_[pos] = distances[i];
    ++topk;
}

RowVectorPtr
PhyIterativeFilterNode::GetOutput() {
    if (is_finished_ || !no_more_input_) {
        return nullptr;
    }

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
                    PanicInfo(ExprInvalid,
                              "PhyIterativeFilterNode result should be bitmap");
                }
            } else {
                PanicInfo(
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
        int nq_index = 0;

        search_result.seg_offsets_.resize(nq * unity_topk, INVALID_SEG_OFFSET);
        search_result.distances_.resize(nq * unity_topk);
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
                if (is_native_supported_) {
                    eval_ctx.set_offset_input(&offsets);
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
                } else {
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
    monitor::internal_core_search_latency_iterative_filter.Observe(scalar_cost /
                                                                   1000);

    return input_;
}

}  // namespace exec
}  // namespace milvus
