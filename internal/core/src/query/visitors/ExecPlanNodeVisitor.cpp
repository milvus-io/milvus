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

#include "query/generated/ExecPlanNodeVisitor.h"

#include <memory>
#include <utility>

#include "expr/ITypeExpr.h"
#include "query/PlanImpl.h"
#include "query/SubSearchResult.h"
#include "query/generated/ExecExprVisitor.h"
#include "query/Utils.h"
#include "segcore/SegmentGrowing.h"
#include "common/Json.h"
#include "log/Log.h"
#include "plan/PlanNode.h"
#include "exec/Task.h"
#include "segcore/SegmentInterface.h"
#include "query/groupby/SearchGroupByOperator.h"
namespace milvus::query {

namespace impl {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR UNDER suvlim/core_gen/
class ExecPlanNodeVisitor : PlanNodeVisitor {
 public:
    ExecPlanNodeVisitor(const segcore::SegmentInterface& segment,
                        Timestamp timestamp,
                        const PlaceholderGroup& placeholder_group)
        : segment_(segment),
          timestamp_(timestamp),
          placeholder_group_(placeholder_group) {
    }

    SearchResult
    get_moved_result(PlanNode& node) {
        assert(!search_result_opt_.has_value());
        node.accept(*this);
        assert(search_result_opt_.has_value());
        auto ret = std::move(search_result_opt_).value();
        search_result_opt_ = std::nullopt;
        return ret;
    }

 private:
    template <typename VectorType>
    void
    VectorVisitorImpl(VectorPlanNode& node);

 private:
    const segcore::SegmentInterface& segment_;
    Timestamp timestamp_;
    const PlaceholderGroup& placeholder_group_;

    SearchResultOpt search_result_opt_;
};
}  // namespace impl

static SearchResult
empty_search_result(int64_t num_queries, SearchInfo& search_info) {
    SearchResult final_result;
    final_result.total_nq_ = num_queries;
    final_result.unity_topK_ = 0;  // no result
    final_result.total_data_cnt_ = 0;
    return final_result;
}

void
ExecPlanNodeVisitor::ExecuteExprNode(
    const std::shared_ptr<milvus::plan::PlanNode>& plannode,
    const milvus::segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    BitsetType& bitset_holder) {
    bitset_holder.clear();
    LOG_DEBUG("plannode: {}, active_count: {}, timestamp: {}",
              plannode->ToString(),
              active_count,
              timestamp_);
    auto plan = plan::PlanFragment(plannode);
    // TODO: get query id from proxy
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, timestamp_);

    auto task =
        milvus::exec::Task::Create(DEFAULT_TASK_ID, plan, 0, query_context);
    bool cache_offset_getted = false;
    for (;;) {
        auto result = task->Next();
        if (!result) {
            break;
        }
        auto childrens = result->childrens();
        AssertInfo(childrens.size() == 1,
                   "expr result vector's children size not equal one");
        LOG_DEBUG("output result length:{}", childrens[0]->size());
        if (auto vec = std::dynamic_pointer_cast<ColumnVector>(childrens[0])) {
            TargetBitmapView view(vec->GetRawData(), vec->size());
            AppendOneChunk(bitset_holder, view);
        } else if (auto row =
                       std::dynamic_pointer_cast<RowVector>(childrens[0])) {
            auto bit_vec =
                std::dynamic_pointer_cast<ColumnVector>(row->child(0));
            TargetBitmapView view(bit_vec->GetRawData(), bit_vec->size());
            AppendOneChunk(bitset_holder, view);

            if (!cache_offset_getted) {
                // offset cache only get once because not support iterator batch
                auto cache_bits_vec =
                    std::dynamic_pointer_cast<ColumnVector>(row->child(1));
                TargetBitmapView view(cache_bits_vec->GetRawData(),
                                      cache_bits_vec->size());
                // If get empty cached bits. mean no record hits in this segment
                // no need to get next batch.
                if (view.count() == 0) {
                    bitset_holder.resize(active_count);
                    task->RequestCancel();
                    break;
                }
                cache_offset_getted = true;
            }
        } else {
            PanicInfo(UnexpectedError, "expr return type not matched");
        }
    }
    //    std::string s;
    //    boost::to_string(*bitset_holder, s);
    //    std::cout << bitset_holder->size() << " .  " << s << std::endl;
}

template <typename VectorType>
void
ExecPlanNodeVisitor::VectorVisitorImpl(VectorPlanNode& node) {
    // TODO: optimize here, remove the dynamic cast
    assert(!search_result_opt_.has_value());
    auto segment =
        dynamic_cast<const segcore::SegmentInternalInterface*>(&segment_);
    AssertInfo(segment, "support SegmentSmallIndex Only");
    SearchResult search_result;
    auto& ph = placeholder_group_->at(0);
    auto src_data = ph.get_blob();
    auto num_queries = ph.num_of_queries_;

    // TODO: add API to unify row_count
    // auto row_count = segment->get_row_count();
    auto active_count = segment->get_active_count(timestamp_);

    // skip all calculation
    if (active_count == 0) {
        search_result_opt_ =
            empty_search_result(num_queries, node.search_info_);
        return;
    }

    std::chrono::high_resolution_clock::time_point scalar_start =
        std::chrono::high_resolution_clock::now();
    std::unique_ptr<BitsetType> bitset_holder;
    if (node.filter_plannode_.has_value()) {
        BitsetType expr_res;
        ExecuteExprNode(
            node.filter_plannode_.value(), segment, active_count, expr_res);
        bitset_holder = std::make_unique<BitsetType>(expr_res.clone());
        bitset_holder->flip();
    } else {
        bitset_holder = std::make_unique<BitsetType>(active_count, false);
    }
    segment->mask_with_timestamps(*bitset_holder, timestamp_);

    segment->mask_with_delete(*bitset_holder, active_count, timestamp_);
    std::chrono::high_resolution_clock::time_point scalar_end =
        std::chrono::high_resolution_clock::now();
    double scalar_cost =
        std::chrono::duration<double, std::micro>(scalar_end - scalar_start)
            .count();
    monitor::internal_core_search_latency_scalar.Observe(scalar_cost);

    // if bitset_holder is all 1's, we got empty result
    if (bitset_holder->all()) {
        search_result_opt_ =
            empty_search_result(num_queries, node.search_info_);
        return;
    }

    std::chrono::high_resolution_clock::time_point vector_start =
        std::chrono::high_resolution_clock::now();
    BitsetView final_view = *bitset_holder;
    segment->vector_search(node.search_info_,
                           src_data,
                           num_queries,
                           timestamp_,
                           final_view,
                           search_result);
    search_result.total_data_cnt_ = final_view.size();
    if (search_result.vector_iterators_.has_value()) {
        AssertInfo(search_result.vector_iterators_.value().size() ==
                       search_result.total_nq_,
                   "Vector Iterators' count must be equal to total_nq_, Check "
                   "your code");
        std::vector<GroupByValueType> group_by_values;
        SearchGroupBy(search_result.vector_iterators_.value(),
                      node.search_info_,
                      group_by_values,
                      *segment,
                      search_result.seg_offsets_,
                      search_result.distances_,
                      search_result.topk_per_nq_prefix_sum_);
        search_result.group_by_values_ = std::move(group_by_values);
        search_result.group_size_ = node.search_info_.group_size_;
        AssertInfo(search_result.seg_offsets_.size() ==
                       search_result.group_by_values_.value().size(),
                   "Wrong state! search_result group_by_values_ size:{} is not "
                   "equal to search_result.seg_offsets.size:{}",
                   search_result.group_by_values_.value().size(),
                   search_result.seg_offsets_.size());
    }
    search_result_opt_ = std::move(search_result);
    std::chrono::high_resolution_clock::time_point vector_end =
        std::chrono::high_resolution_clock::now();
    double vector_cost =
        std::chrono::duration<double, std::micro>(vector_end - vector_start)
            .count();
    monitor::internal_core_search_latency_vector.Observe(vector_cost);

    double total_cost =
        std::chrono::duration<double, std::micro>(vector_end - scalar_start)
            .count();
    double scalar_ratio = total_cost > 0.0 ? scalar_cost / total_cost : 0.0;
    monitor::internal_core_search_latency_scalar_proportion.Observe(
        scalar_ratio);
}

std::unique_ptr<RetrieveResult>
wrap_num_entities(int64_t cnt) {
    auto retrieve_result = std::make_unique<RetrieveResult>();
    DataArray arr;
    arr.set_type(milvus::proto::schema::Int64);
    auto scalar = arr.mutable_scalars();
    scalar->mutable_long_data()->mutable_data()->Add(cnt);
    retrieve_result->field_data_ = {arr};
    retrieve_result->total_data_cnt_ = 0;
    return retrieve_result;
}

void
ExecPlanNodeVisitor::visit(RetrievePlanNode& node) {
    assert(!retrieve_result_opt_.has_value());
    auto segment =
        dynamic_cast<const segcore::SegmentInternalInterface*>(&segment_);
    AssertInfo(segment, "Support SegmentSmallIndex Only");
    RetrieveResult retrieve_result;
    retrieve_result.total_data_cnt_ = 0;

    auto active_count = segment->get_active_count(timestamp_);

    if (active_count == 0 && !node.is_count_) {
        retrieve_result_opt_ = std::move(retrieve_result);
        return;
    }

    if (active_count == 0 && node.is_count_) {
        retrieve_result = *(wrap_num_entities(0));
        retrieve_result_opt_ = std::move(retrieve_result);
        return;
    }

    BitsetType bitset_holder;
    // For case that retrieve by expression, bitset will be allocated when expression is being executed.
    if (node.is_count_) {
        bitset_holder.resize(active_count);
    }

    std::vector<int64_t> cache_offsets;
    if (node.filter_plannode_.has_value()) {
        ExecuteExprNode(node.filter_plannode_.value(),
                        segment,
                        active_count,
                        bitset_holder);
        bitset_holder.flip();
    }

    segment->mask_with_timestamps(bitset_holder, timestamp_);

    segment->mask_with_delete(bitset_holder, active_count, timestamp_);
    // if bitset_holder is all 1's, we got empty result
    if (bitset_holder.all() && !node.is_count_) {
        retrieve_result_opt_ = std::move(retrieve_result);
        return;
    }

    if (node.is_count_) {
        auto cnt = bitset_holder.size() - bitset_holder.count();
        retrieve_result = *(wrap_num_entities(cnt));
        retrieve_result.total_data_cnt_ = bitset_holder.size();
        retrieve_result_opt_ = std::move(retrieve_result);
        return;
    }

    retrieve_result.total_data_cnt_ = bitset_holder.size();
    if (node.offset_bound_ == -1) {
        auto results_pair = std::move(segment->find_first(node.limit_, bitset_holder));
        retrieve_result.result_offsets_ = std::move(results_pair.first);
        retrieve_result.has_more_result = results_pair.second;
    } else {
        auto results_pair = std::move(segment->find_after(node.limit_, node.offset_bound_, bitset_holder));
        retrieve_result.result_offsets_ = std::move(results_pair.first);
        retrieve_result.has_more_result = results_pair.second;
    }

    retrieve_result_opt_ = std::move(retrieve_result);
}

void
ExecPlanNodeVisitor::visit(FloatVectorANNS& node) {
    VectorVisitorImpl<FloatVector>(node);
}

void
ExecPlanNodeVisitor::visit(BinaryVectorANNS& node) {
    VectorVisitorImpl<BinaryVector>(node);
}

void
ExecPlanNodeVisitor::visit(Float16VectorANNS& node) {
    VectorVisitorImpl<Float16Vector>(node);
}

void
ExecPlanNodeVisitor::visit(BFloat16VectorANNS& node) {
    VectorVisitorImpl<BFloat16Vector>(node);
}

void
ExecPlanNodeVisitor::visit(SparseFloatVectorANNS& node) {
    VectorVisitorImpl<SparseFloatVector>(node);
}

}  // namespace milvus::query
