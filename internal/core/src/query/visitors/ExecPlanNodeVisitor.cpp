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

#include "utils/Json.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentGrowing.h"
#include <utility>
#include "query/generated/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "query/generated/ExecExprVisitor.h"
#include "query/SearchOnGrowing.h"
#include "query/SearchOnSealed.h"
#include "boost_ext/dynamic_bitset_ext.hpp"

namespace milvus::query {

#if 1
namespace impl {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR UNDER suvlim/core_gen/
class ExecPlanNodeVisitor : PlanNodeVisitor {
 public:
    using RetType = SearchResult;
    ExecPlanNodeVisitor(const segcore::SegmentInterface& segment,
                        Timestamp timestamp,
                        const PlaceholderGroup& placeholder_group)
        : segment_(segment), timestamp_(timestamp), placeholder_group_(placeholder_group) {
    }
    // using RetType = nlohmann::json;

    RetType
    get_moved_result(PlanNode& node) {
        assert(!ret_.has_value());
        node.accept(*this);
        assert(ret_.has_value());
        auto ret = std::move(ret_).value();
        ret_ = std::nullopt;
        return ret;
    }

 private:
    template <typename VectorType>
    void
    VectorVisitorImpl(VectorPlanNode& node);

 private:
    // std::optional<RetType> ret_;
    const segcore::SegmentInterface& segment_;
    Timestamp timestamp_;
    const PlaceholderGroup& placeholder_group_;

    std::optional<RetType> ret_;
};
}  // namespace impl
#endif

static SearchResult
empty_search_result(int64_t num_queries, int64_t topk, int64_t round_decimal, MetricType metric_type) {
    SearchResult final_result;
    SubSearchResult result(num_queries, topk, metric_type, round_decimal);
    final_result.num_queries_ = num_queries;
    final_result.topk_ = topk;
    final_result.internal_seg_offsets_ = std::move(result.mutable_labels());
    final_result.result_distances_ = std::move(result.mutable_values());
    return final_result;
}

template <typename VectorType>
void
ExecPlanNodeVisitor::VectorVisitorImpl(VectorPlanNode& node) {
    // TODO: optimize here, remove the dynamic cast
    assert(!ret_.has_value());
    auto segment = dynamic_cast<const segcore::SegmentInternalInterface*>(&segment_);
    AssertInfo(segment, "support SegmentSmallIndex Only");
    RetType ret;
    auto& ph = placeholder_group_.at(0);
    auto src_data = ph.get_blob<EmbeddedType<VectorType>>();
    auto num_queries = ph.num_of_queries_;

    boost::dynamic_bitset<> bitset_holder;
    BitsetView view;
    // TODO: add API to unify row_count
    // auto row_count = segment->get_row_count();
    auto active_count = segment->get_active_count(timestamp_);

    // skip all calculation
    if (active_count == 0) {
        ret_ = empty_search_result(num_queries, node.search_info_.topk_, node.search_info_.round_decimal_,
                                   node.search_info_.metric_type_);
        return;
    }

    if (node.predicate_.has_value()) {
        ExecExprVisitor::RetType expr_ret =
            ExecExprVisitor(*segment, active_count, timestamp_).call_child(*node.predicate_.value());
        bitset_holder = std::move(expr_ret);
    }
    segment->mask_with_timestamps(bitset_holder, timestamp_);

    if (!bitset_holder.empty()) {
        bitset_holder.flip();
        view = BitsetView((uint8_t*)boost_ext::get_data(bitset_holder), bitset_holder.size());
    }

    segment->vector_search(active_count, node.search_info_, src_data, num_queries, MAX_TIMESTAMP, view, ret);

    ret_ = ret;
}

void
ExecPlanNodeVisitor::visit(RetrievePlanNode& node) {
    assert(!retrieve_ret_.has_value());
    auto segment = dynamic_cast<const segcore::SegmentInternalInterface*>(&segment_);
    AssertInfo(segment, "Support SegmentSmallIndex Only");
    RetrieveRetType ret;

    boost::dynamic_bitset<> bitset_holder;
    auto active_count = segment->get_active_count(timestamp_);

    if (active_count == 0) {
        retrieve_ret_ = ret;
        return;
    }

    if (node.predicate_ != nullptr) {
        ExecExprVisitor::RetType expr_ret =
            ExecExprVisitor(*segment, active_count, timestamp_).call_child(*(node.predicate_));
        bitset_holder = std::move(expr_ret);
    }

    segment->mask_with_timestamps(bitset_holder, timestamp_);

    auto seg_offsets = std::move(segment->search_ids(bitset_holder, MAX_TIMESTAMP));
    ret.result_offsets_.assign((int64_t*)seg_offsets.data(), (int64_t*)seg_offsets.data() + seg_offsets.size());
    retrieve_ret_ = ret;
}

void
ExecPlanNodeVisitor::visit(FloatVectorANNS& node) {
    VectorVisitorImpl<FloatVector>(node);
}

void
ExecPlanNodeVisitor::visit(BinaryVectorANNS& node) {
    VectorVisitorImpl<BinaryVector>(node);
}

}  // namespace milvus::query
