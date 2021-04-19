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

namespace milvus::query {

#if 1
namespace impl {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR UNDER suvlim/core_gen/
class ExecPlanNodeVisitor : PlanNodeVisitor {
 public:
    using RetType = QueryResult;
    ExecPlanNodeVisitor(const segcore::SegmentGrowing& segment,
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
    const segcore::SegmentGrowing& segment_;
    Timestamp timestamp_;
    const PlaceholderGroup& placeholder_group_;

    std::optional<RetType> ret_;
};
}  // namespace impl
#endif

template <typename VectorType>
void
ExecPlanNodeVisitor::VectorVisitorImpl(VectorPlanNode& node) {
    // TODO: optimize here, remove the dynamic cast
    assert(!ret_.has_value());
    auto segment = dynamic_cast<const segcore::SegmentGrowingImpl*>(&segment_);
    AssertInfo(segment, "support SegmentSmallIndex Only");
    RetType ret;
    auto& ph = placeholder_group_.at(0);
    auto src_data = ph.get_blob<EmbeddedType<VectorType>>();
    auto num_queries = ph.num_of_queries_;

    aligned_vector<uint8_t> bitset_holder;
    BitsetView view;
    // TODO: add API to unify row_count
    auto row_count = segcore::get_barrier(segment->get_insert_record(), timestamp_);

    if (node.predicate_.has_value()) {
        ExecExprVisitor::RetType expr_ret = ExecExprVisitor(*segment, row_count).call_child(*node.predicate_.value());
        bitset_holder = AssembleNegBitmap(expr_ret);
        view = BitsetView(bitset_holder.data(), bitset_holder.size() * 8);
    }

    auto& sealed_indexing = segment->get_sealed_indexing_record();
    if (sealed_indexing.is_ready(node.query_info_.field_offset_)) {
        SearchOnSealed(segment->get_schema(), sealed_indexing, node.query_info_, src_data, num_queries, view, ret);
    } else {
        SearchOnGrowing<VectorType>(*segment, row_count, node.query_info_, src_data, num_queries, view, ret);
    }

    ret_ = ret;
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
