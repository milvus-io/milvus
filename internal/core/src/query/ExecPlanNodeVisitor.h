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

#pragma once
#include "common/Json.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentGrowing.h"
#include <utility>
#include "PlanNodeVisitor.h"
#include "plan/PlanNode.h"
#include "exec/QueryContext.h"
#include "futures/Future.h"
#include "query/SharedFilterBitsetResult.h"

namespace milvus::query {

class ExecPlanNodeVisitor : public PlanNodeVisitor {
 public:
    void
    visit(VectorPlanNode& node) override;

    void
    visit(RetrievePlanNode& node) override;

    // no extra visit for vector types

 public:
    ExecPlanNodeVisitor(const segcore::SegmentInterface& segment,
                        Timestamp timestamp,
                        const PlaceholderGroup* placeholder_group,
                        const folly::CancellationToken& cancel_token =
                            folly::CancellationToken(),
                        int32_t consistency_level = 0,
                        Timestamp collection_ttl = 0)
        : segment_(segment),
          timestamp_(timestamp),
          placeholder_group_(placeholder_group),
          cancel_token_(cancel_token),
          consistency_level_(consistency_level),
          collection_ttl_timestamp_(collection_ttl) {
    }

    // Only used for test
    ExecPlanNodeVisitor(const segcore::SegmentInterface& segment,
                        Timestamp timestamp,
                        const folly::CancellationToken& cancel_token =
                            folly::CancellationToken(),
                        int32_t consistency_level = 0,
                        Timestamp collection_ttl = 0)
        : segment_(segment),
          timestamp_(timestamp),
          cancel_token_(cancel_token),
          consistency_level_(consistency_level),
          collection_ttl_timestamp_(collection_ttl) {
        placeholder_group_ = nullptr;
    }

    SearchResult
    get_moved_result(PlanNode& node) {
        assert(!search_result_opt_.has_value());
        node.accept(*this);
        assert(search_result_opt_.has_value());
        auto ret = std::move(search_result_opt_).value();
        search_result_opt_.reset();
        search_result_opt_ = std::nullopt;
        return ret;
    }

    RetrieveResult
    get_retrieve_result(PlanNode& node) {
        assert(!retrieve_result_opt_.has_value());
        std::cout.flush();
        node.accept(*this);
        assert(retrieve_result_opt_.has_value());
        auto ret = std::move(retrieve_result_opt_).value();
        retrieve_result_opt_.reset();
        retrieve_result_opt_ = std::nullopt;
        return ret;
    }

    void
    SetExprUsePkIndex(bool use_pk_index) {
        expr_use_pk_index_ = use_pk_index;
    }

    bool
    GetExprUsePkIndex() {
        return expr_use_pk_index_;
    }

    // ---- shared-filter hybrid search ----

    // Phase 1: evaluate only the filter subtree and hand back its bitset.
    // Mutually exclusive with SetPrecomputedBitset.
    SharedFilterBitsetResultPtr
    get_shared_filter_bitset_result(PlanNode& node) {
        AssertInfo(precomputed_bitset_result_ == nullptr,
                   "shared filter bitset computation cannot be combined with "
                   "precomputed-bitset mode");
        compute_filter_bitset_only_ = true;
        node.accept(*this);
        compute_filter_bitset_only_ = false;
        AssertInfo(shared_filter_bitset_result_ != nullptr,
                   "shared filter bitset execution produced no result");
        return std::move(shared_filter_bitset_result_);
    }

    // Phase 2: execute one branch against a bitset computed earlier. The
    // caller owns `result` and must keep it alive for the whole call; it is
    // read-only here, so concurrent branches may share one.
    ExecPlanNodeVisitor&
    SetPrecomputedBitset(const SharedFilterBitsetResult* result) {
        precomputed_bitset_result_ = result;
        return *this;
    }

    static BitsetType
    ExecuteTask(plan::PlanFragment& plan,
                std::shared_ptr<milvus::exec::QueryContext> query_context,
                bool collect_bitset = true);

    // Runs a filter prefix and returns its output as the RowVector (bitmap +
    // validity) the vector search consumes, rather than the bare bitset
    // ExecuteTask folds it into.
    static RowVectorPtr
    ExecuteFilterPrefix(
        plan::PlanFragment& plan,
        std::shared_ptr<milvus::exec::QueryContext> query_context);

 private:
    const segcore::SegmentInterface& segment_;
    Timestamp timestamp_;
    const PlaceholderGroup* placeholder_group_;
    folly::CancellationToken cancel_token_;
    int32_t consistency_level_ = 0;
    Timestamp collection_ttl_timestamp_;

    SearchResultOpt search_result_opt_;
    RetrieveResultOpt retrieve_result_opt_;

    bool expr_use_pk_index_ = false;
    bool compute_filter_bitset_only_ = false;
    SharedFilterBitsetResultPtr shared_filter_bitset_result_{nullptr};
    const SharedFilterBitsetResult* precomputed_bitset_result_{nullptr};
};

// for test use only
inline BitsetType
ExecuteQueryExpr(std::shared_ptr<milvus::plan::PlanNode> plannode,
                 const milvus::segcore::SegmentInternalInterface* segment,
                 uint64_t active_count,
                 uint64_t timestamp) {
    auto plan_fragment = plan::PlanFragment(plannode);

    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, timestamp);
    auto bitset =
        ExecPlanNodeVisitor::ExecuteTask(plan_fragment, query_context);

    // For test case, bitset 1 indicates true but executor is verse
    bitset.flip();
    return bitset;
}

}  // namespace milvus::query
