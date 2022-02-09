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
// Generated File
// DO NOT EDIT
#include "utils/Json.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentGrowing.h"
#include <utility>
#include "PlanNodeVisitor.h"

namespace milvus::query {
class ExecPlanNodeVisitor : public PlanNodeVisitor {
 public:
    void
    visit(FloatVectorANNS& node) override;

    void
    visit(BinaryVectorANNS& node) override;

    void
    visit(RetrievePlanNode& node) override;

 public:
    using RetType = SearchResult;
    using RetrieveRetType = RetrieveResult;
    ExecPlanNodeVisitor(const segcore::SegmentInterface& segment,
                        Timestamp timestamp,
                        const PlaceholderGroup& placeholder_group)
        : segment_(segment), timestamp_(timestamp), placeholder_group_(placeholder_group) {
    }

    ExecPlanNodeVisitor(const segcore::SegmentInterface& segment, Timestamp timestamp)
        : segment_(segment), timestamp_(timestamp) {
    }

    RetType
    get_moved_result(PlanNode& node) {
        assert(!ret_.has_value());
        node.accept(*this);
        assert(ret_.has_value());
        auto ret = std::move(ret_).value();
        ret_ = std::nullopt;
        return ret;
    }

    RetrieveRetType
    get_retrieve_result(PlanNode& node) {
        assert(!retrieve_ret_.has_value());
        std::cout.flush();
        node.accept(*this);
        assert(retrieve_ret_.has_value());
        auto retrieve_ret = std::move(retrieve_ret_).value();
        retrieve_ret_.reset();
        retrieve_ret_ = std::nullopt;
        return retrieve_ret;
    }

 private:
    template <typename VectorType>
    void
    VectorVisitorImpl(VectorPlanNode& node);

 private:
    // std::optional<RetType> ret_;
    const segcore::SegmentInterface& segment_;
    Timestamp timestamp_;
    PlaceholderGroup placeholder_group_;

    std::optional<RetType> ret_;
    std::optional<RetrieveResult> retrieve_ret_;
};
}  // namespace milvus::query
