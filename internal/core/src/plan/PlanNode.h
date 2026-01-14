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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "common/Vector.h"
#include "expr/ITypeExpr.h"
#include "common/EasyAssert.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentInterface.h"
#include "plan/PlanNodeIdGenerator.h"
#include "rescores/Scorer.h"

namespace milvus {
namespace plan {

typedef std::string PlanNodeId;
/** 
 * @brief Base class for all logic plan node
 * 
 */
class PlanNode {
 public:
    explicit PlanNode(const PlanNodeId& id) : id_(id) {
    }

    virtual ~PlanNode() = default;

    const PlanNodeId&
    id() const {
        return id_;
    }

    virtual RowTypePtr
    output_type() const = 0;

    virtual std::vector<std::shared_ptr<PlanNode>>
    sources() const = 0;

    virtual bool
    RequireSplits() const {
        return false;
    }

    virtual std::string
    ToString() const = 0;

    virtual std::string_view
    name() const = 0;

    virtual expr::ExprInfo
    GatherInfo() const {
        return {};
    };

    std::string
    SourceToString() const {
        std::vector<std::string> sources_str;
        for (auto& source : sources()) {
            sources_str.emplace_back(source->ToString());
        }
        return "[" + Join(sources_str, ",") + "]";
    }

 private:
    PlanNodeId id_;
};

using PlanNodePtr = std::shared_ptr<PlanNode>;
class FilterNode : public PlanNode {
 public:
    FilterNode(const PlanNodeId& id,
               expr::TypedExprPtr filter,
               std::vector<PlanNodePtr> sources)
        : PlanNode(id),
          sources_{std::move(sources)},
          filter_(std::move(filter)) {
        AssertInfo(
            filter_->type() == DataType::BOOL,
            fmt::format("Filter expression must be of type BOOLEAN, Got {}",
                        filter_->type()));
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    const expr::TypedExprPtr&
    filter() const {
        return filter_;
    }

    std::string_view
    name() const override {
        return "Filter";
    }

    std::string
    ToString() const override {
        return "";
    }

 private:
    const std::vector<PlanNodePtr> sources_;
    const expr::TypedExprPtr filter_;
};

class FilterBitsNode : public PlanNode {
 public:
    FilterBitsNode(
        const PlanNodeId& id,
        expr::TypedExprPtr filter,
        std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id),
          sources_{std::move(sources)},
          filter_(std::move(filter)) {
        AssertInfo(
            filter_->type() == DataType::BOOL,
            fmt::format("Filter expression must be of type BOOLEAN, Got {}",
                        filter_->type()));
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    const expr::TypedExprPtr&
    filter() const {
        return filter_;
    }

    std::string_view
    name() const override {
        return "FilterBits";
    }

    std::string
    ToString() const override {
        return fmt::format("FilterBitsNode:[filter_expr:{}]",
                           filter_->ToString());
    }

    expr::ExprInfo
    GatherInfo() const override {
        expr::ExprInfo info;
        filter_->GatherInfo(info);
        return info;
    }

 private:
    const std::vector<PlanNodePtr> sources_;
    const expr::TypedExprPtr filter_;
};

class ElementFilterNode : public PlanNode {
 public:
    ElementFilterNode(const PlanNodeId& id,
                      expr::TypedExprPtr element_filter,
                      std::string struct_name,
                      std::vector<PlanNodePtr> sources)
        : PlanNode(id),
          sources_{std::move(sources)},
          element_filter_(std::move(element_filter)),
          struct_name_(std::move(struct_name)) {
        AssertInfo(
            element_filter_->type() == DataType::BOOL,
            fmt::format(
                "Element filter expression must be of type BOOLEAN, Got {}",
                element_filter_->type()));
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    const expr::TypedExprPtr&
    element_filter() const {
        return element_filter_;
    }

    const std::string&
    struct_name() const {
        return struct_name_;
    }

    std::string_view
    name() const override {
        return "ElementFilter";
    }

    std::string
    ToString() const override {
        return fmt::format(
            "ElementFilterNode:[struct_name:{}, element_filter:{}]",
            struct_name_,
            element_filter_->ToString());
    }

 private:
    const std::vector<PlanNodePtr> sources_;
    const expr::TypedExprPtr element_filter_;
    const std::string struct_name_;
};

class ElementFilterBitsNode : public PlanNode {
 public:
    ElementFilterBitsNode(
        const PlanNodeId& id,
        expr::TypedExprPtr element_filter,
        std::string struct_name,
        std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id),
          sources_{std::move(sources)},
          element_filter_(std::move(element_filter)),
          struct_name_(std::move(struct_name)) {
        AssertInfo(
            element_filter_->type() == DataType::BOOL,
            fmt::format(
                "Element filter expression must be of type BOOLEAN, Got {}",
                element_filter_->type()));
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    const expr::TypedExprPtr&
    element_filter() const {
        return element_filter_;
    }

    const std::string&
    struct_name() const {
        return struct_name_;
    }

    std::string_view
    name() const override {
        return "ElementFilterBits";
    }

    std::string
    ToString() const override {
        return fmt::format(
            "ElementFilterBitsNode:[struct_name:{}, element_filter:{}]",
            struct_name_,
            element_filter_->ToString());
    }

    expr::ExprInfo
    GatherInfo() const override {
        expr::ExprInfo info;
        element_filter_->GatherInfo(info);
        return info;
    }

 private:
    const std::vector<PlanNodePtr> sources_;
    const expr::TypedExprPtr element_filter_;
    const std::string struct_name_;
};

class ProjectNode : public PlanNode {
 public:
    ProjectNode(const PlanNodeId& id,
                std::vector<FieldId>&& field_ids,
                std::vector<std::string>&& field_names,
                std::vector<milvus::DataType>&& field_types,
                std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id),
          sources_(std::move(sources)),
          field_ids_(std::move(field_ids)),
          output_type_(std::make_shared<RowType>(std::move(field_names),
                                                 std::move(field_types))) {
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    RowTypePtr
    output_type() const override {
        return output_type_;
    }

    std::string_view
    name() const override {
        return "ProjectNode";
    }

    std::string
    ToString() const override {
        return fmt::format("ProjectNode:\n\t[source node:{}]",
                           SourceToString());
    }

    const std::vector<FieldId>&
    FieldsToProject() const {
        return field_ids_;
    }

 private:
    const std::vector<PlanNodePtr> sources_;
    const std::vector<FieldId> field_ids_;
    const RowTypePtr output_type_;
};

class MvccNode : public PlanNode {
 public:
    MvccNode(const PlanNodeId& id,
             std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id), sources_{std::move(sources)} {
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    std::string_view
    name() const override {
        return "MvccNode";
    }

    std::string
    ToString() const override {
        return fmt::format("MvccNode:[source_node:{}]", SourceToString());
    }

 private:
    const std::vector<PlanNodePtr> sources_;
};

class RandomSampleNode : public PlanNode {
 public:
    RandomSampleNode(
        const PlanNodeId& id,
        float factor,
        std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id), factor_(factor), sources_(std::move(sources)) {
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    std::string_view
    name() const override {
        return "RandomSampleNode";
    }

    std::string
    ToString() const override {
        return fmt::format("RandomSampleNode:[factor:{}]", factor_);
    }

    float
    factor() const {
        return factor_;
    }

 private:
    float factor_;
    const std::vector<PlanNodePtr> sources_;
};

class VectorSearchNode : public PlanNode {
 public:
    VectorSearchNode(
        const PlanNodeId& id,
        std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id), sources_{std::move(sources)} {
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    std::string_view
    name() const override {
        return "VectorSearchNode";
    }

    std::string
    ToString() const override {
        return fmt::format("VectorSearchNode:[source_node:{}]",
                           SourceToString());
    }

 private:
    const std::vector<PlanNodePtr> sources_;
};

class SearchGroupByNode : public PlanNode {
 public:
    SearchGroupByNode(
        const PlanNodeId& id,
        std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id), sources_{std::move(sources)} {
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    std::string_view
    name() const override {
        return "SearchGroupByNode";
    }

    std::string
    ToString() const override {
        return fmt::format("SearchGroupByNode:\n\t[source node:{}]",
                           SourceToString());
    }

 private:
    const std::vector<PlanNodePtr> sources_;
};

class SearchOrderByNode : public PlanNode {
 public:
    using OrderByField = plan::OrderByField;

    SearchOrderByNode(
        const PlanNodeId& id,
        std::vector<OrderByField>&& order_by_fields,
        std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{})
        : PlanNode(id),
          sources_{std::move(sources)},
          order_by_fields_{std::move(order_by_fields)} {
    }

    RowTypePtr
    output_type() const override {
        return RowType::None;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    const std::vector<OrderByField>&
    order_by_fields() const {
        return order_by_fields_;
    }

    std::string_view
    name() const override {
        return "SearchOrderByNode";
    }

    std::string
    ToString() const override {
        return fmt::format("SearchOrderByNode:\n\t[source node:{}]",
                           SourceToString());
    }

 private:
    const std::vector<PlanNodePtr> sources_;
    const std::vector<OrderByField> order_by_fields_;
};

class RescoresNode : public PlanNode {
 public:
    RescoresNode(
        const PlanNodeId& id,
        const std::vector<std::shared_ptr<rescores::Scorer>>& scorers,
        const proto::plan::ScoreOption& option,
        const std::vector<PlanNodePtr>& sources = std::vector<PlanNodePtr>{})
        : PlanNode(id),
          scorers_(std::move(scorers)),
          option_(std::move(option)),
          sources_{std::move(sources)} {
    }

    RowTypePtr
    output_type() const override {
        return std::make_shared<const RowType>(
            std::vector<std::string>{"scores"},
            std::vector<milvus::DataType>{DataType::INT64});
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    const proto::plan::ScoreOption*
    option() const {
        return &option_;
    }

    const std::vector<std::shared_ptr<rescores::Scorer>>&
    scorers() const {
        return scorers_;
    }

    std::string_view
    name() const override {
        return "RescoresNode";
    }

    std::string
    ToString() const override {
        return fmt::format("RescoresNode:[source_node:{}]", SourceToString());
    }

 private:
    const proto::plan::ScoreOption option_;
    const std::vector<PlanNodePtr> sources_;
    const std::vector<std::shared_ptr<rescores::Scorer>> scorers_;
};

class AggregationNode : public PlanNode {
 public:
    struct Aggregate {
        /// Function name and input column names.
        expr::CallExprPtr call_;

        /// Raw input types used to properly identify aggregate function.
        std::vector<DataType> rawInputTypes_;

        DataType resultType_;

     public:
        Aggregate(expr::CallExprPtr call) : call_(call) {
        }
    };

    AggregationNode(
        const PlanNodeId& id,
        std::vector<expr::FieldAccessTypeExprPtr>&& groupingKeys,
        std::vector<std::string>&& aggNames,
        std::vector<Aggregate>&& aggregates,
        std::vector<PlanNodePtr> sources = std::vector<PlanNodePtr>{});

    RowTypePtr
    output_type() const override {
        return output_type_;
    }

    std::vector<PlanNodePtr>
    sources() const override {
        return sources_;
    }

    std::string
    ToString() const override {
        return "";
    }

    std::string_view
    name() const override {
        return "agg";
    }

    const std::vector<expr::FieldAccessTypeExprPtr>&
    GroupingKeys() const {
        return groupingKeys_;
    }

    const std::vector<Aggregate>&
    aggregates() const {
        return aggregates_;
    }

 private:
    const std::vector<expr::FieldAccessTypeExprPtr> groupingKeys_;
    const std::vector<std::string> aggregateNames_;
    const std::vector<Aggregate> aggregates_;
    const std::vector<PlanNodePtr> sources_;
    const RowTypePtr output_type_;
};

enum class ExecutionStrategy {
    // Process splits as they come in any available driver.
    kUngrouped,
    // Process splits from each split group only in one driver.
    // It is used when split groups represent separate partitions of the data on
    // the grouping keys or join keys. In that case it is sufficient to keep only
    // the keys from a single split group in a hash table used by group-by or
    // join.
    kGrouped,
};
struct PlanFragment {
    std::shared_ptr<const PlanNode> plan_node_;
    ExecutionStrategy execution_strategy_{ExecutionStrategy::kUngrouped};
    int32_t num_splitgroups_{0};

    PlanFragment() = default;

    inline bool
    IsGroupedExecution() const {
        return execution_strategy_ == ExecutionStrategy::kGrouped;
    }

    explicit PlanFragment(std::shared_ptr<const PlanNode> top_node,
                          ExecutionStrategy strategy,
                          int32_t num_splitgroups)
        : plan_node_(std::move(top_node)),
          execution_strategy_(strategy),
          num_splitgroups_(num_splitgroups) {
    }

    explicit PlanFragment(std::shared_ptr<const PlanNode> top_node)
        : plan_node_(std::move(top_node)) {
    }
};

}  // namespace plan
}  // namespace milvus
