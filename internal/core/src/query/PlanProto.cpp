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

#include "PlanProto.h"

#include <google/protobuf/text_format.h>
#include <algorithm>
#include <cstddef>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "NamedType/underlying_functionalities.hpp"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/SystemProperty.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/protobuf_utils.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "glog/logging.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/comp/materialized_view.h"
#include "knowhere/config.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "nlohmann/json_fwd.hpp"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "plan/PlanNodeIdGenerator.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "rescores/Scorer.h"

namespace milvus::query {
namespace planpb = milvus::proto::plan;

void
ProtoParser::PlanOptionsFromProto(
    const proto::plan::PlanOption& plan_option_proto,
    PlanOptions& plan_options) {
    plan_options.expr_use_json_stats = plan_option_proto.expr_use_json_stats();
    LOG_TRACE("plan_options.expr_use_json_stats: {}",
              plan_options.expr_use_json_stats);
}

expr::TypedExprPtr
MergeExprWithNamespace(const SchemaPtr schema,
                       const expr::TypedExprPtr& expr,
                       const std::string& namespace_) {
    auto namespace_field_id =
        schema->get_field_id(FieldName(NAMESPACE_FIELD_NAME));
    proto::plan::GenericValue namespace_value;
    namespace_value.set_string_val(namespace_);

    auto namespace_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(namespace_field_id, DataType::VARCHAR),
        OpType::Equal,
        namespace_value,
        std::vector<proto::plan::GenericValue>{});
    auto and_expr = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        milvus::expr::LogicalBinaryExpr::OpType::And, expr, namespace_expr);
    return and_expr;
}

SearchInfo
ProtoParser::ParseSearchInfo(const planpb::VectorANNS& anns_proto) {
    SearchInfo search_info;
    auto& query_info_proto = anns_proto.query_info();
    auto field_id = FieldId(anns_proto.field_id());
    search_info.field_id_ = field_id;

    search_info.metric_type_ = query_info_proto.metric_type();
    search_info.topk_ = query_info_proto.topk();
    search_info.round_decimal_ = query_info_proto.round_decimal();
    search_info.search_params_ =
        nlohmann::json::parse(query_info_proto.search_params());
    search_info.materialized_view_involved =
        query_info_proto.materialized_view_involved();
    // currently, iterative filter does not support range search
    if (!search_info.search_params_.contains(RADIUS)) {
        if (query_info_proto.hints() != "") {
            if (query_info_proto.hints() == "disable") {
                search_info.iterative_filter_execution = false;
            } else if (query_info_proto.hints() == ITERATIVE_FILTER) {
                search_info.iterative_filter_execution = true;
            } else {
                // check if hints is valid
                ThrowInfo(ConfigInvalid,
                          "hints: {} not supported",
                          query_info_proto.hints());
            }
        } else if (search_info.search_params_.contains(HINTS)) {
            if (search_info.search_params_[HINTS] == ITERATIVE_FILTER) {
                search_info.iterative_filter_execution = true;
            } else {
                // check if hints is valid
                ThrowInfo(ConfigInvalid,
                          "hints: {} not supported",
                          search_info.search_params_[HINTS]);
            }
        }
    }

    if (query_info_proto.bm25_avgdl() > 0) {
        search_info.search_params_[knowhere::meta::BM25_AVGDL] =
            query_info_proto.bm25_avgdl();
    }

    if (query_info_proto.group_by_field_id() > 0) {
        auto group_by_field_id = FieldId(query_info_proto.group_by_field_id());
        search_info.group_by_field_id_ = group_by_field_id;
        search_info.group_size_ = query_info_proto.group_size() > 0
                                      ? query_info_proto.group_size()
                                      : 1;
        search_info.strict_group_size_ = query_info_proto.strict_group_size();
        // Always set json_path to distinguish between unset and empty string
        // Empty string means accessing the entire JSON object
        search_info.json_path_ = query_info_proto.json_path();
        if (query_info_proto.json_type() !=
            milvus::proto::schema::DataType::None) {
            search_info.json_type_ =
                static_cast<milvus::DataType>(query_info_proto.json_type());
        }
        search_info.strict_cast_ = query_info_proto.strict_cast();
    }

    if (query_info_proto.has_search_iterator_v2_info()) {
        auto& iterator_v2_info_proto =
            query_info_proto.search_iterator_v2_info();
        search_info.iterator_v2_info_ = SearchIteratorV2Info{
            .token = iterator_v2_info_proto.token(),
            .batch_size = iterator_v2_info_proto.batch_size(),
        };
        if (iterator_v2_info_proto.has_last_bound()) {
            search_info.iterator_v2_info_->last_bound =
                iterator_v2_info_proto.last_bound();
        }
    }

    return search_info;
}

std::string
getAggregateOpName(planpb::AggregateOp op) {
    switch (op) {
        case planpb::sum:
            return "sum";
        case planpb::count:
            return "count";
        case planpb::avg:
            return "avg";
        case planpb::min:
            return "min";
        case planpb::max:
            return "max";
        default:
            ThrowInfo(OpTypeInvalid, "Unknown op type for aggregation");
    }
}

namespace {
// Helper function to build FilterBitsNode and RandomSampleNode
plan::PlanNodePtr
BuildFilterAndSampleNodes(const proto::plan::QueryPlanNode& query,
                          const planpb::PlanNode& plan_node_proto,
                          const SchemaPtr& schema,
                          const std::vector<plan::PlanNodePtr>& sources,
                          ProtoParser* parser) {
    if (!query.has_predicates()) {
        return nullptr;
    }

    auto parse_expr_to_filter_node =
        [&](const proto::plan::Expr& predicate_proto) -> plan::PlanNodePtr {
        auto expr = parser->ParseExprs(predicate_proto);
        if (plan_node_proto.has_namespace_()) {
            expr = MergeExprWithNamespace(
                schema, expr, plan_node_proto.namespace_());
        }
        return std::make_shared<plan::FilterBitsNode>(
            milvus::plan::GetNextPlanNodeId(), expr, sources);
    };

    auto* predicate_proto = &query.predicates();
    if (predicate_proto->expr_case() == proto::plan::Expr::kRandomSampleExpr) {
        // Predicate exists in random_sample_expr means we encounter expression
        // like "`predicate expression` && random_sample(...)". Extract it to construct
        // FilterBitsNode and make it be executed before RandomSampleNode.
        auto& sample_expr = predicate_proto->random_sample_expr();
        plan::PlanNodePtr filter_node = nullptr;
        if (sample_expr.has_predicate()) {
            filter_node = parse_expr_to_filter_node(sample_expr.predicate());
        }

        std::vector<plan::PlanNodePtr> sample_sources;
        if (filter_node) {
            sample_sources = {filter_node};
        } else {
            sample_sources = sources;
        }

        return std::make_shared<plan::RandomSampleNode>(
            milvus::plan::GetNextPlanNodeId(),
            sample_expr.sample_factor(),
            sample_sources);
    } else {
        return parse_expr_to_filter_node(query.predicates());
    }
}

// Helper function to process group_by fields
void
ProcessGroupByFields(const proto::plan::QueryPlanNode& query,
                     const SchemaPtr& schema,
                     std::vector<expr::FieldAccessTypeExprPtr>& groupingKeys,
                     std::vector<FieldId>& project_id_list,
                     std::vector<std::string>& project_name_list,
                     std::vector<milvus::DataType>& project_type_list) {
    auto group_by_field_count = query.group_by_field_ids_size();
    groupingKeys.reserve(group_by_field_count);
    project_id_list.reserve(group_by_field_count);
    project_name_list.reserve(group_by_field_count);
    project_type_list.reserve(group_by_field_count);

    auto insert_project_field_if_not_exist = [&](FieldId field_id,
                                                 const std::string& field_name,
                                                 milvus::DataType field_type) {
        if (std::count(project_id_list.begin(),
                       project_id_list.end(),
                       field_id) == 0) {
            project_id_list.emplace_back(field_id);
            project_name_list.emplace_back(field_name);
            project_type_list.emplace_back(field_type);
        }
    };

    for (int i = 0; i < group_by_field_count; i++) {
        auto input_field_id = query.group_by_field_ids(i);
        AssertInfo(input_field_id > 0,
                   "input field_id to group by must be positive, "
                   "but is:{}",
                   input_field_id);
        auto field_id = FieldId(input_field_id);
        auto field_type = schema->GetFieldType(field_id);
        auto field_name = schema->GetFieldName(field_id);
        groupingKeys.emplace_back(
            std::make_shared<const expr::FieldAccessTypeExpr>(
                field_type, field_name, field_id));
        insert_project_field_if_not_exist(field_id, field_name, field_type);
    }
}

// Helper function to process aggregates
void
ProcessAggregates(const proto::plan::QueryPlanNode& query,
                  const SchemaPtr& schema,
                  std::vector<plan::AggregationNode::Aggregate>& aggregates,
                  std::vector<std::string>& agg_names,
                  std::vector<FieldId>& project_id_list,
                  std::vector<std::string>& project_name_list,
                  std::vector<milvus::DataType>& project_type_list) {
    aggregates.reserve(query.aggregates_size());
    agg_names.reserve(query.aggregates_size());

    auto insert_project_field_if_not_exist = [&](FieldId field_id,
                                                 const std::string& field_name,
                                                 milvus::DataType field_type) {
        if (std::count(project_id_list.begin(),
                       project_id_list.end(),
                       field_id) == 0) {
            project_id_list.emplace_back(field_id);
            project_name_list.emplace_back(field_name);
            project_type_list.emplace_back(field_type);
        }
    };

    for (int i = 0; i < query.aggregates_size(); i++) {
        auto aggregate = query.aggregates(i);
        auto agg_name = getAggregateOpName(aggregate.op());
        agg_names.emplace_back(agg_name);
        auto input_agg_field_id = aggregate.field_id();
        if (input_agg_field_id == 0) {
            // count(*) do not need input project columns
            auto call = std::make_shared<const expr::CallExpr>(
                agg_name, std::vector<expr::TypedExprPtr>{}, nullptr);
            aggregates.emplace_back(plan::AggregationNode::Aggregate{call});
            aggregates.back().resultType_ =
                GetAggResultType(agg_name, DataType::NONE);
        } else {
            AssertInfo(input_agg_field_id > 0,
                       "input field_id to aggregate must be "
                       "positive or zero, but is:{}",
                       input_agg_field_id);
            auto field_id = FieldId(input_agg_field_id);
            auto field_type = schema->GetFieldType(field_id);
            auto field_name = schema->GetFieldName(field_id);
            auto agg_input = std::make_shared<const expr::FieldAccessTypeExpr>(
                field_type, field_name, field_id);
            auto call = std::make_shared<const expr::CallExpr>(
                agg_name, std::vector<expr::TypedExprPtr>{agg_input}, nullptr);
            aggregates.emplace_back(plan::AggregationNode::Aggregate(call));
            aggregates.back().rawInputTypes_.emplace_back(field_type);
            aggregates.back().resultType_ =
                GetAggResultType(agg_name, field_type);
            insert_project_field_if_not_exist(field_id, field_name, field_type);
        }
    }
}

// Helper function to build ProjectNode and AggregationNode
plan::PlanNodePtr
BuildProjectAndAggregationNodes(
    const proto::plan::QueryPlanNode& query,
    const std::vector<plan::PlanNodePtr>& sources,
    std::vector<expr::FieldAccessTypeExprPtr> groupingKeys,
    std::vector<std::string> agg_names,
    std::vector<plan::AggregationNode::Aggregate> aggregates,
    std::vector<FieldId> project_id_list,
    std::vector<std::string> project_name_list,
    std::vector<milvus::DataType> project_type_list) {
    plan::PlanNodePtr plannode = sources.empty() ? nullptr : sources[0];

    // Build ProjectNode if needed
    if (!project_id_list.empty()) {
        auto project_field_id_list = std::vector<FieldId>(
            project_id_list.begin(), project_id_list.end());
        plannode = std::make_shared<plan::ProjectNode>(
            milvus::plan::GetNextPlanNodeId(),
            std::move(project_field_id_list),
            std::move(project_name_list),
            std::move(project_type_list),
            sources);
    }

    // Build AggregationNode
    std::vector<plan::PlanNodePtr> agg_sources =
        plannode ? std::vector<plan::PlanNodePtr>{plannode} : sources;
    return std::make_shared<plan::AggregationNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(groupingKeys),
        std::move(agg_names),
        std::move(aggregates),
        agg_sources);
}
// Helper function to build ProjectNode for ORDER BY queries.
// Returns {ProjectNode, deferred_field_ids, pipeline_field_ids}.
// deferred_field_ids is empty for single-project mode (all columns materialized
// in the first project), or non-empty for two-project mode (variable-width
// non-sort output columns deferred until after TopK).
// pipeline_field_ids mirrors project_ids so FillOrderByResult can stamp
// the correct field_id on each DataArray produced by the pipeline.
std::tuple<plan::PlanNodePtr, std::vector<FieldId>, std::vector<FieldId>>
BuildOrderByProjectNode(const proto::plan::QueryPlanNode& query,
                        const planpb::PlanNode& plan_node_proto,
                        const SchemaPtr& schema,
                        const std::vector<plan::PlanNodePtr>& sources) {
    std::vector<FieldId> project_ids;
    std::vector<std::string> project_names;
    std::vector<milvus::DataType> project_types;

    // Positional layout contract:
    //   [pk, orderby_fields, non-sort-output-fields]
    // PK at position 0 for proxy reduce/dedup.
    // ORDER BY fields at positions 1..N for sorting.
    // Remaining output fields at positions N+1..M.
    std::set<int64_t> seen_field_ids;
    auto pk_field_id = schema->get_primary_field_id();
    if (pk_field_id.has_value()) {
        auto pk_fid = pk_field_id.value();
        seen_field_ids.insert(pk_fid.get());
        project_ids.push_back(pk_fid);
        project_names.push_back(schema->GetFieldName(pk_fid));
        project_types.push_back(schema->GetFieldType(pk_fid));
    }
    auto order_by_field_count = query.order_by_fields_size();
    for (int i = 0; i < order_by_field_count; i++) {
        auto fid_raw = query.order_by_fields(i).field_id();
        if (seen_field_ids.insert(fid_raw).second) {
            auto fid = FieldId(fid_raw);
            project_ids.push_back(fid);
            project_names.push_back(schema->GetFieldName(fid));
            project_types.push_back(schema->GetFieldType(fid));
        }
    }

    // Collect non-sort output fields and check for variable-width types.
    // Skip system fields (RowFieldID, TimestampFieldID) — they are handled
    // separately in FillTargetEntry and must not enter the pipeline.
    std::vector<FieldId> non_sort_output_fields;
    bool has_variable_width = false;
    for (auto fid_raw : plan_node_proto.output_field_ids()) {
        if (seen_field_ids.count(fid_raw) == 0) {
            auto fid = FieldId(fid_raw);
            if (SystemProperty::Instance().IsSystem(fid)) {
                continue;
            }
            non_sort_output_fields.push_back(fid);
            if (IsVariableDataType(schema->GetFieldType(fid))) {
                has_variable_width = true;
            }
        }
    }

    std::vector<FieldId> deferred_field_ids;
    if (has_variable_width) {
        // Two-project mode: defer ALL non-sort output fields until after TopK.
        deferred_field_ids = non_sort_output_fields;
    } else {
        // Single-project mode: materialize all columns in the first project.
        for (auto& fid : non_sort_output_fields) {
            seen_field_ids.insert(fid.get());
            project_ids.push_back(fid);
            project_names.push_back(schema->GetFieldName(fid));
            project_types.push_back(schema->GetFieldType(fid));
        }
    }

    // Always append SegmentOffsetFieldID as the last pipeline column.
    // FillOrderByResult uses these offsets to populate system fields
    // (e.g., TimestampField for QN-side pk+ts dedup) and, in two-project
    // mode, to bulk-fetch deferred fields via late materialization.
    project_ids.push_back(SegmentOffsetFieldID);
    project_names.push_back("SegmentOffset");
    project_types.push_back(DataType::INT64);

    // Save pipeline field IDs before moving project_ids into ProjectNode.
    auto pipeline_field_ids = project_ids;

    auto plannode =
        std::make_shared<plan::ProjectNode>(milvus::plan::GetNextPlanNodeId(),
                                            std::move(project_ids),
                                            std::move(project_names),
                                            std::move(project_types),
                                            sources);
    return {
        plannode, std::move(deferred_field_ids), std::move(pipeline_field_ids)};
}

// Helper function to build OrderByNode with sorting keys.
plan::PlanNodePtr
BuildOrderByNode(const proto::plan::QueryPlanNode& query,
                 const SchemaPtr& schema,
                 const std::vector<plan::PlanNodePtr>& sources) {
    auto order_by_field_count = query.order_by_fields_size();
    std::vector<expr::FieldAccessTypeExprPtr> sorting_keys;
    std::vector<plan::SortOrder> sorting_orders;
    sorting_keys.reserve(order_by_field_count);
    sorting_orders.reserve(order_by_field_count);

    for (int i = 0; i < order_by_field_count; i++) {
        auto& order_by_field = query.order_by_fields(i);
        auto input_field_id = order_by_field.field_id();
        AssertInfo(input_field_id > 0,
                   "input field_id to order by must be positive, "
                   "but is:{}",
                   input_field_id);
        auto field_id = FieldId(input_field_id);
        auto field_type = schema->GetFieldType(field_id);
        auto field_name = schema->GetFieldName(field_id);

        sorting_keys.emplace_back(
            std::make_shared<const expr::FieldAccessTypeExpr>(
                field_type, field_name, field_id));
        sorting_orders.emplace_back(plan::SortOrder(
            order_by_field.ascending(), order_by_field.nulls_first()));
    }

    return std::make_shared<plan::OrderByNode>(
        milvus::plan::GetNextPlanNodeId(),
        std::move(sorting_keys),
        std::move(sorting_orders),
        query.limit(),
        sources);
}
}  // namespace

std::unique_ptr<VectorPlanNode>
ProtoParser::PlanNodeFromProto(const planpb::PlanNode& plan_node_proto) {
    Assert(plan_node_proto.has_vector_anns());
    auto& anns_proto = plan_node_proto.vector_anns();

    // Parse search information from proto
    auto plan_node = std::make_unique<VectorPlanNode>();
    plan_node->placeholder_tag_ = anns_proto.placeholder_tag();
    plan_node->search_info_ = ParseSearchInfo(anns_proto);

    milvus::plan::PlanNodePtr plannode;
    std::vector<milvus::plan::PlanNodePtr> sources;

    // Build plan node chain based on predicate and filter execution strategy
    if (anns_proto.has_predicates()) {
        auto* predicate_proto = &anns_proto.predicates();
        bool is_element_level = predicate_proto->expr_case() ==
                                proto::plan::Expr::kElementFilterExpr;

        // Parse expressions based on filter type (similar to RandomSampleExpr pattern)
        expr::TypedExprPtr element_expr = nullptr;
        expr::TypedExprPtr doc_expr = nullptr;
        std::string struct_name;

        if (is_element_level) {
            // Element-level query: extract both element_expr and optional doc-level predicate
            auto& element_filter_expr = predicate_proto->element_filter_expr();
            element_expr = ParseExprs(element_filter_expr.element_expr());
            struct_name = element_filter_expr.struct_name();

            if (element_filter_expr.has_predicate()) {
                doc_expr = ParseExprs(element_filter_expr.predicate());
                if (plan_node_proto.has_namespace_()) {
                    doc_expr = MergeExprWithNamespace(
                        schema, doc_expr, plan_node_proto.namespace_());
                }
            }
        } else {
            // Document-level query: only doc expr
            doc_expr = ParseExprs(anns_proto.predicates());
            if (plan_node_proto.has_namespace_()) {
                doc_expr = MergeExprWithNamespace(
                    schema, doc_expr, plan_node_proto.namespace_());
            }
        }

        bool is_iterative =
            plan_node->search_info_.iterative_filter_execution &&
            plan_node->search_info_.group_by_field_id_ == std::nullopt;
        if (is_iterative) {
            plannode = std::make_shared<milvus::plan::MvccNode>(
                milvus::plan::GetNextPlanNodeId());
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

            plannode = std::make_shared<milvus::plan::VectorSearchNode>(
                milvus::plan::GetNextPlanNodeId(), sources);
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

            // Add element-level filter if needed
            if (is_element_level) {
                plannode = std::make_shared<plan::ElementFilterNode>(
                    milvus::plan::GetNextPlanNodeId(),
                    element_expr,
                    struct_name,
                    sources);
                sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            }

            // Add doc-level filter if present
            if (doc_expr) {
                plannode = std::make_shared<plan::FilterNode>(
                    milvus::plan::GetNextPlanNodeId(), doc_expr, sources);
                sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            }
        } else {
            if (doc_expr) {
                plannode = std::make_shared<plan::FilterBitsNode>(
                    milvus::plan::GetNextPlanNodeId(), doc_expr);
                sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

                if (!is_element_level &&
                    plan_node->search_info_.materialized_view_involved) {
                    const auto expr_info = plannode->GatherInfo();
                    knowhere::MaterializedViewSearchInfo
                        materialized_view_search_info;
                    for (const auto& [expr_field_id, vals] :
                         expr_info.field_id_to_values) {
                        materialized_view_search_info
                            .field_id_to_touched_categories_cnt[expr_field_id] =
                            vals.size();
                    }
                    materialized_view_search_info.is_pure_and =
                        expr_info.is_pure_and;
                    materialized_view_search_info.has_not = expr_info.has_not;

                    plan_node->search_info_.search_params_
                        [knowhere::meta::MATERIALIZED_VIEW_SEARCH_INFO] =
                        materialized_view_search_info;
                }
            }

            plannode = std::make_shared<milvus::plan::MvccNode>(
                milvus::plan::GetNextPlanNodeId(), sources);
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

            if (is_element_level) {
                plannode = std::make_shared<plan::ElementFilterBitsNode>(
                    milvus::plan::GetNextPlanNodeId(),
                    element_expr,
                    struct_name,
                    sources);
                sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            }

            plannode = std::make_shared<milvus::plan::VectorSearchNode>(
                milvus::plan::GetNextPlanNodeId(), sources);
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
        }
    } else {
        // no filter, force set iterative filter hint to false, go with normal vector search path
        plan_node->search_info_.iterative_filter_execution = false;
        plannode = std::make_shared<milvus::plan::MvccNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

        plannode = std::make_shared<milvus::plan::VectorSearchNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
    }

    if (plan_node->search_info_.group_by_field_id_ != std::nullopt) {
        plannode = std::make_shared<milvus::plan::SearchGroupByNode>(
            milvus::plan::GetNextPlanNodeId(), sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
    }

    // if has score function, run filter and scorer at last
    if (plan_node_proto.scorers_size() > 0) {
        std::vector<std::shared_ptr<rescores::Scorer>> scorers;
        for (const auto& function : plan_node_proto.scorers()) {
            scorers.push_back(ParseScorer(function));
        }

        plannode = std::make_shared<milvus::plan::RescoresNode>(
            milvus::plan::GetNextPlanNodeId(),
            std::move(scorers),
            plan_node_proto.score_option(),
            sources);
        sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
    }

    plan_node->plannodes_ = plannode;

    PlanOptionsFromProto(plan_node_proto.plan_options(),
                         plan_node->plan_options_);

    return plan_node;
}

std::unique_ptr<RetrievePlanNode>
ProtoParser::RetrievePlanNodeFromProto(
    const planpb::PlanNode& plan_node_proto) {
    Assert(plan_node_proto.has_predicates() || plan_node_proto.has_query());

    milvus::plan::PlanNodePtr plannode;
    std::vector<milvus::plan::PlanNodePtr> sources;

    auto plan_node = [&]() -> std::unique_ptr<RetrievePlanNode> {
        auto node = std::make_unique<RetrievePlanNode>();
        if (plan_node_proto.has_predicates()) {  // version before 2023.03.30.
            auto& predicate_proto = plan_node_proto.predicates();
            auto expr_parser = [&]() -> plan::PlanNodePtr {
                auto expr = ParseExprs(predicate_proto);
                if (plan_node_proto.has_namespace_()) {
                    expr = MergeExprWithNamespace(
                        schema, expr, plan_node_proto.namespace_());
                }
                return std::make_shared<plan::FilterBitsNode>(
                    milvus::plan::GetNextPlanNodeId(), expr);
            }();
            plannode = std::move(expr_parser);
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            plannode = std::make_shared<milvus::plan::MvccNode>(
                milvus::plan::GetNextPlanNodeId(), sources);
            node->plannodes_ = std::move(plannode);
        } else {
            // mvccNode--->FilterBitsNode or
            // aggNode---> projectNode --->mvccNode--->FilterBitsNode
            auto& query = plan_node_proto.query();

            // 1. Build FilterBitsNode and RandomSampleNode if needed
            auto filter_node = BuildFilterAndSampleNodes(
                query, plan_node_proto, schema, sources, this);
            if (filter_node) {
                plannode = filter_node;
                sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            }

            // 2. Build MvccNode
            plannode = std::make_shared<milvus::plan::MvccNode>(
                milvus::plan::GetNextPlanNodeId(), sources);
            sources = std::vector<milvus::plan::PlanNodePtr>{plannode};

            // 3. Build ProjectNode and AggregationNode if needed
            auto group_by_field_count = query.group_by_field_ids_size();
            auto agg_functions_count = query.aggregates_size();
            if (group_by_field_count > 0 || agg_functions_count > 0) {
                std::vector<FieldId> project_id_list;
                std::vector<std::string> project_name_list;
                std::vector<milvus::DataType> project_type_list;
                std::vector<expr::FieldAccessTypeExprPtr> groupingKeys;
                std::vector<plan::AggregationNode::Aggregate> aggregates;
                std::vector<std::string> agg_names;

                // Process group_by fields
                ProcessGroupByFields(query,
                                     schema,
                                     groupingKeys,
                                     project_id_list,
                                     project_name_list,
                                     project_type_list);

                // Process aggregates
                ProcessAggregates(query,
                                  schema,
                                  aggregates,
                                  agg_names,
                                  project_id_list,
                                  project_name_list,
                                  project_type_list);

                // Build ProjectNode and AggregationNode
                plannode = BuildProjectAndAggregationNodes(
                    query,
                    sources,
                    std::move(groupingKeys),
                    std::move(agg_names),
                    std::move(aggregates),
                    std::move(project_id_list),
                    std::move(project_name_list),
                    std::move(project_type_list));
                sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
            }

            // 4. Build OrderByNode if needed
            auto order_by_field_count = query.order_by_fields_size();
            if (order_by_field_count > 0) {
                // Without aggregation, the source is MvccNode which has
                // no output_type (bitmap-only). Insert a ProjectNode to
                // materialize column data so OrderByNode can sort it.
                bool has_aggregation =
                    (group_by_field_count > 0 || agg_functions_count > 0);
                if (!has_aggregation) {
                    auto [project, deferred, pipeline_ids] =
                        BuildOrderByProjectNode(
                            query, plan_node_proto, schema, sources);
                    plannode = project;
                    sources = std::vector<milvus::plan::PlanNodePtr>{plannode};
                    node->deferred_field_ids_ = std::move(deferred);
                    node->pipeline_field_ids_ = std::move(pipeline_ids);
                }
                plannode = BuildOrderByNode(query, schema, sources);
                node->has_order_by_ = true;
            }
            node->plannodes_ = plannode;
            node->limit_ = query.limit();
        }
        return node;
    }();

    PlanOptionsFromProto(plan_node_proto.plan_options(),
                         plan_node->plan_options_);
    return plan_node;
}

std::unique_ptr<Plan>
ProtoParser::CreatePlan(const proto::plan::PlanNode& plan_node_proto) {
    LOG_DEBUG("create search plan from proto: {}",
              plan_node_proto.ShortDebugString());
    auto plan = std::make_unique<Plan>(schema);

    auto plan_node = PlanNodeFromProto(plan_node_proto);
    plan->plan_node_ = std::move(plan_node);
    plan->tag2field_["$0"] = plan->plan_node_->search_info_.field_id_;
    ExtractedPlanInfo extra_info(schema->size());
    extra_info.add_involved_field(plan->plan_node_->search_info_.field_id_);
    plan->extra_info_opt_ = std::move(extra_info);

    for (auto field_id_raw : plan_node_proto.output_field_ids()) {
        auto field_id = FieldId(field_id_raw);
        plan->target_entries_.push_back(field_id);
    }
    for (auto dynamic_field : plan_node_proto.dynamic_fields()) {
        plan->target_dynamic_fields_.push_back(dynamic_field);
    }

    return plan;
}

std::unique_ptr<RetrievePlan>
ProtoParser::CreateRetrievePlan(const proto::plan::PlanNode& plan_node_proto) {
    LOG_DEBUG("create retrieve plan from proto: {}",
              plan_node_proto.ShortDebugString());
    auto retrieve_plan = std::make_unique<RetrievePlan>(schema);

    auto plan_node = RetrievePlanNodeFromProto(plan_node_proto);

    retrieve_plan->plan_node_ = std::move(plan_node);
    for (auto field_id_raw : plan_node_proto.output_field_ids()) {
        auto field_id = FieldId(field_id_raw);
        retrieve_plan->field_ids_.push_back(field_id);
    }
    for (auto dynamic_field : plan_node_proto.dynamic_fields()) {
        retrieve_plan->target_dynamic_fields_.push_back(dynamic_field);
    }
    return retrieve_plan;
}

expr::TypedExprPtr
ProtoParser::ParseUnaryRangeExprs(const proto::plan::UnaryRangeExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (column_info.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() ==
               static_cast<DataType>(column_info.element_type()));
    } else {
        Assert(data_type == static_cast<DataType>(column_info.data_type()));
    }
    std::vector<::milvus::proto::plan::GenericValue> extra_values;
    extra_values.reserve(expr_pb.extra_values_size());
    for (const auto& val : expr_pb.extra_values()) {
        extra_values.emplace_back(val);
    }
    return std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(column_info),
        expr_pb.op(),
        expr_pb.value(),
        extra_values);
}

expr::TypedExprPtr
ProtoParser::ParseNullExprs(const proto::plan::NullExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (column_info.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() ==
               static_cast<DataType>(column_info.element_type()));
    } else {
        Assert(data_type == static_cast<DataType>(column_info.data_type()));
    }
    return std::make_shared<milvus::expr::NullExpr>(
        expr::ColumnInfo(column_info), expr_pb.op());
}

expr::TypedExprPtr
ProtoParser::ParseBinaryRangeExprs(
    const proto::plan::BinaryRangeExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (columnInfo.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() == (DataType)columnInfo.element_type());
    } else {
        Assert(data_type == (DataType)columnInfo.data_type());
    }
    return std::make_shared<expr::BinaryRangeFilterExpr>(
        columnInfo,
        expr_pb.lower_value(),
        expr_pb.upper_value(),
        expr_pb.lower_inclusive(),
        expr_pb.upper_inclusive());
}

expr::TypedExprPtr
ProtoParser::ParseTimestamptzArithCompareExprs(
    const proto::plan::TimestamptzArithCompareExpr& expr_pb) {
    auto& columnInfo = expr_pb.timestamptz_column();
    auto field_id = FieldId(columnInfo.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (columnInfo.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() == (DataType)columnInfo.element_type());
    } else {
        Assert(data_type == (DataType)columnInfo.data_type());
    }
    return std::make_shared<expr::TimestamptzArithCompareExpr>(
        columnInfo,
        expr_pb.arith_op(),
        expr_pb.interval(),
        expr_pb.compare_op(),
        expr_pb.compare_value());
}

expr::TypedExprPtr
ProtoParser::ParseElementFilterExprs(
    const proto::plan::ElementFilterExpr& expr_pb) {
    // ElementFilterExpr is not a regular expression that can be evaluated directly.
    // It should be handled at the PlanNode level (in PlanNodeFromProto).
    // This method should never be called.
    ThrowInfo(ExprInvalid,
              "ParseElementFilterExprs should not be called directly. "
              "ElementFilterExpr must be handled at PlanNode level.");
}

expr::TypedExprPtr
ProtoParser::ParseMatchExprs(const proto::plan::MatchExpr& expr_pb) {
    auto struct_name = expr_pb.struct_name();
    auto match_type = expr_pb.match_type();
    auto count = expr_pb.count();
    auto predicate = this->ParseExprs(expr_pb.predicate());
    return std::make_shared<expr::MatchExpr>(
        struct_name, match_type, count, predicate);
}

expr::TypedExprPtr
ProtoParser::ParseCallExprs(const proto::plan::CallExpr& expr_pb) {
    const auto param_count = expr_pb.function_parameters_size();
    std::vector<expr::TypedExprPtr> parameters;
    parameters.reserve(param_count);
    std::vector<DataType> func_param_type_list;
    func_param_type_list.reserve(param_count);
    for (const auto& param_expr : expr_pb.function_parameters()) {
        // function parameter can be any type
        auto e = this->ParseExprs(param_expr, TypeIsAny);
        parameters.push_back(e);
        func_param_type_list.push_back(e->type());
    }
    auto& factory = exec::expression::FunctionFactory::Instance();
    exec::expression::FilterFunctionRegisterKey func_sig{
        expr_pb.function_name(), std::move(func_param_type_list)};

    auto function = factory.GetFilterFunction(func_sig);
    if (function == nullptr) {
        ThrowInfo(ExprInvalid,
                  "function " + func_sig.ToString() + " not found. ");
    }
    return std::make_shared<expr::CallExpr>(
        expr_pb.function_name(), parameters, function);
}

expr::TypedExprPtr
ProtoParser::ParseCompareExprs(const proto::plan::CompareExpr& expr_pb) {
    auto& left_column_info = expr_pb.left_column_info();
    auto left_field_id = FieldId(left_column_info.field_id());
    auto& left_field = schema->operator[](left_field_id);
    auto left_data_type = left_field.get_data_type();

    if (left_column_info.is_element_level()) {
        Assert(left_data_type == DataType::ARRAY);
        Assert(left_field.get_element_type() ==
               static_cast<DataType>(left_column_info.element_type()));
    } else {
        Assert(left_data_type ==
               static_cast<DataType>(left_column_info.data_type()));
    }

    auto& right_column_info = expr_pb.right_column_info();
    auto right_field_id = FieldId(right_column_info.field_id());
    auto& right_field = schema->operator[](right_field_id);
    auto right_data_type = right_field.get_data_type();

    if (right_column_info.is_element_level()) {
        Assert(right_data_type == DataType::ARRAY);
        Assert(right_field.get_element_type() ==
               static_cast<DataType>(right_column_info.element_type()));
    } else {
        Assert(right_data_type ==
               static_cast<DataType>(right_column_info.data_type()));
    }

    return std::make_shared<expr::CompareExpr>(left_field_id,
                                               right_field_id,
                                               left_data_type,
                                               right_data_type,
                                               expr_pb.op());
}

expr::TypedExprPtr
ProtoParser::ParseTermExprs(const proto::plan::TermExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (columnInfo.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() == (DataType)columnInfo.element_type());
    } else {
        Assert(data_type == (DataType)columnInfo.data_type());
    }
    std::vector<::milvus::proto::plan::GenericValue> values;
    values.reserve(expr_pb.values_size());
    for (size_t i = 0; i < expr_pb.values_size(); i++) {
        values.emplace_back(expr_pb.values(i));
    }
    return std::make_shared<expr::TermFilterExpr>(
        columnInfo, values, expr_pb.is_in_field());
}

expr::TypedExprPtr
ProtoParser::ParseUnaryExprs(const proto::plan::UnaryExpr& expr_pb) {
    auto op = static_cast<expr::LogicalUnaryExpr::OpType>(expr_pb.op());
    Assert(op == expr::LogicalUnaryExpr::OpType::LogicalNot);
    auto child_expr = this->ParseExprs(expr_pb.child());
    return std::make_shared<expr::LogicalUnaryExpr>(op, child_expr);
}

expr::TypedExprPtr
ProtoParser::ParseBinaryExprs(const proto::plan::BinaryExpr& expr_pb) {
    auto op = static_cast<expr::LogicalBinaryExpr::OpType>(expr_pb.op());
    auto left_expr = this->ParseExprs(expr_pb.left());
    auto right_expr = this->ParseExprs(expr_pb.right());
    return std::make_shared<expr::LogicalBinaryExpr>(op, left_expr, right_expr);
}

expr::TypedExprPtr
ProtoParser::ParseBinaryArithOpEvalRangeExprs(
    const proto::plan::BinaryArithOpEvalRangeExpr& expr_pb) {
    auto& column_info = expr_pb.column_info();
    auto field_id = FieldId(column_info.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (column_info.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() ==
               static_cast<DataType>(column_info.element_type()));
    } else {
        Assert(data_type == static_cast<DataType>(column_info.data_type()));
    }
    return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        column_info,
        expr_pb.op(),
        expr_pb.arith_op(),
        expr_pb.value(),
        expr_pb.right_operand());
}

expr::TypedExprPtr
ProtoParser::ParseExistExprs(const proto::plan::ExistsExpr& expr_pb) {
    auto& column_info = expr_pb.info();
    auto field_id = FieldId(column_info.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (column_info.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() ==
               static_cast<DataType>(column_info.element_type()));
    } else {
        Assert(data_type == static_cast<DataType>(column_info.data_type()));
    }
    return std::make_shared<expr::ExistsExpr>(column_info);
}

expr::TypedExprPtr
ProtoParser::ParseJsonContainsExprs(
    const proto::plan::JSONContainsExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (columnInfo.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() == (DataType)columnInfo.element_type());
    } else {
        Assert(data_type == (DataType)columnInfo.data_type());
    }
    std::vector<::milvus::proto::plan::GenericValue> values;
    values.reserve(expr_pb.elements_size());
    for (size_t i = 0; i < expr_pb.elements_size(); i++) {
        values.emplace_back(expr_pb.elements(i));
    }
    return std::make_shared<expr::JsonContainsExpr>(
        columnInfo,
        expr_pb.op(),
        expr_pb.elements_same_type(),
        std::move(values));
}

expr::TypedExprPtr
ProtoParser::ParseColumnExprs(const proto::plan::ColumnExpr& expr_pb) {
    return std::make_shared<expr::ColumnExpr>(expr_pb.info());
}

expr::TypedExprPtr
ProtoParser::ParseValueExprs(const proto::plan::ValueExpr& expr_pb) {
    return std::make_shared<expr::ValueExpr>(expr_pb.value());
}

expr::TypedExprPtr
ProtoParser::ParseGISFunctionFilterExprs(
    const proto::plan::GISFunctionFilterExpr& expr_pb) {
    auto& columnInfo = expr_pb.column_info();
    auto field_id = FieldId(columnInfo.field_id());
    auto& field = schema->operator[](field_id);
    auto data_type = field.get_data_type();

    if (columnInfo.is_element_level()) {
        Assert(data_type == DataType::ARRAY);
        Assert(field.get_element_type() == (DataType)columnInfo.element_type());
    } else {
        Assert(data_type == (DataType)columnInfo.data_type());
    }

    auto expr = std::make_shared<expr::GISFunctionFilterExpr>(
        columnInfo, expr_pb.op(), expr_pb.wkt_string(), expr_pb.distance());
    return expr;
}

expr::TypedExprPtr
ProtoParser::CreateAlwaysTrueExprs() {
    return std::make_shared<expr::AlwaysTrueExpr>();
}

expr::TypedExprPtr
ProtoParser::ParseExprs(const proto::plan::Expr& expr_pb,
                        TypeCheckFunction type_check) {
    using ppe = proto::plan::Expr;
    expr::TypedExprPtr result;
    switch (expr_pb.expr_case()) {
        case ppe::kUnaryRangeExpr: {
            result = ParseUnaryRangeExprs(expr_pb.unary_range_expr());
            break;
        }
        case ppe::kBinaryExpr: {
            result = ParseBinaryExprs(expr_pb.binary_expr());
            break;
        }
        case ppe::kUnaryExpr: {
            result = ParseUnaryExprs(expr_pb.unary_expr());
            break;
        }
        case ppe::kTermExpr: {
            result = ParseTermExprs(expr_pb.term_expr());
            break;
        }
        case ppe::kBinaryRangeExpr: {
            result = ParseBinaryRangeExprs(expr_pb.binary_range_expr());
            break;
        }
        case ppe::kCompareExpr: {
            result = ParseCompareExprs(expr_pb.compare_expr());
            break;
        }
        case ppe::kBinaryArithOpEvalRangeExpr: {
            result = ParseBinaryArithOpEvalRangeExprs(
                expr_pb.binary_arith_op_eval_range_expr());
            break;
        }
        case ppe::kExistsExpr: {
            result = ParseExistExprs(expr_pb.exists_expr());
            break;
        }
        case ppe::kAlwaysTrueExpr: {
            result = CreateAlwaysTrueExprs();
            break;
        }
        case ppe::kJsonContainsExpr: {
            result = ParseJsonContainsExprs(expr_pb.json_contains_expr());
            break;
        }
        case ppe::kCallExpr: {
            result = ParseCallExprs(expr_pb.call_expr());
            break;
        }
            // may emit various types
        case ppe::kColumnExpr: {
            result = ParseColumnExprs(expr_pb.column_expr());
            break;
        }
        case ppe::kValueExpr: {
            result = ParseValueExprs(expr_pb.value_expr());
            break;
        }
        case ppe::kNullExpr: {
            result = ParseNullExprs(expr_pb.null_expr());
            break;
        }
        case ppe::kGisfunctionFilterExpr: {
            result =
                ParseGISFunctionFilterExprs(expr_pb.gisfunction_filter_expr());
            break;
        }
        case ppe::kTimestamptzArithCompareExpr: {
            result = ParseTimestamptzArithCompareExprs(
                expr_pb.timestamptz_arith_compare_expr());
            break;
        }
        case ppe::kElementFilterExpr: {
            ThrowInfo(ExprInvalid,
                      "ElementFilterExpr should be handled at PlanNode level, "
                      "not in ParseExprs");
        }
        case ppe::kMatchExpr: {
            result = ParseMatchExprs(expr_pb.match_expr());
            break;
        }
        default: {
            std::string s;
            google::protobuf::TextFormat::PrintToString(expr_pb, &s);
            ThrowInfo(ExprInvalid,
                      std::string("unsupported expr proto node: ") + s);
        }
    }
    if (type_check(result->type())) {
        return result;
    }
    ThrowInfo(
        ExprInvalid, "expr type check failed, actual type: {}", result->type());
}

std::shared_ptr<rescores::Scorer>
ProtoParser::ParseScorer(const proto::plan::ScoreFunction& function) {
    expr::TypedExprPtr expr = nullptr;
    if (function.has_filter()) {
        expr = ParseExprs(function.filter());
    }

    switch (function.type()) {
        case proto::plan::FunctionTypeWeight:
            return std::make_shared<rescores::WeightScorer>(expr,
                                                            function.weight());
        case proto::plan::FunctionTypeRandom:
            return std::make_shared<rescores::RandomScorer>(
                expr, function.weight(), function.params());
        default:
            ThrowInfo(UnexpectedError, "unknown function type");
    }
}

}  // namespace milvus::query
