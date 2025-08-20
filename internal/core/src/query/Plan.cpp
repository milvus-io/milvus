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

#include "Plan.h"
#include "common/Utils.h"
#include "PlanProto.h"

namespace milvus::query {

// deprecated
std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan,
                      const std::string_view placeholder_group_blob) {
    return ParsePlaceholderGroup(
        plan,
        reinterpret_cast<const uint8_t*>(placeholder_group_blob.data()),
        placeholder_group_blob.size());
}

bool
check_data_type(const FieldMeta& field_meta,
                const milvus::proto::common::PlaceholderType type) {
    if (field_meta.get_data_type() == DataType::VECTOR_ARRAY) {
        return type ==
               milvus::proto::common::PlaceholderType::EmbListFloatVector;
    }
    return static_cast<int>(field_meta.get_data_type()) ==
           static_cast<int>(type);
}

std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan,
                      const uint8_t* blob,
                      const int64_t blob_len) {
    auto result = std::make_unique<PlaceholderGroup>();
    milvus::proto::common::PlaceholderGroup ph_group;
    auto ok = ph_group.ParseFromArray(blob, blob_len);
    Assert(ok);
    for (auto& info : ph_group.placeholders()) {
        Placeholder element;
        element.tag_ = info.tag();
        Assert(plan->tag2field_.count(element.tag_));
        auto field_id = plan->tag2field_.at(element.tag_);
        auto& field_meta = plan->schema_->operator[](field_id);
        AssertInfo(check_data_type(field_meta, info.type()),
                   "vector type must be the same, field {} - type {}, search "
                   "info type {}",
                   field_meta.get_name().get(),
                   field_meta.get_data_type(),
                   static_cast<DataType>(info.type()));
        element.num_of_queries_ = info.values_size();
        AssertInfo(element.num_of_queries_ > 0, "must have queries");
        if (info.type() ==
            milvus::proto::common::PlaceholderType::SparseFloatVector) {
            element.sparse_matrix_ =
                SparseBytesToRows(info.values(), /*validate=*/true);
        } else {
            auto line_size = info.values().Get(0).size();
            auto& target = element.blob_;

            if (field_meta.get_data_type() != DataType::VECTOR_ARRAY) {
                if (field_meta.get_sizeof() != line_size) {
                    ThrowInfo(DimNotMatch,
                              fmt::format(
                                  "vector dimension mismatch, expected vector "
                                  "size(byte) {}, actual {}.",
                                  field_meta.get_sizeof(),
                                  line_size));
                }
                target.reserve(line_size * element.num_of_queries_);
                for (auto& line : info.values()) {
                    AssertInfo(line_size == line.size(),
                               "vector dimension mismatch, expected vector "
                               "size(byte) {}, actual {}.",
                               line_size,
                               line.size());
                    target.insert(target.end(), line.begin(), line.end());
                }
            } else {
                target.reserve(line_size * element.num_of_queries_);
                auto dim = field_meta.get_dim();

                // If the vector is embedding list, line contains multiple vectors.
                // And we should record the offsets so that we can identify each
                // embedding list in a flattened vectors.
                auto& lims = element.lims_;
                lims.reserve(element.num_of_queries_ + 1);
                size_t offset = 0;
                lims.push_back(offset);

                auto elem_size = milvus::index::vector_element_size(
                    field_meta.get_element_type());
                for (auto& line : info.values()) {
                    target.insert(target.end(), line.begin(), line.end());
                    AssertInfo(
                        line.size() % (dim * elem_size) == 0,
                        "line.size() % (dim * elem_size) == 0 assert failed, "
                        "line.size() = {}, dim = {}, elem_size = {}",
                        line.size(),
                        dim,
                        elem_size);

                    offset += line.size() / (dim * elem_size);
                    lims.push_back(offset);
                }
            }
        }
        result->emplace_back(std::move(element));
    }
    return result;
}

void
ParsePlanNodeProto(proto::plan::PlanNode& plan_node,
                   const void* serialized_expr_plan,
                   int64_t size) {
    google::protobuf::io::ArrayInputStream array_stream(serialized_expr_plan,
                                                        size);
    google::protobuf::io::CodedInputStream input_stream(&array_stream);
    input_stream.SetRecursionLimit(std::numeric_limits<int32_t>::max());

    auto res = plan_node.ParsePartialFromCodedStream(&input_stream);
    if (!res) {
        ThrowInfo(UnexpectedError, "parse plan node proto failed");
    }
}

std::unique_ptr<Plan>
CreateSearchPlanByExpr(SchemaPtr schema,
                       const void* serialized_expr_plan,
                       const int64_t size) {
    // Note: serialized_expr_plan is of binary format
    proto::plan::PlanNode plan_node;
    ParsePlanNodeProto(plan_node, serialized_expr_plan, size);
    return ProtoParser(std::move(schema)).CreatePlan(plan_node);
}

std::unique_ptr<Plan>
CreateSearchPlanFromPlanNode(SchemaPtr schema,
                             const proto::plan::PlanNode& plan_node) {
    return ProtoParser(std::move(schema)).CreatePlan(plan_node);
}

std::unique_ptr<RetrievePlan>
CreateRetrievePlanByExpr(SchemaPtr schema,
                         const void* serialized_expr_plan,
                         const int64_t size) {
    proto::plan::PlanNode plan_node;
    ParsePlanNodeProto(plan_node, serialized_expr_plan, size);
    return ProtoParser(std::move(schema)).CreateRetrievePlan(plan_node);
}

int64_t
GetTopK(const Plan* plan) {
    return plan->plan_node_->search_info_.topk_;
}

int64_t
GetFieldID(const Plan* plan) {
    return plan->plan_node_->search_info_.field_id_.get();
}

int64_t
GetNumOfQueries(const PlaceholderGroup* group) {
    return group->at(0).num_of_queries_;
}

// std::unique_ptr<RetrievePlan>
// CreateRetrievePlan(const Schema& schema, proto::segcore::RetrieveRequest&& request) {
//    auto plan = std::make_unique<RetrievePlan>();
//    plan->seg_offsets_ = std::unique_ptr<proto::schema::IDs>(request.release_ids());
//    for (auto& field_id : request.output_fields_id()) {
//        plan->field_ids_.push_back(schema.get_offset(FieldId(field_id)));
//    }
//    return plan;
//}

}  // namespace milvus::query
