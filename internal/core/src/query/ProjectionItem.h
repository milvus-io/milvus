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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "grep/CompiledPlan.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"

namespace milvus::query {

// One output column that is not a stored field: a virtual column whose value
// is produced while the request runs. output_id is local to the request. It
// is assigned by the proxy and is looked up in the plan's item list; no code
// interprets its numeric value. See ProjectionItem in plan.proto.
struct ProjectionItem {
    FieldId output_id;
    proto::plan::VirtualColumn column;
    // Set for a GREP_MATCHES(field, args) item; column is then
    // VirtualColumnInvalid. The column is nullable JSON, built by
    // grep::BuildGrepMatchesColumn at the points that read output fields.
    std::shared_ptr<const proto::plan::GrepMatches> grep_matches;
    // The compiled patterns of grep_matches, shared by every segment of the
    // request and with a predicate of the same pattern. Null for other items
    // and for an item that selects no line.
    std::shared_ptr<const grep::Plan> grep_plan;
};

// Name of the column in Arrow exports and diagnostics.
inline constexpr const char* kGrepMatchesProjectionName = "grep_matches";

// The schema-less description of a GREP_MATCHES column, for the code that
// merges and exports output columns by FieldMeta.
inline FieldMeta
GrepMatchesFieldMeta(const ProjectionItem& item) {
    return FieldMeta(FieldName(kGrepMatchesProjectionName),
                     item.output_id,
                     DataType::JSON,
                     /*nullable=*/true,
                     std::nullopt);
}

// An empty GREP_MATCHES column: no rows, typed as nullable JSON.
inline std::unique_ptr<proto::schema::FieldData>
BuildEmptyGrepMatchesColumn(const ProjectionItem& item) {
    auto column = std::make_unique<proto::schema::FieldData>();
    column->set_field_id(item.output_id.get());
    column->set_type(proto::schema::DataType::JSON);
    column->mutable_scalars()->mutable_json_data();
    return column;
}

inline const ProjectionItem*
FindProjectionItem(const std::vector<ProjectionItem>& items, FieldId id) {
    for (const auto& item : items) {
        if (item.output_id == id) {
            return &item;
        }
    }
    return nullptr;
}

inline std::string
ProjectionItemName(const ProjectionItem& item) {
    if (item.grep_matches != nullptr) {
        return kGrepMatchesProjectionName;
    }
    switch (item.column) {
        case proto::plan::VirtualColumn::VirtualColumnSegmentID:
            return "$seg_id";
        case proto::plan::VirtualColumn::VirtualColumnSegmentOffset:
            return "$seg_offset";
        case proto::plan::VirtualColumn::VirtualColumnDistance:
            return "$distance";
        case proto::plan::VirtualColumn::VirtualColumnScore:
            return "$score";
        default:
            return "$unknown";
    }
}

// The virtual columns that describe where a row is stored. Both are Int64
// and are computed from the segment and the row offsets alone.
inline bool
IsPlacementProjectionItem(const ProjectionItem& item) {
    return item.column == proto::plan::VirtualColumn::VirtualColumnSegmentID ||
           item.column ==
               proto::plan::VirtualColumn::VirtualColumnSegmentOffset;
}

// Builds the Int64 column of a placement item for `size` rows of one segment.
inline std::unique_ptr<proto::schema::FieldData>
BuildPlacementProjectionColumn(const ProjectionItem& item,
                               int64_t segment_id,
                               const int64_t* offsets,
                               int64_t size) {
    AssertInfo(IsPlacementProjectionItem(item),
               "projection item {} is not computed from row placement",
               item.output_id.get());
    auto column = std::make_unique<proto::schema::FieldData>();
    column->set_field_id(item.output_id.get());
    column->set_type(proto::schema::DataType::Int64);
    auto* data = column->mutable_scalars()->mutable_long_data()->mutable_data();
    data->Reserve(size);
    if (item.column == proto::plan::VirtualColumn::VirtualColumnSegmentID) {
        for (int64_t i = 0; i < size; ++i) {
            data->AddAlreadyReserved(segment_id);
        }
    } else {
        for (int64_t i = 0; i < size; ++i) {
            data->AddAlreadyReserved(offsets[i]);
        }
    }
    return column;
}

inline std::vector<ProjectionItem>
ProjectionItemsFromProto(const proto::plan::PlanNode& plan_node_proto,
                         grep::PlanCompiler& grep_plans) {
    std::vector<ProjectionItem> items;
    items.reserve(plan_node_proto.projection_items_size());
    for (const auto& item : plan_node_proto.projection_items()) {
        AssertInfo(item.has_grep_matches() ||
                       item.virtual_column() !=
                           proto::plan::VirtualColumn::VirtualColumnInvalid,
                   "projection item {} has no virtual column",
                   item.output_id());
        AssertInfo(
            FindProjectionItem(items, FieldId(item.output_id())) == nullptr,
            "duplicate projection item id {}",
            item.output_id());
        if (item.has_grep_matches()) {
            items.push_back({FieldId(item.output_id()),
                             proto::plan::VirtualColumn::VirtualColumnInvalid,
                             std::make_shared<const proto::plan::GrepMatches>(
                                 item.grep_matches()),
                             grep_plans.Compile(item.grep_matches().spec())});
            continue;
        }
        items.push_back({FieldId(item.output_id()),
                         item.virtual_column(),
                         nullptr,
                         nullptr});
    }
    return items;
}

}  // namespace milvus::query
